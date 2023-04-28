import { WebSocket } from "@libsql/isomorphic-ws";

import { IdAlloc } from "./id_alloc.js";
import { ClientError, ProtoError, ClosedError, WebSocketError } from "./errors.js";
import type * as proto from "./proto.js";
import { errorFromProto } from "./result.js";
import { Stream } from "./stream.js";

/** A client that talks to a SQL server using the Hrana protocol over a WebSocket. */
export class Client {
    #socket: WebSocket;
    // List of messages that we queue until the socket transitions from the CONNECTING to the OPEN state.
    #msgsWaitingToOpen: proto.ClientMsg[];
    // Stores the error that caused us to close the client (and the socket). If we are not closed, this is
    // `undefined`.
    #closed: Error | undefined;

    // Have we received a response to our "hello" from the server?
    #recvdHello: boolean;
    // A map from request id to the responses that we expect to receive from the server.
    #responseMap: Map<number, ResponseState>;
    // An allocator of request ids.
    #requestIdAlloc: IdAlloc;
    // An allocator of stream ids.
    #streamIdAlloc: IdAlloc;

    /** @private */
    constructor(socket: WebSocket, jwt: string | null) {
        this.#socket = socket;
        this.#socket.binaryType = "arraybuffer";
        this.#msgsWaitingToOpen = [];
        this.#closed = undefined;

        this.#recvdHello = false;
        this.#responseMap = new Map();
        this.#requestIdAlloc = new IdAlloc();
        this.#streamIdAlloc = new IdAlloc();

        this.#socket.addEventListener("open", () => this.#onSocketOpen());
        this.#socket.addEventListener("close", (event) => this.#onSocketClose(event));
        this.#socket.addEventListener("error", (event) => this.#onSocketError(event));
        this.#socket.addEventListener("message", (event) => this.#onSocketMessage(event));

        this.#send({"type": "hello", "jwt": jwt});
    }

    // Send (or enqueue to send) a message to the server.
    #send(msg: proto.ClientMsg): void {
        if (this.#closed !== undefined) {
            throw new ClientError("Internal error: trying to send a message on a closed client");
        }

        if (this.#socket.readyState >= WebSocket.OPEN) {
            this.#sendToSocket(msg);
        } else {
            this.#msgsWaitingToOpen.push(msg);
        }
    }

    // The socket transitioned from CONNECTING to OPEN
    #onSocketOpen(): void {
        for (const msg of this.#msgsWaitingToOpen) {
            this.#sendToSocket(msg);
        }
        this.#msgsWaitingToOpen.length = 0;
    }

    #sendToSocket(msg: proto.ClientMsg): void {
        this.#socket.send(JSON.stringify(msg));
    }

    // Send a request to the server and invoke a callback when we get the response.
    #sendRequest(request: proto.Request, callbacks: ResponseCallbacks) {
        if (this.#closed !== undefined) {
            callbacks.errorCallback(new ClosedError("Client is closed", this.#closed));
            return;
        }

        const requestId = this.#requestIdAlloc.alloc();
        this.#responseMap.set(requestId, {...callbacks, type: request.type});
        this.#send({"type": "request", "request_id": requestId, request});
    }

    // The socket encountered an error.
    #onSocketError(event: Event | WebSocket.ErrorEvent): void {
        const eventMessage = (event as {message?: string}).message;
        const message = eventMessage ?? "Connection was closed due to an error";
        this.#setClosed(new WebSocketError(message));
    }

    // The socket was closed.
    #onSocketClose(event: WebSocket.CloseEvent): void {
        let message = `WebSocket was closed with code ${event.code}`;
        if (event.reason) {
            message += `: ${event.reason}`;
        }
        this.#setClosed(new WebSocketError(message));
    }

    // Close the client with the given error.
    #setClosed(error: Error): void {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;

        for (const [requestId, responseState] of this.#responseMap.entries()) {
            responseState.errorCallback(error);
            this.#requestIdAlloc.free(requestId);
        }
        this.#responseMap.clear();

        this.#socket.close();
    }

    // We received a message from the socket.
    #onSocketMessage(event: WebSocket.MessageEvent): void {
        if (typeof event.data !== "string") {
            this.#socket.close(3003, "Only string messages are accepted");
            this.#setClosed(new ProtoError("Received non-string message from server"))
            return;
        }

        try {
            this.#handleMsg(event.data);
        } catch (e) {
            this.#socket.close(3007, "Could not handle message");
            this.#setClosed(e as Error);
        }
    }

    // Handle a message from the server.
    #handleMsg(msgText: string): void {
        const msg = JSON.parse(msgText) as proto.ServerMsg;

        if (msg["type"] === "hello_ok" || msg["type"] === "hello_error") {
            if (this.#recvdHello) {
                throw new ProtoError("Received a duplicated hello response");
            }
            this.#recvdHello = true;

            if (msg["type"] === "hello_error") {
                throw errorFromProto(msg["error"]);
            }
            return;
        } else if (!this.#recvdHello) {
            throw new ProtoError("Received a non-hello message before a hello response");
        }

        if (msg["type"] === "response_ok") {
            const requestId = msg["request_id"];
            const responseState = this.#responseMap.get(requestId);
            this.#responseMap.delete(requestId);

            if (responseState === undefined) {
                throw new ProtoError("Received unexpected OK response");
            }
            this.#requestIdAlloc.free(requestId);

            try {
                if (responseState.type !== msg["response"]["type"]) {
                    throw new ProtoError("Received unexpected type of response");
                }
                responseState.responseCallback(msg["response"]);
            } catch (e) {
                responseState.errorCallback(e as Error);
                throw e;
            }
        } else if (msg["type"] === "response_error") {
            const requestId = msg["request_id"];
            const responseState = this.#responseMap.get(requestId);
            this.#responseMap.delete(requestId);

            if (responseState === undefined) {
                throw new ProtoError("Received unexpected error response");
            }
            this.#requestIdAlloc.free(requestId);

            responseState.errorCallback(errorFromProto(msg["error"]));
        } else {
            throw new ProtoError("Received unexpected message type");
        }
    }

    /** Open a {@link Stream}, a stream for executing SQL statements. */
    openStream(): Stream {
        const streamId = this.#streamIdAlloc.alloc();
        const streamState = {
            streamId,
            closed: undefined,
        };

        const responseCallback = () => undefined;
        const errorCallback = (e: Error) => this._closeStream(streamState, e);

        const request: proto.OpenStreamReq = {
            "type": "open_stream",
            "stream_id": streamId,
        };
        this.#sendRequest(request, {responseCallback, errorCallback});

        return new Stream(this, streamState);
    }

    // Make sure that the stream is closed.
    /** @private */
    _closeStream(streamState: StreamState, error: Error): void {
        if (streamState.closed !== undefined || this.#closed !== undefined) {
            return;
        }
        streamState.closed = error;

        const callback = () => {
            this.#streamIdAlloc.free(streamState.streamId);
        };
        const request: proto.CloseStreamReq = {
            "type": "close_stream",
            "stream_id": streamState.streamId,
        };
        this.#sendRequest(request, {responseCallback: callback, errorCallback: callback});
    }

    // Execute a statement on a stream and invoke callbacks in `stmtCallbacks` when we get the results (or an
    // error).
    /** @private */
    _execute(streamState: StreamState, stmtCallbacks: StmtCallbacks): void {
        const responseCallback = (response: proto.Response) => {
            stmtCallbacks.resultCallback((response as proto.ExecuteResp)["result"]);
        };
        const errorCallback = (error: Error) => {
            stmtCallbacks.errorCallback(error);
        }

        if (streamState.closed !== undefined) {
            errorCallback(new ClosedError("Stream was closed", streamState.closed));
            return;
        }

        const request: proto.ExecuteReq = {
            "type": "execute",
            "stream_id": streamState.streamId,
            "stmt": stmtCallbacks.stmt,
        };
        this.#sendRequest(request, {responseCallback, errorCallback});
    }

    // Execute a batch on a stream and invoke callbacks in `batchCallbacks` when we get the results (or an
    // error).
    /** @private */
    _batch(streamState: StreamState, batchCallbacks: BatchCallbacks): void {
        const responseCallback = (response: proto.Response) => {
            const result = (response as proto.BatchResp)["result"];
            for (const callback of batchCallbacks.resultCallbacks) {
                callback(result);
            }
            batchCallbacks.doneCallback();
        };
        const errorCallback = (error: Error) => {
            batchCallbacks.errorCallback(error);
        };

        if (streamState.closed !== undefined) {
            errorCallback(new ClosedError("Stream was closed", streamState.closed));
            return;
        }

        const request: proto.BatchReq = {
            "type": "batch",
            "stream_id": streamState.streamId,
            "batch": batchCallbacks.batch,
        };
        this.#sendRequest(request, {responseCallback, errorCallback});
    }

    /** Close the client and the WebSocket. */
    close() {
        this.#setClosed(new ClientError("Client was manually closed"));
    }

    /** True if the client is closed. */
    get closed() {
        return this.#closed !== undefined;
    }
}

interface ResponseCallbacks {
    responseCallback: (_: proto.Response) => void;
    errorCallback: (_: Error) => void;
}

interface ResponseState extends ResponseCallbacks {
    type: string;
}

export interface StmtCallbacks {
    stmt: proto.Stmt;
    resultCallback: (_: proto.StmtResult) => void;
    errorCallback: (_: Error) => void;
}

export interface StreamState {
    streamId: number;
    closed: Error | undefined;
}

export interface BatchCallbacks {
    batch: proto.Batch;
    resultCallbacks: Array<(_: proto.BatchResult) => void>;
    doneCallback: () => void;
    errorCallback: (_: Error) => void;
}
