import { WebSocket } from "@libsql/isomorphic-ws";

import { IdAlloc } from "./id_alloc.js";
import { ClientError, ProtoError, ClosedError, WebSocketError, ProtocolVersionError } from "./errors.js";
import type * as proto from "./proto.js";
import { errorFromProto } from "./result.js";
import { Sql } from "./sql.js";
import { Stream } from "./stream.js";

export type ProtocolVersion = 1 | 2;
export const protocolVersions: Map<string, ProtocolVersion> = new Map([
    ["hrana2", 2],
    ["hrana1", 1],
]);

/** A client that talks to a SQL server using the Hrana protocol over a WebSocket. */
export class Client {
    #socket: WebSocket;
    // List of callbacks that we queue until the socket transitions from the CONNECTING to the OPEN state.
    #openCallbacks: Array<OpenCallbacks>;
    // Stores the error that caused us to close the client (and the socket). If we are not closed, this is
    // `undefined`.
    #closed: Error | undefined;

    // Have we received a response to our "hello" from the server?
    #recvdHello: boolean;
    // Protocol version negotiated with the server. It is only available after the socket transitions to the
    // OPEN state.
    #version: ProtocolVersion | undefined;
    // Has the `getVersion()` function been called? This is only used to validate that the API is used
    // correctly.
    #getVersionCalled: boolean;
    // A map from request id to the responses that we expect to receive from the server.
    #responseMap: Map<number, ResponseState>;
    // An allocator of request ids.
    #requestIdAlloc: IdAlloc;
    // An allocator of stream ids.
    #streamIdAlloc: IdAlloc;
    // An allocator of SQL text ids.
    #sqlIdAlloc: IdAlloc;

    /** @private */
    constructor(socket: WebSocket, jwt: string | null) {
        this.#socket = socket;
        this.#socket.binaryType = "arraybuffer";
        this.#openCallbacks = [];
        this.#closed = undefined;

        this.#recvdHello = false;
        this.#version = undefined;
        this.#getVersionCalled = false;
        this.#responseMap = new Map();
        this.#requestIdAlloc = new IdAlloc();
        this.#streamIdAlloc = new IdAlloc();
        this.#sqlIdAlloc = new IdAlloc();

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
            const openCallback = () => this.#sendToSocket(msg);
            const errorCallback = (_: Error) => undefined;
            this.#openCallbacks.push({openCallback, errorCallback});
        }
    }

    // The socket transitioned from CONNECTING to OPEN
    #onSocketOpen(): void {
        const protocol = this.#socket.protocol;
        if (protocol === "") {
            this.#version = 1;
        } else {
            this.#version = protocolVersions.get(protocol);
            if (this.#version === undefined) {
                this.#setClosed(new ProtoError(
                    `Unrecognized WebSocket subprotocol: ${JSON.stringify(protocol)}`,
                ));
            }
        }

        for (const callbacks of this.#openCallbacks) {
            callbacks.openCallback();
        }
        this.#openCallbacks.length = 0;
    }

    #sendToSocket(msg: proto.ClientMsg): void {
        this.#socket.send(JSON.stringify(msg));
    }

    /** Get the protocol version negotiated with the server, possibly waiting until the socket is open. */
    getVersion(): Promise<ProtocolVersion> {
        return new Promise((versionCallback, errorCallback) => {
            this.#getVersionCalled = true;
            if (this.#closed !== undefined) {
                errorCallback(this.#closed);
            } else if (this.#version !== undefined) {
                versionCallback(this.#version);
            } else {
                const openCallback = () => versionCallback(this.#version!);
                this.#openCallbacks.push({openCallback, errorCallback});
            }
        });
    }

    // Make sure that the negotiated version is at least `minVersion`.
    /** @private */
    _ensureVersion(minVersion: ProtocolVersion, feature: string): void {
        if (this.#version === undefined || !this.#getVersionCalled) {
            throw new ProtocolVersionError(
                `${feature} is supported only on protocol version ${minVersion} and higher, ` +
                    "but the version supported by the server is not yet known. Use Client.getVersion() " +
                    "to wait until the version is available.",
            );
        } else if (this.#version < minVersion) {
            throw new ProtocolVersionError(
                `${feature} is supported on protocol version ${minVersion} and higher, ` +
                    `but the server only supports version ${this.#version}`
            );
        }
    }

    // Send a request to the server and invoke a callback when we get the response.
    /** @private */
    _sendRequest(request: proto.Request, callbacks: ResponseCallbacks) {
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

        for (const callbacks of this.#openCallbacks) {
            callbacks.errorCallback(error);
        }
        this.#openCallbacks.length = 0;

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
        this._sendRequest(request, {responseCallback, errorCallback});

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
        this._sendRequest(request, {responseCallback: callback, errorCallback: callback});
    }

    // Send a stream-specific request to the server and invoke a callback when we get the response.
    /** @private */
    _sendStreamRequest(streamState: StreamState, request: proto.Request, callbacks: ResponseCallbacks): void {
        if (streamState.closed !== undefined) {
            callbacks.errorCallback(new ClosedError("Stream is closed", streamState.closed));
            return;
        }
        this._sendRequest(request, callbacks);
    }

    /** Cache a SQL text on the server. This requires protocol version 2 or higher. */
    storeSql(sql: string): Sql {
        this._ensureVersion(2, "storeSql()");

        const sqlId = this.#sqlIdAlloc.alloc();
        const sqlState = {
            sqlId,
            closed: undefined,
        };


        const responseCallback = () => undefined;
        const errorCallback = (e: Error) => this._closeSql(sqlState, e);

        const request: proto.StoreSqlReq = {
            "type": "store_sql",
            "sql_id": sqlId,
            "sql": sql,
        };
        this._sendRequest(request, {responseCallback, errorCallback});

        return new Sql(this, sqlState);
    }

    // Make sure that the SQL text is closed.
    /** @private */
    _closeSql(sqlState: SqlState, error: Error): void {
        if (sqlState.closed !== undefined || this.#closed !== undefined) {
            return;
        }
        sqlState.closed = error;

        const callback = () => {
            this.#sqlIdAlloc.free(sqlState.sqlId);
        };
        const request: proto.CloseSqlReq = {
            "type": "close_sql",
            "sql_id": sqlState.sqlId,
        };
        this._sendRequest(request, {responseCallback: callback, errorCallback: callback});
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

export interface OpenCallbacks {
    openCallback: () => void;
    errorCallback: (_: Error) => void;
}

export interface ResponseCallbacks {
    responseCallback: (_: proto.Response) => void;
    errorCallback: (_: Error) => void;
}

interface ResponseState extends ResponseCallbacks {
    type: string;
}

export interface StreamState {
    streamId: number;
    closed: Error | undefined;
}

export interface SqlState {
    sqlId: number;
    closed: Error | undefined;
}
