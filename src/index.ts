import WebSocket from "isomorphic-ws";

import { Op, Expr, Var } from "./compute.js";
import { ClientError, ProtoError, ClosedError } from "./errors.js";
import IdAlloc from "./id_alloc.js";
import type * as proto from "./proto.js";
import type { StmtResult, RowsResult, RowResult, ValueResult } from "./result.js";
import {
    rowsResultFromProto, rowResultFromProto,
    valueResultFromProto, stmtResultFromProto,
    errorFromProto,
} from "./result.js";
import type { InStmt } from "./stmt.js";
import { stmtToProto } from "./stmt.js";
import type { Value, InValue } from "./value.js";
import { valueFromProto } from "./value.js";

export { Op, Expr, Var } from "./compute.js";
export * from "./errors.js";
export type { StmtResult, RowsResult, RowResult, ValueResult, Row } from "./result.js";
export type { InStmt, InStmtArgs } from "./stmt.js";
export { Stmt } from "./stmt.js";
export type { Value, InValue } from "./value.js";
export type { proto };

/** Open a Hrana client connected to the given `url`. */
export function open(url: string, jwt?: string): Client {
    const socket = new WebSocket(url, ["hrana1"]);
    return new Client(socket, jwt ?? null);
}

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
    // An allocator of vars.
    #varAlloc: IdAlloc;

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
        this.#varAlloc = new IdAlloc();

        this.#socket.onopen = () => this.#onSocketOpen();
        this.#socket.onclose = (event) => this.#onSocketClose(event);
        this.#socket.onerror = (event) => this.#onSocketError(event);
        this.#socket.onmessage = (event) => this.#onSocketMessage(event);

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
        this.#setClosed(new ClientError(message));
    }

    // The socket was closed.
    #onSocketClose(event: WebSocket.CloseEvent): void {
        this.#setClosed(new ClientError(`WebSocket was closed with code ${event.code}: ${event.reason}`));
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
            } else if (responseState.type !== msg["response"]["type"]) {
                throw new ProtoError("Received unexpected type of response");
            }

            try {
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

    // Execute a statement on a stream and invoke callbacks in `stmtState` when we get the results (or an
    // error).
    /** @private */
    _execute(streamState: StreamState, stmtState: StmtState): void {
        const responseCallback = (response: proto.Response) => {
            stmtState.resultCallback((response as proto.ExecuteResp)["result"]);
        };
        const errorCallback = (error: Error) => {
            stmtState.errorCallback(error);
        }

        if (streamState.closed !== undefined) {
            errorCallback(new ClosedError("Stream was closed", streamState.closed));
            return;
        }

        const request: proto.ExecuteReq = {
            "type": "execute",
            "stream_id": streamState.streamId,
            "stmt": stmtState.stmt,
            "condition": stmtState.condition,
            "on_ok": stmtState.onOk,
            "on_error": stmtState.onError,
        };
        this.#sendRequest(request, {responseCallback, errorCallback});
    }

    /** Return a builder for creating and executing a sequence of compute operations. */
    compute(): Compute {
        return new Compute(this);
    }

    // Execute a compute request and invoke callbacks in `computeState` when we get the results (or an error).
    /** @private */
    _compute(computeState: ComputeState): void {
        const errorCallback = (error: Error) => {
            computeState.errorCallbacks.forEach((callback) => callback(error));
        };

        const responseCallback = (response: proto.Response) => {
            const results = (response as proto.ComputeResp)["results"];
            if (results.length !== computeState.resultCallbacks.length) {
                errorCallback(new ProtoError("Received wrong number of compute results"));
                return;
            }

            computeState.doneCallback();
            computeState.resultCallbacks.forEach((callback, i) => callback(results[i]));
        };

        const request: proto.ComputeReq = {
            "type": "compute",
            "ops": computeState.ops,
        };
        this.#sendRequest(request, {responseCallback, errorCallback});
    }

    /** Close the client and the WebSocket. */
    close() {
        this.#setClosed(new ClientError("Client was manually closed"));
    }

    /** Allocate a fresh var. */
    allocVar(): Var {
        return new Var(this.#varAlloc.alloc());
    }

    /** Free a var allocated with `this.allocVar()`. */
    freeVar(var_: Var): void {
        this.#varAlloc.free(var_._proto);
    }
}

interface ResponseCallbacks {
    responseCallback: (_: proto.Response) => void;
    errorCallback: (_: Error) => void;
}

interface ResponseState extends ResponseCallbacks {
    type: string;
}

interface StmtState {
    stmt: proto.Stmt;
    condition: proto.ComputeExpr | null;
    onOk: Array<proto.ComputeOp>;
    onError: Array<proto.ComputeOp>;
    resultCallback: (_: proto.StmtResult | null) => void;
    errorCallback: (_: Error) => void;
}

interface StreamState {
    streamId: number;
    closed: Error | undefined;
}

interface ComputeState {
    ops: Array<proto.ComputeOp>;
    doneCallback: () => void;
    resultCallbacks: Array<(_: proto.Value) => void>;
    errorCallbacks: Array<(_: Error) => void>;
}

/** A stream for executing SQL statements (a "database connection"). */
export class Stream {
    #client: Client;
    #state: StreamState;

    /** @private */
    constructor(client: Client, state: StreamState) {
        this.#client = client;
        this.#state = state;
    }

    /** Execute a statement and return rows. */
    query(stmt: InStmt): Promise<RowsResult> {
        return this.execute().query(stmt).then(assertResult);
    }

    /** Execute a statement and return at most a single row. */
    queryRow(stmt: InStmt): Promise<RowResult> {
        return this.execute().queryRow(stmt).then(assertResult);
    }

    /** Execute a statement and return at most a single value. */
    queryValue(stmt: InStmt): Promise<ValueResult> {
        return this.execute().queryValue(stmt).then(assertResult);
    }

    /** Execute a statement without returning rows. */
    run(stmt: InStmt): Promise<StmtResult> {
        return this.execute().run(stmt).then(assertResult);
    }

    /** Execute a raw Hrana statement. */
    executeRaw(stmt: proto.Stmt): Promise<proto.StmtResult> {
        return this.execute().executeRaw(stmt).then(assertResult);
    }

    /** Return a builder that you can use to execute a statement conditionally. */
    execute(): Execute {
        return new Execute(this.#client, this.#state);
    }

    /** Close the stream. */
    close(): void {
        this.#client._closeStream(this.#state, new ClientError("Stream was manually closed"));
    }
}

function assertResult<T>(x: T | undefined): T {
    if (x === undefined) {
        throw new ProtoError("Server did not return a result");
    }
    return x;
}

/** A builder for executing a statement with a condition and optional compute operations on success or
* failure. */
export class Execute {
    #client: Client;
    #state: StreamState;

    #condition: proto.ComputeExpr | null;
    #onOk: Array<proto.ComputeOp>;
    #onError: Array<proto.ComputeOp>;

    /** @private */
    constructor(client: Client, state: StreamState) {
        this.#client = client;
        this.#state = state;

        this.#condition = null;
        this.#onOk = [];
        this.#onError = [];
    }

    /** Set the condition that needs to be satisfied to execute the statement. */
    condition(expr: Expr): this {
        this.#condition = expr._proto;
        return this;
    }

    /** Add an operation to evaluate when the statement executed successfully. */
    onOk(op: Op): this {
        this.#onOk.push(op._proto);
        return this;
    }

    /** Add an operation to evaluate when the statement failed to execute. */
    onError(op: Op): this {
        this.#onError.push(op._proto);
        return this;
    }


    /** Execute a statement and return rows. */
    query(stmt: InStmt): Promise<RowsResult | undefined> {
        return this.#execute(stmtToProto(stmt, true), rowsResultFromProto);
    }

    /** Execute a statement and return at most a single row. */
    queryRow(stmt: InStmt): Promise<RowResult | undefined> {
        return this.#execute(stmtToProto(stmt, true), rowResultFromProto);
    }

    /** Execute a statement and return at most a single value. */
    queryValue(stmt: InStmt): Promise<ValueResult | undefined> {
        return this.#execute(stmtToProto(stmt, true), valueResultFromProto);
    }

    /** Execute a statement without returning rows. */
    run(stmt: InStmt): Promise<StmtResult | undefined> {
        return this.#execute(stmtToProto(stmt, false), stmtResultFromProto);
    }

    /** Execute a raw Hrana statement. */
    executeRaw(stmt: proto.Stmt): Promise<proto.StmtResult | undefined> {
        return this.#execute(stmt, (result) => result);
    }

    #execute<T>(stmt: proto.Stmt, fromProto: (result: proto.StmtResult) => T): Promise<T | undefined> {
        return new Promise((doneCallback, errorCallback) => {
            this.#client._execute(this.#state, {
                stmt,
                condition: this.#condition,
                onOk: this.#onOk,
                onError: this.#onError,
                resultCallback(result) {
                    doneCallback(result !== null ? fromProto(result) : undefined);
                },
                errorCallback,
            });
        });
    }
}

/** A builder for executing a sequence of compute operations. */
export class Compute {
    #client: Client;
    #ops: Array<proto.ComputeOp>;
    #resultCallbacks: Array<(_: proto.Value) => void>;
    #errorCallbacks: Array<(_: Error) => void>;

    /** @private */
    constructor(client: Client) {
        this.#client = client;
        this.#ops = [];
        this.#resultCallbacks = [];
        this.#errorCallbacks = [];
    }

    /** Enqueues a compute operation. The returned promise will resolve only after you call `send()`. */
    enqueue(op: Op): Promise<Value> {
        return new Promise((valueCallback, errorCallback) => {
            this.#ops.push(op._proto);
            this.#resultCallbacks.push((value) => valueCallback(valueFromProto(value)));
            this.#errorCallbacks.push(errorCallback);
        });
    }

    /** Enqueues a raw compute operation and returns the raw protocol value. */
    enqueueRaw(op: proto.ComputeOp): Promise<proto.Value> {
        return new Promise((valueCallback, errorCallback) => {
            this.#ops.push(op);
            this.#resultCallbacks.push(valueCallback);
            this.#errorCallbacks.push(errorCallback);
        });
    }

    /** Sends the compute operations to the server. */
    send(): Promise<void> {
        return new Promise((doneCallback, errorCallback) => {
            this.#errorCallbacks.push(errorCallback);
            this.#client._compute({
                ops: this.#ops,
                doneCallback,
                resultCallbacks: this.#resultCallbacks,
                errorCallbacks: this.#errorCallbacks,
            });
        });
    }
}
