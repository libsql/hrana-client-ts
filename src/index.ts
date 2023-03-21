import WebSocket from "isomorphic-ws";

import { ClientError, ProtoError, ClosedError } from "./errors.js";
import IdAlloc from "./id_alloc.js";
import { ProgOp, ProgExpr, ProgVar } from "./prog.js";
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

export * from "./errors.js";
export { ProgOp, ProgExpr, ProgVar } from "./prog.js";
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
            }

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
        };
        this.#sendRequest(request, {responseCallback, errorCallback});
    }

    // Execute a program on a stream and invoke callbacks in `progState` when we get the results (or an
    // error).
    /** @private */
    _prog(streamState: StreamState, progState: ProgState): void {
        const responseCallback = (response: proto.Response) => {
            const result = (response as proto.ProgResp)["result"];
            progState.resultCallbacks.forEach(callback => callback(result));
        };
        const errorCallback = (error: Error) => {
            progState.errorCallbacks.forEach(callback => callback(error));
        };

        if (streamState.closed !== undefined) {
            errorCallback(new ClosedError("Stream was closed", streamState.closed));
            return;
        }

        const request: proto.ProgReq = {
            "type": "prog",
            "stream_id": streamState.streamId,
            "prog": progState.prog,
        };
        this.#sendRequest(request, {responseCallback, errorCallback});
    }

    /** Close the client and the WebSocket. */
    close() {
        this.#setClosed(new ClientError("Client was manually closed"));
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
    resultCallback: (_: proto.StmtResult) => void;
    errorCallback: (_: Error) => void;
}

interface StreamState {
    streamId: number;
    closed: Error | undefined;
}

interface ProgState {
    prog: proto.Prog;
    resultCallbacks: Array<(_: proto.ProgResult) => void>;
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
        return this.#execute(stmtToProto(stmt, true), rowsResultFromProto);
    }

    /** Execute a statement and return at most a single row. */
    queryRow(stmt: InStmt): Promise<RowResult> {
        return this.#execute(stmtToProto(stmt, true), rowResultFromProto);
    }

    /** Execute a statement and return at most a single value. */
    queryValue(stmt: InStmt): Promise<ValueResult> {
        return this.#execute(stmtToProto(stmt, true), valueResultFromProto);
    }

    /** Execute a statement without returning rows. */
    run(stmt: InStmt): Promise<StmtResult> {
        return this.#execute(stmtToProto(stmt, false), stmtResultFromProto);
    }

    #execute<T>(stmt: proto.Stmt, fromProto: (result: proto.StmtResult) => T): Promise<T> {
        return new Promise((doneCallback, errorCallback) => {
            this.#client._execute(this.#state, {
                stmt,
                resultCallback(result) {
                    doneCallback(fromProto(result));
                },
                errorCallback,
            });
        });
    }

    /** Return a builder for creating and executing a program. */
    prog(): Prog {
        return new Prog(this.#client, this.#state);
    }

    /** Close the stream. */
    close(): void {
        this.#client._closeStream(this.#state, new ClientError("Stream was manually closed"));
    }
}

/** A builder for creating a program and executing it on the server. */
export class Prog {
    #client: Client;
    #streamState: StreamState;

    /** @private */
    _steps: Array<proto.ProgStep>;
    /** @private */
    _resultCallbacks: Array<(_: proto.ProgResult) => void>;
    /** @private */
    _errorCallbacks: Array<(_: Error) => void>;

    /** @private */
    _executeCount: number;
    #outputCount: number;
    #varAlloc: IdAlloc;

    /** @private */
    constructor(client: Client, streamState: StreamState) {
        this.#client = client;
        this.#streamState = streamState;

        this._steps = [];
        this._resultCallbacks = [];
        this._errorCallbacks = [];

        this._executeCount = 0;
        this.#outputCount = 0;
        this.#varAlloc = new IdAlloc();
    }

    /** Return a builder for executing a statement in the program. */
    execute(): ProgExecute {
        return new ProgExecute(this);
    }

    /** Add an expression to the program. */
    output(expr: ProgExpr): Promise<Value> {
        const outputIdx = this.#outputCount++;
        this._steps.push({
            "type": "output",
            "expr": expr._proto,
        });

        return new Promise((valueCallback, errorCallback) => {
            this._resultCallbacks.push((result) => {
                valueCallback(valueFromProto(result["outputs"][outputIdx]));
            });
            this._errorCallbacks.push(errorCallback);
        });
    }

    /** Add a sequence of operations to the program. */
    ops(ops: Array<ProgOp>): void {
        this._steps.push({
            "type": "op",
            "ops": ops.map(op => op._proto),
        });
    }

    /** Add a single operation to the program. */
    op(op: ProgOp): void {
        this.ops([op]);
    }

    /** Allocate a fresh var. */
    allocVar(): ProgVar {
        return new ProgVar(this.#varAlloc.alloc());
    }

    /** Free a var allocated with `this.allocVar()`. */
    freeVar(var_: ProgVar): void {
        this.#varAlloc.free(var_._proto);
    }

    /** Run the program. */
    run(): Promise<void> {
        const promise = new Promise<void>((doneCallback, errorCallback) => {
            this._resultCallbacks.push((_result) => doneCallback(undefined));
            this._errorCallbacks.push(errorCallback);
        });

        const progState = {
            prog: {
                "steps": this._steps,
            },
            resultCallbacks: this._resultCallbacks,
            errorCallbacks: this._errorCallbacks,
        };
        this.#client._prog(this.#streamState, progState);

        return promise;
    }
}

/** A builder for adding a statement in a program. */
export class ProgExecute {
    #prog: Prog
    #condition: proto.ProgExpr | null;
    #onOk: Array<proto.ProgOp>;
    #onError: Array<proto.ProgOp>;

    /** @private */
    constructor(prog: Prog) {
        this.#prog = prog;
        this.#condition = null;
        this.#onOk = [];
        this.#onError = [];
    }

    /** Set the condition that needs to be satisfied to execute the statement. */
    condition(expr: ProgExpr): this {
        this.#condition = expr._proto;
        return this;
    }

    /** Add an operation to evaluate when the statement executed successfully. */
    onOk(op: ProgOp): this {
        this.#onOk.push(op._proto);
        return this;
    }

    /** Add an operation to evaluate when the statement failed to execute. */
    onError(op: ProgOp): this {
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

    #execute<T>(stmt: proto.Stmt, fromProto: (result: proto.StmtResult) => T): Promise<T | undefined> {
        const executeIdx = this.#prog._executeCount++;
        this.#prog._steps.push({
            "type": "execute",
            "stmt": stmt,
            "condition": this.#condition,
            "on_ok": this.#onOk,
            "on_error": this.#onError,
        });

        return new Promise((outputCallback, errorCallback) => {
            this.#prog._resultCallbacks.push((result) => {
                const executeResult = result["execute_results"][executeIdx];
                const executeError = result["execute_errors"][executeIdx];
                if (executeResult === undefined || executeError === undefined) {
                    errorCallback(new ProtoError("Server returned fewer results or errors than expected"));
                } else if (executeResult !== null && executeError !== null) {
                    errorCallback(new ProtoError("Server returned both result and error"));
                } else if (executeError !== null) {
                    errorCallback(errorFromProto(executeError));
                } else if (executeResult !== null) {
                    outputCallback(fromProto(executeResult));
                } else {
                    outputCallback(undefined);
                }
            });
            this.#prog._errorCallbacks.push(errorCallback);
        });
    }
}
