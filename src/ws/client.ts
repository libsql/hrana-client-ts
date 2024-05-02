import { WebSocket } from "@libsql/isomorphic-ws";

import type { ProtocolVersion, ProtocolEncoding, ClientConfig } from "../client.js";
import { Client } from "../client.js";
import {
    readJsonObject, writeJsonObject, readProtobufMessage, writeProtobufMessage,
} from "../encoding/index.js";
import {
    ClientError, ProtoError, ClosedError, WebSocketError, ProtocolVersionError,
    InternalError, MisuseError,
} from "../errors.js";
import { IdAlloc } from "../id_alloc.js";
import { errorFromProto } from "../result.js";
import { Sql, SqlOwner } from "../sql.js";
import { impossible } from "../util.js";

import type * as proto from "./proto.js";
import { WsStream } from "./stream.js";

import { ClientMsg as json_ClientMsg } from "./json_encode.js";
import { ClientMsg as protobuf_ClientMsg } from "./protobuf_encode.js";
import { ServerMsg as json_ServerMsg } from "./json_decode.js";
import { ServerMsg as protobuf_ServerMsg } from "./protobuf_decode.js";

export type Subprotocol = {
    version: ProtocolVersion,
    encoding: ProtocolEncoding,
};

export const subprotocolsV2: Map<string, Subprotocol> = new Map([
    ["hrana2", {version: 2, encoding: "json"}],
    ["hrana1", {version: 1, encoding: "json"}],
]);

export const subprotocolsV3: Map<string, Subprotocol> = new Map([
    ["hrana3-protobuf", {version: 3, encoding: "protobuf"}],
    ["hrana3", {version: 3, encoding: "json"}],
    ["hrana2", {version: 2, encoding: "json"}],
    ["hrana1", {version: 1, encoding: "json"}],
]);

/** A client for the Hrana protocol over a WebSocket. */
export class WsClient extends Client implements SqlOwner {
    #socket: WebSocket;
    // List of callbacks that we queue until the socket transitions from the CONNECTING to the OPEN state.
    #openCallbacks: Array<OpenCallbacks>;
    // Have we already transitioned from CONNECTING to OPEN and fired the callbacks in #openCallbacks?
    #opened: boolean;
    // Stores the error that caused us to close the client (and the socket). If we are not closed, this is
    // `undefined`.
    #closed: Error | undefined;

    // Have we received a response to our "hello" from the server?
    #recvdHello: boolean;
    // Subprotocol negotiated with the server. It is only available after the socket transitions to the OPEN
    // state.
    #subprotocol: Subprotocol | undefined;
    // Has the `getVersion()` function been called? This is only used to validate that the API is used
    // correctly.
    #getVersionCalled: boolean;
    // A map from request id to the responses that we expect to receive from the server.
    #responseMap: Map<number, ResponseState>;
    // An allocator of request ids.
    #requestIdAlloc: IdAlloc;

    // An allocator of stream ids.
    /** @private */
    _streamIdAlloc: IdAlloc;
    // An allocator of cursor ids.
    /** @private */
    _cursorIdAlloc: IdAlloc;
    // An allocator of SQL text ids.
    #sqlIdAlloc: IdAlloc;

    /** @private */
    constructor(socket: WebSocket, jwt: string | undefined, config: ClientConfig) {
        super(config);

        this.#socket = socket;
        this.#openCallbacks = [];
        this.#opened = false;
        this.#closed = undefined;

        this.#recvdHello = false;
        this.#subprotocol = undefined;
        this.#getVersionCalled = false;
        this.#responseMap = new Map();

        this.#requestIdAlloc = new IdAlloc();
        this._streamIdAlloc = new IdAlloc();
        this._cursorIdAlloc = new IdAlloc();
        this.#sqlIdAlloc = new IdAlloc();

        this.#socket.binaryType = "arraybuffer";
        this.#socket.addEventListener("open", () => this.#onSocketOpen());
        this.#socket.addEventListener("close", (event) => this.#onSocketClose(event));
        this.#socket.addEventListener("error", (event) => this.#onSocketError(event));
        this.#socket.addEventListener("message", (event) => this.#onSocketMessage(event));

        this.#send({type: "hello", jwt});
    }

    // Send (or enqueue to send) a message to the server.
    #send(msg: proto.ClientMsg): void {
        if (this.#closed !== undefined) {
            throw new InternalError("Trying to send a message on a closed client");
        }

        if (this.#opened) {
            this.#sendToSocket(msg);
        } else {
            const openCallback = () => this.#sendToSocket(msg);
            const errorCallback = () => undefined;
            this.#openCallbacks.push({openCallback, errorCallback});
        }
    }

    // The socket transitioned from CONNECTING to OPEN
    #onSocketOpen(): void {
        const protocol = this.#socket.protocol;
        if (protocol === undefined) {
            this.#setClosed(new ClientError(
                "The `WebSocket.protocol` property is undefined. This most likely means that the WebSocket " +
                    "implementation provided by the environment is broken. If you are using Miniflare 2, " +
                    "please update to Miniflare 3, which fixes this problem."
            ));
            return;
        } else if (protocol === "") {
            this.#subprotocol = {version: 1, encoding: "json"};
        } else {
            this.#subprotocol = subprotocolsV3.get(protocol);
            if (this.#subprotocol === undefined) {
                this.#setClosed(new ProtoError(
                    `Unrecognized WebSocket subprotocol: ${JSON.stringify(protocol)}`,
                ));
                return;
            }
        }

        for (const callbacks of this.#openCallbacks) {
            callbacks.openCallback();
        }
        this.#openCallbacks.length = 0;
        this.#opened = true;
    }

    #sendToSocket(msg: proto.ClientMsg): void {
        const encoding = this.#subprotocol!.encoding;
        if (encoding === "json") {
            const jsonMsg = writeJsonObject(msg, json_ClientMsg);
            this.#socket.send(jsonMsg);
        } else if (encoding === "protobuf") {
            const protobufMsg = writeProtobufMessage(msg, protobuf_ClientMsg);
            this.#socket.send(protobufMsg);
        } else {
            throw impossible(encoding, "Impossible encoding");
        }
    }

    /** Get the protocol version negotiated with the server, possibly waiting until the socket is open. */
    override getVersion(): Promise<ProtocolVersion> {
        return new Promise((versionCallback, errorCallback) => {
            this.#getVersionCalled = true;
            if (this.#closed !== undefined) {
                errorCallback(this.#closed);
            } else if (!this.#opened) {
                const openCallback = () => versionCallback(this.#subprotocol!.version);
                this.#openCallbacks.push({openCallback, errorCallback});
            } else {
                versionCallback(this.#subprotocol!.version);
            }
        });
    }

    // Make sure that the negotiated version is at least `minVersion`.
    /** @private */
    override _ensureVersion(minVersion: ProtocolVersion, feature: string): void {
        if (this.#subprotocol === undefined || !this.#getVersionCalled) {
            throw new ProtocolVersionError(
                `${feature} is supported only on protocol version ${minVersion} and higher, ` +
                    "but the version supported by the WebSocket server is not yet known. " +
                    "Use Client.getVersion() to wait until the version is available.",
            );
        } else if (this.#subprotocol.version < minVersion) {
            throw new ProtocolVersionError(
                `${feature} is supported on protocol version ${minVersion} and higher, ` +
                    `but the WebSocket server only supports version ${this.#subprotocol.version}`
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
        this.#send({type: "request", requestId, request});
    }

    // The socket encountered an error.
    #onSocketError(event: Event | WebSocket.ErrorEvent): void {
        const eventMessage = (event as {message?: string}).message;
        const message = eventMessage ?? "WebSocket was closed due to an error";
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
        if (this.#closed !== undefined) {
            return;
        }

        try {
            let msg: proto.ServerMsg;

            const encoding = this.#subprotocol!.encoding;
            if (encoding === "json") {
                if (typeof event.data !== "string") {
                    this.#socket.close(3003, "Only text messages are accepted with JSON encoding");
                    this.#setClosed(new ProtoError(
                        "Received non-text message from server with JSON encoding"))
                    return;
                }
                msg = readJsonObject(JSON.parse(event.data), json_ServerMsg);
            } else if (encoding === "protobuf") {
                if (!(event.data instanceof ArrayBuffer)) {
                    this.#socket.close(3003, "Only binary messages are accepted with Protobuf encoding");
                    this.#setClosed(new ProtoError(
                        "Received non-binary message from server with Protobuf encoding"))
                    return;
                }
                msg = readProtobufMessage(new Uint8Array(event.data), protobuf_ServerMsg);
            } else {
                throw impossible(encoding, "Impossible encoding");
            }

            this.#handleMsg(msg);
        } catch (e) {
            this.#socket.close(3007, "Could not handle message");
            this.#setClosed(e as Error);
        }
    }

    // Handle a message from the server.
    #handleMsg(msg: proto.ServerMsg): void {
        if (msg.type === "none") {
            throw new ProtoError("Received an unrecognized ServerMsg");
        } else if (msg.type === "hello_ok" || msg.type === "hello_error") {
            if (this.#recvdHello) {
                throw new ProtoError("Received a duplicated hello response");
            }
            this.#recvdHello = true;

            if (msg.type === "hello_error") {
                throw errorFromProto(msg.error);
            }
            return;
        } else if (!this.#recvdHello) {
            throw new ProtoError("Received a non-hello message before a hello response");
        }

        if (msg.type === "response_ok") {
            const requestId = msg.requestId;
            const responseState = this.#responseMap.get(requestId);
            this.#responseMap.delete(requestId);

            if (responseState === undefined) {
                throw new ProtoError("Received unexpected OK response");
            }
            this.#requestIdAlloc.free(requestId);

            try {
                if (responseState.type !== msg.response.type) {
                    console.dir({responseState, msg});
                    throw new ProtoError("Received unexpected type of response");
                }
                responseState.responseCallback(msg.response);
            } catch (e) {
                responseState.errorCallback(e as Error);
                throw e;
            }
        } else if (msg.type === "response_error") {
            const requestId = msg.requestId;
            const responseState = this.#responseMap.get(requestId);
            this.#responseMap.delete(requestId);

            if (responseState === undefined) {
                throw new ProtoError("Received unexpected error response");
            }
            this.#requestIdAlloc.free(requestId);

            responseState.errorCallback(errorFromProto(msg.error));
        } else {
            throw impossible(msg, "Impossible ServerMsg type");
        }
    }

    /** Open a {@link WsStream}, a stream for executing SQL statements. */
    override openStream(): WsStream {
        return WsStream.open(this);
    }

    /** Cache a SQL text on the server. This requires protocol version 2 or higher. */
    storeSql(sql: string): Sql {
        this._ensureVersion(2, "storeSql()");

        const sqlId = this.#sqlIdAlloc.alloc();
        const sqlObj = new Sql(this, sqlId);

        const responseCallback = () => undefined;
        const errorCallback = (e: Error) => sqlObj._setClosed(e);

        const request: proto.StoreSqlReq = {type: "store_sql", sqlId, sql};
        this._sendRequest(request, {responseCallback, errorCallback});
        return sqlObj;
    }

    /** @private */
    _closeSql(sqlId: number): void {
        if (this.#closed !== undefined) {
            return;
        }

        const responseCallback = () => this.#sqlIdAlloc.free(sqlId);
        const errorCallback = (e: Error) => this.#setClosed(e);
        const request: proto.CloseSqlReq = {type: "close_sql", sqlId};
        this._sendRequest(request, {responseCallback, errorCallback});
    }

    /** Close the client and the WebSocket. */
    override close(): void {
        this.#setClosed(new ClientError("Client was manually closed"));
    }

    /** True if the client is closed. */
    override get closed(): boolean {
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
