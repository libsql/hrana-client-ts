import type * as proto from "./proto.js";

/** Generic error produced by the Hrana client. */
export class ClientError extends Error {
    /** @private */
    constructor(message: string) {
        super(message);
        this.name = "ClientError";
    }
}

/** Error thrown when the server violates the protocol. */
export class ProtoError extends ClientError {
    /** @private */
    constructor(message: string) {
        super(message);
        this.name = "ProtoError";
    }
}

/** Error thrown when the server returns an error response. */
export class ResponseError extends ClientError {
    code: string | undefined;
    /** @internal */
    proto: proto.Error;

    /** @private */
    constructor(message: string, protoError: proto.Error) {
        super(message);
        this.name = "ResponseError";
        this.code = protoError["code"] ?? undefined;
        this.proto = protoError;
        this.stack = undefined;
    }
}

/** Error thrown when the client or stream is closed. */
export class ClosedError extends ClientError {
    /** @private */
    constructor(message: string, cause: Error) {
        super(`${message}: ${cause}`);
        this.cause = cause;
    }
}

/** Error thrown when the environment does not seem to support WebSockets. */
export class WebSocketUnsupportedError extends ClientError {
    /** @private */
    constructor(message: string) {
        super(message);
        this.name = "WebSocketUnsupportedError";
    }
}

/** Error thrown when we encounter a WebSocket error. */
export class WebSocketError extends ClientError {
    /** @private */
    constructor(message: string) {
        super(message);
        this.name = "WebSocketError";
    }
}

/** Error thrown when a libsql URL is not valid. */
export class LibsqlUrlParseError extends ClientError {
    /** @private */
    constructor(message: string) {
        super(message);
        this.name = "LibsqlUrlParseError";
    }
}
