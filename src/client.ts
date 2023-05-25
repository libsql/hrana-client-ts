import type { Stream } from "./stream.js";

export type ProtocolVersion = 1 | 2;
export const protocolVersions: Map<string, ProtocolVersion> = new Map([
    ["hrana2", 2],
    ["hrana1", 1],
]);

/** A client for the Hrana protocol (a "database connection pool"). */
export abstract class Client {
    /** @private */
    constructor() {}

    /** Get the protocol version negotiated with the server. */
    abstract getVersion(): Promise<ProtocolVersion>;

    /** Open a {@link Stream}, a stream for executing SQL statements. */
    abstract openStream(): Stream;

    /** Close the client. */
    abstract close(): void;

    /** True if the client is closed. */
    abstract get closed(): boolean;
}
