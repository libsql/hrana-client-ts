import { fetch, Request } from "cross-fetch";

import type { ProtocolVersion, ProtocolEncoding, ClientConfig } from "../client.js";
import { Client } from "../client.js";
import { ClientError, ClosedError, ProtocolVersionError } from "../errors.js";

import { HttpStream } from "./stream.js";

export type Endpoint = {
    versionPath: string,
    pipelinePath: string,
    cursorPath: string | undefined,
    version: ProtocolVersion,
    encoding: ProtocolEncoding,
};

export const checkEndpoints: Array<Endpoint> = [
    {
        versionPath: "v3-protobuf",
        pipelinePath: "v3-protobuf/pipeline",
        cursorPath: "v3-protobuf/cursor",
        version: 3,
        encoding: "protobuf",
    },
    /*
    {
        versionPath: "v3",
        pipelinePath: "v3/pipeline",
        cursorPath: "v3/cursor",
        version: 3,
        encoding: "json",
    },
    */
];

const fallbackEndpoint: Endpoint = {
    versionPath: "v2",
    pipelinePath: "v2/pipeline",
    cursorPath: undefined,
    version: 2,
    encoding: "json",
};

/** A client for the Hrana protocol over HTTP. */
export class HttpClient extends Client {
    #url: URL;
    #jwt: string | undefined;
    #fetch: typeof fetch;

    #closed: Error | undefined;
    #streams: Set<HttpStream>;

    /** @private */
    _endpointPromise: Promise<Endpoint>;
    /** @private */
    _endpoint: Endpoint | undefined;

    /** @private */
    constructor(url: URL, jwt: string | undefined, customFetch: unknown | undefined, protocolVersion: ProtocolVersion = 2, config: ClientConfig) {
        super(config);
        this.#url = url;
        this.#jwt = jwt;
        this.#fetch = (customFetch as typeof fetch) ?? fetch;

        this.#closed = undefined;
        this.#streams = new Set();

        if (protocolVersion == 3) {
            this._endpointPromise = findEndpoint(this.#fetch, this.#url);
            this._endpointPromise.then(
                (endpoint) => this._endpoint = endpoint,
                (error) => this.#setClosed(error),
            );
        } else {
            this._endpointPromise = Promise.resolve(fallbackEndpoint);
            this._endpointPromise.then(
                (endpoint) => this._endpoint = endpoint,
                (error) => this.#setClosed(error),
            );
        }
    }

    /** Get the protocol version supported by the server. */
    override async getVersion(): Promise<ProtocolVersion> {
        if (this._endpoint !== undefined) {
            return this._endpoint.version;
        }
        return (await this._endpointPromise).version;
    }

    // Make sure that the negotiated version is at least `minVersion`.
    /** @private */
    override _ensureVersion(minVersion: ProtocolVersion, feature: string): void {
        if (minVersion <= fallbackEndpoint.version) {
            return;
        } else if (this._endpoint === undefined) {
            throw new ProtocolVersionError(
                `${feature} is supported only on protocol version ${minVersion} and higher, ` +
                    "but the version supported by the HTTP server is not yet known. " +
                    "Use Client.getVersion() to wait until the version is available.",
            );
        } else if (this._endpoint.version < minVersion) {
            throw new ProtocolVersionError(
                `${feature} is supported only on protocol version ${minVersion} and higher, ` +
                    `but the HTTP server only supports version ${this._endpoint.version}.`,
            );
        }
    }

    /** Open a {@link HttpStream}, a stream for executing SQL statements. */
    override openStream(): HttpStream {
        if (this.#closed !== undefined) {
            throw new ClosedError("Client is closed", this.#closed);
        }
        const stream = new HttpStream(this, this.#url, this.#jwt, this.#fetch);
        this.#streams.add(stream);
        return stream;
    }

    /** @private */
    _streamClosed(stream: HttpStream): void {
        this.#streams.delete(stream);
    }

    /** Close the client and all its streams. */
    override close(): void {
        this.#setClosed(new ClientError("Client was manually closed"));
    }

    /** True if the client is closed. */
    override get closed(): boolean {
        return this.#closed !== undefined;
    }

    #setClosed(error: Error): void {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;
        for (const stream of Array.from(this.#streams)) {
            stream._setClosed(new ClosedError("Client was closed", error));
        }
    }
}

async function findEndpoint(customFetch: typeof fetch, clientUrl: URL): Promise<Endpoint> {
    const fetch = customFetch;
    for (const endpoint of checkEndpoints) {
        const url = new URL(endpoint.versionPath, clientUrl);
        const request = new Request(url.toString(), {method: "GET"});

        const response = await fetch(request);
        await response.arrayBuffer();
        if (response.ok) {
            return endpoint;
        }
    }
    return fallbackEndpoint;
}

