import type { Response } from "@libsql/isomorphic-fetch";
import { fetch, Request, Headers } from "@libsql/isomorphic-fetch";

import type { ProtocolVersion, ProtocolEncoding } from "../client.js";
import { Client } from "../client.js";
import { ClosedError, ProtocolVersionError } from "../errors.js";

import { HttpStream } from "./stream.js";

export type Endpoint = {
    versionPath: string,
    pipelinePath: string,
    cursorPath: string | undefined,
    version: ProtocolVersion,
    encoding: ProtocolEncoding,
};

const checkEndpoints: Array<Endpoint> = [
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

    #closed: boolean;
    #streams: Set<HttpStream>;

    #versionPromise: Promise<ProtocolVersion> | undefined;
    #versionReady: boolean;
    /** @private */
    _endpoint: Endpoint;

    /** @private */
    constructor(url: URL, jwt: string | undefined, customFetch: unknown | undefined) {
        super();
        this.#url = url;
        this.#jwt = jwt;
        this.#fetch = (customFetch as typeof fetch) ?? fetch;

        this.#closed = false;
        this.#streams = new Set();

        this.#versionPromise = undefined;
        this.#versionReady = false;
        this._endpoint = fallbackEndpoint;
    }

    /** Get the protocol version supported by the server. */
    override getVersion(): Promise<ProtocolVersion> {
        if (this.#versionPromise !== undefined) {
            return this.#versionPromise;
        }
        const promise = this.#findEndpoint().then((endpoint) => {
            this.#versionReady = true;
            this._endpoint = endpoint;
            return endpoint.version;
        });
        this.#versionPromise = promise;
        return promise;
    }

    async #findEndpoint(): Promise<Endpoint> {
        const fetch = this.#fetch;
        for (const endpoint of checkEndpoints) {
            const url = new URL(endpoint.versionPath, this.#url);
            const request = new Request(url.toString(), {method: "GET"});

            const response = await fetch(request);
            await response.arrayBuffer();
            if (response.ok) {
                return endpoint;
            }
        }
        return fallbackEndpoint;
    }

    // Make sure that the negotiated version is at least `minVersion`.
    /** @private */
    override _ensureVersion(minVersion: ProtocolVersion, feature: string): void {
        if (minVersion <= fallbackEndpoint.version) {
            return;
        } else if (!this.#versionReady) {
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
        if (this.#closed) {
            throw new ClosedError("Client is closed", undefined);
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
        this.#closed = true;
        for (const stream of Array.from(this.#streams)) {
            stream._closeFromClient();
        }
    }

    /** True if the client is closed. */
    override get closed(): boolean {
        return this.#closed;
    }
}
