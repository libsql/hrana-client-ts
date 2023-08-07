import type { fetch, Response } from "@libsql/isomorphic-fetch";
import { Request, Headers } from "@libsql/isomorphic-fetch";

import { ClientError, HttpServerError, ProtoError, ClosedError } from "../errors.js";
import {
    readJsonObject, writeJsonObject, readProtobufMessage, writeProtobufMessage,
} from "../encoding/index.js";
import { IdAlloc } from "../id_alloc.js";
import { queueMicrotask } from "../ponyfill.js";
import { errorFromProto } from "../result.js";
import type { SqlOwner, SqlState, ProtoSql } from "../sql.js";
import { Sql } from "../sql.js";
import { Stream } from "../stream.js";
import { impossible } from "../util.js";

import type { HttpClient, Endpoint } from "./client.js";
import type * as proto from "./proto.js";

import { PipelineRequestBody as json_PipelineRequestBody } from "./json_encode.js";
import { PipelineRequestBody as protobuf_PipelineRequestBody } from "./protobuf_encode.js";
import { PipelineResponseBody as json_PipelineResponseBody } from "./json_decode.js";
import { PipelineResponseBody as protobuf_PipelineResponseBody } from "./protobuf_decode.js";

type PipelineEntry = {
    request: proto.StreamRequest;
    responseCallback: (_: proto.StreamResponse) => void;
    errorCallback: (_: Error) => void;
}

export class HttpStream extends Stream implements SqlOwner {
    #client: HttpClient;
    #baseUrl: string;
    #jwt: string | undefined;
    #fetch: typeof fetch;

    #closed: Error | undefined;
    #baton: string | undefined;
    #pipeline: Array<PipelineEntry>;
    #pipelineInProgress: boolean;

    #sqlIdAlloc: IdAlloc;

    /** @private */
    constructor(client: HttpClient, baseUrl: URL, jwt: string | undefined, customFetch: typeof fetch) {
        super(client.intMode);
        this.#client = client;
        this.#baseUrl = baseUrl.toString();
        this.#jwt = jwt;
        this.#fetch = customFetch;

        this.#closed = undefined;
        this.#baton = undefined;
        this.#pipeline = [];
        this.#pipelineInProgress = false;

        this.#sqlIdAlloc = new IdAlloc();
    }

    /** Get the {@link HttpClient} object that this stream belongs to. */
    override client(): HttpClient {
        return this.#client;
    }

    /** @private */
    override _sqlOwner(): SqlOwner {
        return this;
    }

    /** Cache a SQL text on the server. */
    storeSql(sql: string): Sql {
        const sqlId = this.#sqlIdAlloc.alloc();
        const sqlState = {
            sqlId,
            closed: undefined,
        };

        this.#sendStreamRequest({type: "store_sql", sqlId, sql}).then(
            () => undefined,
            (error) => this.#setClosed(error),
        );

        return new Sql(this, sqlState);
    }

    /** @private */
    _closeSql(sqlState: SqlState, error: Error): void {
        if (sqlState.closed !== undefined || this.#closed !== undefined) {
            return;
        }
        sqlState.closed = error;

        this.#sendStreamRequest({type: "close_sql", sqlId: sqlState.sqlId}).then(
            () => this.#sqlIdAlloc.free(sqlState.sqlId),
            (error) => this.#setClosed(error),
        );
    }

    /** @private */
    override _execute(stmt: proto.Stmt): Promise<proto.StmtResult> {
        return this.#sendStreamRequest({type: "execute", stmt}).then((response) => {
            return (response as proto.ExecuteStreamResp).result;
        });
    }

    /** @private */
    override _batch(batch: proto.Batch): Promise<proto.BatchResult> {
        return this.#sendStreamRequest({type: "batch", batch}).then((response) => {
            return (response as proto.BatchStreamResp).result;
        });
    }

    /** @private */
    override _describe(protoSql: ProtoSql): Promise<proto.DescribeResult> {
        return this.#sendStreamRequest({
            type: "describe",
            sql: protoSql.sql,
            sqlId: protoSql.sqlId
        }).then((response) => {
            return (response as proto.DescribeStreamResp).result;
        });
    }

    /** @private */
    override _sequence(protoSql: ProtoSql): Promise<void> {
        return this.#sendStreamRequest({
            type: "sequence",
            sql: protoSql.sql,
            sqlId: protoSql.sqlId,
        }).then((_response) => {
            return undefined;
        });
    }

    /** Check whether the SQL connection underlying this stream is in autocommit state (i.e., outside of an
     * explicit transaction). This requires protocol version 3 or higher.
     */
    override getAutocommit(): Promise<boolean> {
        this.#client._ensureVersion(3, "getAutocommit()");
        return this.#sendStreamRequest({
            type: "get_autocommit",
        }).then((response) => {
            return (response as proto.GetAutocommitStreamResp).isAutocommit;
        });
    }

    /** Close the stream. */
    override close(): void {
        this.#setClosed(new ClientError("Stream was manually closed"));
    }

    /** @private */
    _closeFromClient(): void {
        this.#setClosed(new ClosedError("Client was closed", undefined));
    }

    /** True if the stream is closed. */
    override get closed(): boolean {
        return this.#closed !== undefined;
    }

    #setClosed(error: Error): void {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;
        this.#client._streamClosed(this);

        if (this.#baton !== undefined || this.#pipeline.length !== 0 || this.#pipelineInProgress) {
            this.#pipeline.push({
                request: {type: "close"},
                responseCallback() {},
                errorCallback() {},
            });
            this.#flushPipeline();
        }
    }

    #sendStreamRequest(request: proto.StreamRequest): Promise<proto.StreamResponse> {
        if (this.#closed !== undefined) {
            return Promise.reject(new ClosedError("Stream is closed", this.#closed));
        }
        return new Promise((responseCallback, errorCallback) => {
            this.#pipeline.push({request, responseCallback, errorCallback});
            queueMicrotask(() => this.#flushPipeline());
        });
    }

    #flushPipeline(): void {
        if (this.#pipeline.length === 0 || this.#pipelineInProgress) {
            return;
        }

        const pipeline = Array.from(this.#pipeline);
        const endpoint = this.#client._endpoint;

        let promise;
        try {
            const request = this.#createPipelineRequest(pipeline, endpoint);
            const fetch = this.#fetch;
            promise = fetch(request);
        } catch (error) {
            promise = Promise.reject(error);
        }

        this.#pipelineInProgress = true;
        this.#pipeline.length = 0;

        promise.then((resp: Response): Promise<proto.PipelineResponseBody> => {
            if (!resp.ok) {
                return errorFromResponse(resp).then((error) => {
                    throw error;
                });
            }
            return decodePipelineResponse(resp, endpoint);
        }).then((respBody) => {
            this.#baton = respBody.baton;
            this.#baseUrl = respBody.baseUrl ?? this.#baseUrl;
            handlePipelineResponse(pipeline, respBody);
        }).catch((error) => {
            this.#setClosed(error);
            for (const entry of pipeline) {
                entry.errorCallback(error);
            }
        }).finally(() => {
            this.#pipelineInProgress = false;
            this.#flushPipeline();
        });
    }

    #createPipelineRequest(pipeline: Array<PipelineEntry>, endpoint: Endpoint): Request {
        const url = new URL(endpoint.pipelinePath, this.#baseUrl);
        const requestBody: proto.PipelineRequestBody = {
            baton: this.#baton,
            requests: pipeline.map((entry) => entry.request),
        };
        const [body, contentType] = encodePipelineRequest(requestBody, endpoint);
        const jsonBody = writeJsonObject(requestBody, json_PipelineRequestBody);

        const headers = new Headers();
        headers.set("content-type", contentType);
        if (this.#jwt !== undefined) {
            headers.set("authorization", `Bearer ${this.#jwt}`);
        }

        return new Request(url, {method: "POST", headers, body});
    }
}

function handlePipelineResponse(pipeline: Array<PipelineEntry>, respBody: proto.PipelineResponseBody): void {
    if (respBody.results.length !== pipeline.length) {
        throw new ProtoError("Server returned unexpected number of pipeline results");
    }

    for (let i = 0; i < pipeline.length; ++i) {
        const result = respBody.results[i];
        const entry = pipeline[i];

        if (result.type === "ok") {
            if (result.response.type !== entry.request.type) {
                throw new ProtoError("Received unexpected type of response");
            }
            entry.responseCallback(result.response);
        } else if (result.type === "error") {
            entry.errorCallback(errorFromProto(result.error));
        } else if (result.type === "none") {
            throw new ProtoError("Received unrecognized StreamResult");
        } else {
            throw impossible(result, "Received impossible type of StreamResult");
        }
    }
}

async function decodePipelineResponse(
    resp: Response,
    endpoint: Endpoint,
): Promise<proto.PipelineResponseBody> {
    if (endpoint.encoding === "json") {
        const respJson = await resp.json();
        return readJsonObject(respJson, json_PipelineResponseBody);
    } else if (endpoint.encoding === "protobuf") {
        const respData = await resp.arrayBuffer();
        return readProtobufMessage(new Uint8Array(respData), protobuf_PipelineResponseBody);
    } else {
        throw impossible(endpoint.encoding, "Impossible encoding");
    }
}

function encodePipelineRequest(
    body: proto.PipelineRequestBody,
    endpoint: Endpoint,
): [string | Uint8Array, string] {
    if (endpoint.encoding === "json") {
        const data = writeJsonObject(body, json_PipelineRequestBody);
        return [data, "application/json"];
    } else if (endpoint.encoding === "protobuf") {
        const data = writeProtobufMessage(body, protobuf_PipelineRequestBody);
        return [data, "application/x-protobuf"];
    } else {
        throw impossible(endpoint.encoding, "Impossible encoding");
    }
}

async function errorFromResponse(resp: Response): Promise<Error> {
    const respType = resp.headers.get("content-type") ?? "text/plain";
    if (respType === "application/json") {
        const respBody = await resp.json();
        if ("message" in respBody) {
            return errorFromProto(respBody as proto.Error);
        }
    }

    let message = `Server returned HTTP status ${resp.status}`;
    if (respType === "text/plain") {
        const respBody = (await resp.text()).trim();
        if (respBody !== "") {
            message += `: ${respBody}`;
        }
    }

    if (resp.status === 404) {
        message += ". It seems that the libsql server is outdated, please try updating the database.";
    }

    return new HttpServerError(message, resp.status);
}
