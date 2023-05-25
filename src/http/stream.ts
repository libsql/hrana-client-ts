import { fetch, Request, Response, Headers } from "@libsql/isomorphic-fetch";

import { ClientError, HttpServerError, ProtoError, ClosedError } from "../errors.js";
import { IdAlloc } from "../id_alloc.js";
import { errorFromProto } from "../result.js";
import type { SqlOwner, SqlState, ProtoSql } from "../sql.js";
import { Sql } from "../sql.js";
import { Stream } from "../stream.js";

import type { HttpClient } from "./client.js";
import type * as proto from "./proto.js";

type PipelineEntry = {
    request: proto.StreamRequest;
    responseCallback: (_: proto.StreamResponse) => void;
    errorCallback: (_: Error) => void;
}

export class HttpStream extends Stream implements SqlOwner {
    #client: HttpClient;
    #baseUrl: string;
    #jwt: string | null;

    #closed: Error | undefined;
    #baton: string | null;
    #pipeline: Array<PipelineEntry>;
    #pipelineInProgress: boolean;

    #sqlIdAlloc: IdAlloc;

    /** @private */
    constructor(client: HttpClient, baseUrl: URL, jwt: string | null) {
        super();
        this.#client = client;
        this.#baseUrl = baseUrl.toString();
        this.#jwt = jwt;

        this.#closed = undefined;
        this.#baton = null;
        this.#pipeline = [];
        this.#pipelineInProgress = false;

        this.#sqlIdAlloc = new IdAlloc();
    }

    /** @private*/
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

        this.#sendStreamRequest({
            "type": "store_sql",
            "sql_id": sqlId,
            "sql": sql,
        }).then(
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

        this.#sendStreamRequest({
            "type": "close_sql",
            "sql_id": sqlState.sqlId,
        }).then(
            () => this.#sqlIdAlloc.free(sqlState.sqlId),
            (error) => this.#setClosed(error),
        );
    }

    /** @private */
    override _execute(stmt: proto.Stmt): Promise<proto.StmtResult> {
        return this.#sendStreamRequest({
            "type": "execute",
            "stmt": stmt,
        }).then((response) => {
            return (response as proto.ExecuteStreamResp)["result"];
        });
    }

    /** @private */
    override _batch(batch: proto.Batch): Promise<proto.BatchResult> {
        return this.#sendStreamRequest({
            "type": "batch",
            "batch": batch,
        }).then((response) => {
            return (response as proto.BatchStreamResp)["result"];
        });
    }

    /** @private */
    override _describe(protoSql: ProtoSql): Promise<proto.DescribeResult> {
        return this.#sendStreamRequest({
            "type": "describe",
            "sql": protoSql.sql,
            "sql_id": protoSql.sqlId,
        }).then((response) => {
            return (response as proto.DescribeStreamResp)["result"];
        });
    }

    /** @private */
    override _sequence(protoSql: ProtoSql): Promise<void> {
        return this.#sendStreamRequest({
            "type": "sequence",
            "sql": protoSql.sql,
            "sql_id": protoSql.sqlId,
        }).then((_response) => {
            return undefined;
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

        if (this.#baton !== null || this.#pipeline.length !== 0 || this.#pipelineInProgress) {
            this.#pipeline.push({
                request: {"type": "close"},
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
        const request = this.#createPipelineRequest(pipeline);
        const promise = fetch(request);
        this.#pipelineInProgress = true;
        this.#pipeline.length = 0;

        promise.then((resp: Response) => {
            if (!resp.ok) {
                return errorFromResponse(resp).then((error) => {
                    throw error;
                });
            }
            return resp.json();
        }).then((respJson) => {
            const respBody = respJson as proto.PipelineResponseBody;
            this.#baton = respBody["baton"] ?? null;
            this.#baseUrl = respBody["base_url"] ?? this.#baseUrl;
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

    #createPipelineRequest(pipeline: Array<PipelineEntry>): Request {
        const url = new URL("v2/pipeline", this.#baseUrl);
        const requestBody: proto.PipelineRequestBody = {
            "baton": this.#baton,
            "requests": pipeline.map((entry) => entry.request),
        };
        const headers = new Headers();
        if (this.#jwt !== null) {
            headers.set("authorization", `Bearer ${this.#jwt}`);
        }

        return new Request(url, {
            method: "POST",
            headers,
            body: JSON.stringify(requestBody),
        });
    }
}

function handlePipelineResponse(pipeline: Array<PipelineEntry>, respBody: proto.PipelineResponseBody): void {
    if (respBody["results"].length !== pipeline.length) {
        throw new ProtoError("Server returned unexpected number of pipeline results");
    }

    for (let i = 0; i < pipeline.length; ++i) {
        const result = respBody["results"][i];
        const entry = pipeline[i];

        if (result["type"] === "ok") {
            if (result["response"]["type"] !== entry.request["type"]) {
                throw new ProtoError("Received unexpected type of response");
            }
            entry.responseCallback(result["response"]);
        } else if (result["type"] === "error") {
            entry.errorCallback(errorFromProto(result["error"]));
        } else {
            throw new ProtoError("Received unexpected type of result");
        }
    }
}

async function errorFromResponse(resp: Response): Promise<Error> {
    const respType = resp.headers.get("content-type") ?? "text/plain";
    if (respType === "application/json") {
        const respBody = await resp.json();
        if ("message" in respBody) {
            return errorFromProto(respBody as proto.Error);
        }
    } else if (respType === "text/plain") {
        const respBody = await resp.text();
        return new HttpServerError(
            `Server returned HTTP status ${resp.status} and error: ${respBody}`,
        );
    }
    return new HttpServerError(`Server returned HTTP status ${resp.status}`);
}
