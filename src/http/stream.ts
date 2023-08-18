import type { fetch, Response } from "@libsql/isomorphic-fetch";
import { Request, Headers } from "@libsql/isomorphic-fetch";

import type { ProtocolEncoding } from "../client.js";
import type { Cursor } from "../cursor.js";
import type * as jsone from "../encoding/json/encode.js";
import type * as protobufe from "../encoding/protobuf/encode.js";
import {
    ClientError, HttpServerError, ProtocolVersionError,
    ProtoError, ClosedError, InternalError,
} from "../errors.js";
import {
    readJsonObject, writeJsonObject, readProtobufMessage, writeProtobufMessage,
} from "../encoding/index.js";
import { IdAlloc } from "../id_alloc.js";
import { queueMicrotask } from "../ponyfill.js";
import { Queue } from "../queue.js";
import { errorFromProto } from "../result.js";
import type { SqlOwner, SqlState, ProtoSql } from "../sql.js";
import { Sql } from "../sql.js";
import { Stream } from "../stream.js";
import { impossible } from "../util.js";

import type { HttpClient, Endpoint } from "./client.js";
import { HttpCursor } from "./cursor.js";
import type * as proto from "./proto.js";

import { PipelineReqBody as json_PipelineReqBody } from "./json_encode.js";
import { PipelineReqBody as protobuf_PipelineReqBody } from "./protobuf_encode.js";
import { CursorReqBody as json_CursorReqBody } from "./json_encode.js";
import { CursorReqBody as protobuf_CursorReqBody } from "./protobuf_encode.js";
import { PipelineRespBody as json_PipelineRespBody } from "./json_decode.js";
import { PipelineRespBody as protobuf_PipelineRespBody } from "./protobuf_decode.js";

type QueueEntry = PipelineEntry | CursorEntry;

type PipelineEntry = {
    type: "pipeline",
    request: proto.StreamRequest,
    responseCallback: (_: proto.StreamResponse) => void,
    errorCallback: (_: Error) => void,
}

type CursorEntry = {
    type: "cursor",
    batch: proto.Batch,
    cursorCallback: (_: HttpCursor) => void,
    errorCallback: (_: Error) => void,
}

export class HttpStream extends Stream implements SqlOwner {
    #client: HttpClient;
    #baseUrl: string;
    #jwt: string | undefined;
    #fetch: typeof fetch;

    #closed: Error | undefined;
    #baton: string | undefined;
    #queue: Queue<QueueEntry>;
    #flushing: boolean;

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
        this.#queue = new Queue();
        this.#flushing = false;

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

    /** @private */
    override _openCursor(batch: proto.Batch): Promise<HttpCursor> {
        return new Promise((cursorCallback, errorCallback) => {
            this.#pushToQueue({type: "cursor", batch, cursorCallback, errorCallback});
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
    _closeFromClient(error: Error): void {
        this.#setClosed(new ClosedError("Client was closed", error));
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

        if (this.#baton !== undefined || this.#queue.length !== 0 || this.#flushing) {
            this.#queue.push({
                type: "pipeline",
                request: {type: "close"},
                responseCallback() {},
                errorCallback() {},
            });
            this.#flushQueue();
        }
    }

    #abort(error: Error): void {
        for (;;) {
            const entry = this.#queue.shift();
            if (entry !== undefined) {
                entry.errorCallback(error);
            } else {
                break;
            }
        }
        this.#setClosed(error);
    }

    #sendStreamRequest(request: proto.StreamRequest): Promise<proto.StreamResponse> {
        return new Promise((responseCallback, errorCallback) => {
            this.#pushToQueue({type: "pipeline", request, responseCallback, errorCallback});
        });
    }

    #pushToQueue(entry: QueueEntry): void {
        if (this.#closed !== undefined) {
            throw new ClosedError("Stream is closed", this.#closed);
        }
        this.#queue.push(entry);
        queueMicrotask(() => this.#flushQueue());
    }


    #flushQueue(): void {
        if (this.#flushing) {
            return;
        }

        const endpoint = this.#client._endpoint;
        if (endpoint === undefined) {
            this.#client._endpointPromise.then(
                () => this.#flushQueue(),
                (error) => this.#abort(error),
            );
            return;
        }

        const firstEntry = this.#queue.shift();
        if (firstEntry === undefined) {
            return;
        } else if (firstEntry.type === "pipeline") {
            const pipeline: Array<PipelineEntry> = [firstEntry];
            for (;;) {
                const entry = this.#queue.first();
                if (entry !== undefined && entry.type === "pipeline") {
                    pipeline.push(entry);
                    this.#queue.shift();
                } else {
                    break;
                }
            }
            this.#flushPipeline(endpoint, pipeline);
        } else if (firstEntry.type === "cursor") {
            this.#flushCursor(endpoint, firstEntry);
        } else {
            throw impossible(firstEntry, "Impossible type of QueueEntry");
        }
    }

    #flushPipeline(endpoint: Endpoint, pipeline: Array<PipelineEntry>): void {
        this.#flush<proto.PipelineRespBody>(
            () => this.#createPipelineRequest(pipeline, endpoint),
            (resp) => decodePipelineResponse(resp, endpoint.encoding),
            (respBody) => respBody.baton,
            (respBody) => respBody.baseUrl,
            (respBody) => handlePipelineResponse(pipeline, respBody),
            (error) => pipeline.forEach((entry) => entry.errorCallback(error)),
        );
    }

    #flushCursor(endpoint: Endpoint, entry: CursorEntry): void {
        this.#flush<[HttpCursor, proto.CursorRespBody]>(
            () => this.#createCursorRequest(entry, endpoint),
            (resp) => HttpCursor.open(resp, endpoint.encoding),
            ([_cursor, respBody]) => respBody.baton,
            ([_cursor, respBody]) => respBody.baseUrl,
            ([cursor, _respBody]) => entry.cursorCallback(cursor),
            (error) => entry.errorCallback(error),
        );
    }

    #flush<R>(
        createRequest: () => Request,
        decodeResponse: (_: Response) => Promise<R>,
        getBaton: (_: R) => string | undefined,
        getBaseUrl: (_: R) => string | undefined,
        handleResponse: (_: R) => void,
        handleError: (_: Error) => void,
    ): void {
        let promise;
        try {
            const request = createRequest();
            const fetch = this.#fetch;
            promise = fetch(request);
        } catch (error) {
            promise = Promise.reject(error);
        }

        this.#flushing = true;
        promise.then((resp: Response): Promise<R> => {
            if (!resp.ok) {
                return errorFromResponse(resp).then((error) => {
                    throw error;
                });
            }
            return decodeResponse(resp);
        }).then((r: R) => {
            this.#baton = getBaton(r);
            this.#baseUrl = getBaseUrl(r) ?? this.#baseUrl;
            handleResponse(r);
        }).catch((error: Error) => {
            this.#setClosed(error);
            handleError(error);
        }).finally(() => {
            this.#flushing = false;
            this.#flushQueue();
        });
    }

    #createPipelineRequest(pipeline: Array<PipelineEntry>, endpoint: Endpoint): Request {
        return this.#createRequest<proto.PipelineReqBody>(
            new URL(endpoint.pipelinePath, this.#baseUrl),
            {
                baton: this.#baton,
                requests: pipeline.map((entry) => entry.request),
            },
            endpoint.encoding,
            json_PipelineReqBody,
            protobuf_PipelineReqBody,
        );
    }

    #createCursorRequest(entry: CursorEntry, endpoint: Endpoint): Request {
        if (endpoint.cursorPath === undefined) {
            throw new ProtocolVersionError(
                "Cursors are supported only on protocol version 3 and higher, " +
                    `but the HTTP server only supports version ${endpoint.version}.`,
            );
        }
        return this.#createRequest<proto.CursorReqBody>(
            new URL(endpoint.cursorPath, this.#baseUrl),
            {
                baton: this.#baton,
                batch: entry.batch,
            },
            endpoint.encoding,
            json_CursorReqBody,
            protobuf_CursorReqBody,
        );
    }

    #createRequest<T>(
        url: URL,
        reqBody: T,
        encoding: ProtocolEncoding,
        jsonFun: jsone.ObjectFun<T>,
        protobufFun: protobufe.MessageFun<T>,
    ): Request {
        let bodyData: string | Uint8Array;
        let contentType: string;
        if (encoding === "json") {
            bodyData = writeJsonObject(reqBody, jsonFun);
            contentType = "application/json";
        } else if (encoding === "protobuf") {
            bodyData = writeProtobufMessage(reqBody, protobufFun);
            contentType = "application/x-protobuf";
        } else {
            throw impossible(encoding, "Impossible encoding");
        }

        const headers = new Headers();
        headers.set("content-type", contentType);
        if (this.#jwt !== undefined) {
            headers.set("authorization", `Bearer ${this.#jwt}`);
        }

        return new Request(url.toString(), {method: "POST", headers, body: bodyData});
    }
}

function handlePipelineResponse(pipeline: Array<PipelineEntry>, respBody: proto.PipelineRespBody): void {
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
            throw new ProtoError("Received unrecognized type of StreamResult");
        } else {
            throw impossible(result, "Received impossible type of StreamResult");
        }
    }
}

async function decodePipelineResponse(
    resp: Response,
    encoding: ProtocolEncoding,
): Promise<proto.PipelineRespBody> {
    if (encoding === "json") {
        const respJson = await resp.json();
        return readJsonObject(respJson, json_PipelineRespBody);
    } else if (encoding === "protobuf") {
        const respData = await resp.arrayBuffer();
        return readProtobufMessage(new Uint8Array(respData), protobuf_PipelineRespBody);
    } else {
        throw impossible(encoding, "Impossible encoding");
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
