import { ClientError, ClosedError, InternalError } from "../errors.js";
import { Queue } from "../queue.js";
import type { SqlOwner, ProtoSql } from "../sql.js";
import { Stream } from "../stream.js";

import type { WsClient } from "./client.js";
import { WsCursor } from "./cursor.js";
import type * as proto from "./proto.js";

type QueueEntry = RequestEntry | CursorEntry;

type RequestEntry = {
    type: "request",
    request: proto.Request,
    responseCallback: (_: proto.Response) => void;
    errorCallback: (_: Error) => void;
}

type CursorEntry = {
    type: "cursor",
    batch: proto.Batch,
    cursorCallback: (_: WsCursor) => void,
    errorCallback: (_: Error) => void,
}

export class WsStream extends Stream {
    #client: WsClient;
    #streamId: number;

    #queue: Queue<QueueEntry>;
    #cursor: WsCursor | undefined;
    #closing: boolean;
    #closed: Error | undefined;

    /** @private */
    static open(client: WsClient): WsStream {
        const streamId = client._streamIdAlloc.alloc();
        const stream = new WsStream(client, streamId);

        const responseCallback = () => undefined;
        const errorCallback = (e: Error) => stream.#setClosed(e);

        const request: proto.OpenStreamReq = {type: "open_stream", streamId};
        client._sendRequest(request, {responseCallback, errorCallback});
        return stream;
    }

    /** @private */
    constructor(client: WsClient, streamId: number) {
        super(client.intMode);
        this.#client = client;
        this.#streamId = streamId;

        this.#queue = new Queue();
        this.#cursor = undefined;
        this.#closing = false;
        this.#closed = undefined;
    }

    /** Get the {@link WsClient} object that this stream belongs to. */
    override client(): WsClient {
        return this.#client;
    }

    /** @private */
    override _sqlOwner(): SqlOwner {
        return this.#client;
    }

    /** @private */
    override _execute(stmt: proto.Stmt): Promise<proto.StmtResult> {
        return this.#sendStreamRequest({
            type: "execute",
            streamId: this.#streamId,
            stmt,
        }).then((response) => {
            return (response as proto.ExecuteResp).result;
        });
    }

    /** @private */
    override _batch(batch: proto.Batch): Promise<proto.BatchResult> {
        return this.#sendStreamRequest({
            type: "batch",
            streamId: this.#streamId,
            batch,
        }).then((response) => {
            return (response as proto.BatchResp).result;
        });
    }

    /** @private */
    override _describe(protoSql: ProtoSql): Promise<proto.DescribeResult> {
        this.#client._ensureVersion(2, "describe()");
        return this.#sendStreamRequest({
            type: "describe",
            streamId: this.#streamId,
            sql: protoSql.sql,
            sqlId: protoSql.sqlId,
        }).then((response) => {
            return (response as proto.DescribeResp).result;
        });
    }

    /** @private */
    override _sequence(protoSql: ProtoSql): Promise<void> {
        this.#client._ensureVersion(2, "sequence()");
        return this.#sendStreamRequest({
            type: "sequence",
            streamId: this.#streamId,
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
            streamId: this.#streamId,
        }).then((response) => {
            return (response as proto.GetAutocommitResp).isAutocommit;
        });
    }

    #sendStreamRequest(request: proto.Request): Promise<proto.Response> {
        return new Promise((responseCallback, errorCallback) => {
            this.#pushToQueue({type: "request", request, responseCallback, errorCallback});
        });
    }

    /** @private */
    override _openCursor(batch: proto.Batch): Promise<WsCursor> {
        this.#client._ensureVersion(3, "cursor");
        return new Promise((cursorCallback, errorCallback) => {
            this.#pushToQueue({type: "cursor", batch, cursorCallback, errorCallback});
        });
    }

    /** @private */
    _sendCursorRequest(cursor: WsCursor, request: proto.Request): Promise<proto.Response> {
        if (cursor !== this.#cursor) {
            throw new InternalError("Cursor not associated with the stream attempted to execute a request");
        }
        return new Promise((responseCallback, errorCallback) => {
            if (this.#closed !== undefined) {
                errorCallback(new ClosedError("Stream is closed", this.#closed));
            } else {
                this.#client._sendRequest(request, {responseCallback, errorCallback});
            }
        });
    }

    /** @private */
    _cursorClosed(cursor: WsCursor): void {
        if (cursor !== this.#cursor) {
            throw new InternalError("Cursor was closed, but it was not associated with the stream");
        }
        this.#cursor = undefined;
        this.#flushQueue();
    }

    #pushToQueue(entry: QueueEntry): void {
        if (this.#closed !== undefined) {
            entry.errorCallback(new ClosedError("Stream is closed", this.#closed));
        } else if (this.#closing) {
            entry.errorCallback(new ClosedError("Stream is closing", undefined));
        } else {
            this.#queue.push(entry);
            this.#flushQueue();
        }
    }

    #flushQueue(): void {
        for (;;) {
            const entry = this.#queue.first();
            if (entry === undefined && this.#cursor === undefined && this.#closing) {
                this.#setClosed(new ClientError("Stream was gracefully closed"));
                break;
            } else if (entry?.type === "request" && this.#cursor === undefined) {
                const {request, responseCallback, errorCallback} = entry;
                this.#queue.shift();

                this.#client._sendRequest(request, {responseCallback, errorCallback});
            } else if (entry?.type === "cursor" && this.#cursor === undefined) {
                const {batch, cursorCallback} = entry;
                this.#queue.shift();

                const cursorId = this.#client._cursorIdAlloc.alloc();
                const cursor = new WsCursor(this.#client, this, cursorId);

                const request: proto.OpenCursorReq = {
                    type: "open_cursor",
                    streamId: this.#streamId,
                    cursorId,
                    batch,
                };
                const responseCallback = () => undefined;
                const errorCallback = (e: Error) => cursor._setClosed(e);
                this.#client._sendRequest(request, {responseCallback, errorCallback});

                this.#cursor = cursor;
                cursorCallback(cursor);
            } else {
                break;
            }
        }
    }

    #setClosed(error: Error): void {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;

        if (this.#cursor !== undefined) {
            this.#cursor._setClosed(error);
        }

        for (;;) {
            const entry = this.#queue.shift();
            if (entry !== undefined) {
                entry.errorCallback(error);
            } else {
                break;
            }
        }

        const request: proto.CloseStreamReq = {type: "close_stream", streamId: this.#streamId};
        const responseCallback = () => this.#client._streamIdAlloc.free(this.#streamId);
        const errorCallback = () => undefined;
        this.#client._sendRequest(request, {responseCallback, errorCallback});
    }

    /** Immediately close the stream. */
    override close(): void {
        this.#setClosed(new ClientError("Stream was manually closed"));
    }

    /** Gracefully close the stream. */
    override closeGracefully(): void {
        this.#closing = true;
        this.#flushQueue();
    }

    /** True if the stream is closed or closing. */
    override get closed(): boolean {
        return this.#closed !== undefined || this.#closing;
    }
}
