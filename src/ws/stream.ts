import { ClientError } from "../errors.js";
import type { SqlOwner, ProtoSql } from "../sql.js";
import { Stream } from "../stream.js";

import type { WsClient } from "./client.js";
import type * as proto from "./proto.js";

export interface StreamState {
    streamId: number;
    closed: Error | undefined;
}

export class WsStream extends Stream {
    #client: WsClient;
    #state: StreamState;

    /** @private */
    constructor(client: WsClient, state: StreamState) {
        super();
        this.#client = client;
        this.#state = state;
    }

    /** @private */
    override _sqlOwner(): SqlOwner {
        return this.#client;
    }

    /** @private */
    override _execute(stmt: proto.Stmt): Promise<proto.StmtResult> {
        return this.#sendStreamRequest({
            "type": "execute",
            "stream_id": this.#state.streamId,
            "stmt": stmt,
        }).then((response) => {
            return (response as proto.ExecuteResp)["result"];
        });
    }

    /** @private */
    override _batch(batch: proto.Batch): Promise<proto.BatchResult> {
        return this.#sendStreamRequest({
            "type": "batch",
            "stream_id": this.#state.streamId,
            "batch": batch,
        }).then((response) => {
            return (response as proto.BatchResp)["result"];
        });
    }

    /** @private */
    override _describe(protoSql: ProtoSql): Promise<proto.DescribeResult> {
        this.#client._ensureVersion(2, "describe()");
        return this.#sendStreamRequest({
            "type": "describe",
            "stream_id": this.#state.streamId,
            "sql": protoSql.sql,
            "sql_id": protoSql.sqlId,
        }).then((response) => {
            return (response as proto.DescribeResp)["result"];
        });
    }

    /** @private */
    override _sequence(protoSql: ProtoSql): Promise<void> {
        this.#client._ensureVersion(2, "sequence()");
        return this.#sendStreamRequest({
            "type": "sequence",
            "stream_id": this.#state.streamId,
            "sql": protoSql.sql,
            "sql_id": protoSql.sqlId,
        }).then((_response) => {
            return undefined;
        });
    }

    #sendStreamRequest(request: proto.Request): Promise<proto.Response> {
        return new Promise((responseCallback, errorCallback) => {
            this.#client._sendStreamRequest(this.#state, request, {responseCallback, errorCallback});
        });
    }

    /** Close the stream. */
    override close(): void {
        this.#client._closeStream(this.#state, new ClientError("Stream was manually closed"));
    }

    /** True if the stream is closed. */
    override get closed(): boolean {
        return this.#state.closed !== undefined;
    }
}
