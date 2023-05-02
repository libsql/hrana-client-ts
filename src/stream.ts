import type { Client, StreamState } from "./client.js";
import type { DescribeResult } from "./describe.js";
import { Batch } from "./batch.js";
import { describeResultFromProto } from "./describe.js";
import { ClientError, ProtocolVersionError } from "./errors.js";
import type * as proto from "./proto.js";
import type { RowsResult, RowResult, ValueResult, StmtResult } from "./result.js";
import {
    stmtResultFromProto, rowsResultFromProto,
    rowResultFromProto, valueResultFromProto,
} from "./result.js";
import type { InStmt } from "./stmt.js";
import { stmtToProto } from "./stmt.js";

/** A stream for executing SQL statements (a "database connection"). */
export class Stream {
    #client: Client;
    #state: StreamState;

    /** @private */
    constructor(client: Client, state: StreamState) {
        this.#client = client;
        this.#state = state;
    }

    /** Execute a statement and return rows. */
    query(stmt: InStmt): Promise<RowsResult> {
        return this.#execute(stmtToProto(stmt, true), rowsResultFromProto);
    }

    /** Execute a statement and return at most a single row. */
    queryRow(stmt: InStmt): Promise<RowResult> {
        return this.#execute(stmtToProto(stmt, true), rowResultFromProto);
    }

    /** Execute a statement and return at most a single value. */
    queryValue(stmt: InStmt): Promise<ValueResult> {
        return this.#execute(stmtToProto(stmt, true), valueResultFromProto);
    }

    /** Execute a statement without returning rows. */
    run(stmt: InStmt): Promise<StmtResult> {
        return this.#execute(stmtToProto(stmt, false), stmtResultFromProto);
    }

    #execute<T>(stmt: proto.Stmt, fromProto: (result: proto.StmtResult) => T): Promise<T> {
        return new Promise((doneCallback, errorCallback) => {
            const request: proto.ExecuteReq = {
                "type": "execute",
                "stream_id": this.#state.streamId,
                "stmt": stmt,
            };
            const responseCallback = (response: proto.Response): void => {
                const result = (response as proto.ExecuteResp)["result"];
                doneCallback(fromProto(result));
            };
            this.#client._sendStreamRequest(this.#state, request, {responseCallback, errorCallback});
        });
    }

    /** Return a builder for creating and executing a batch. */
    batch(): Batch {
        return new Batch(this.#client, this.#state);
    }

    /** Parse and analyze a statement. */
    async describe(sql: string): Promise<DescribeResult> {
        const version = await this.#client._getVersion();
        if (version < 2) {
            return Promise.reject(new ProtocolVersionError(
                "describe() is supported on protocol version 2 and higher, " +
                    `but the server only supports version ${version}`
            ));
        }

        return new Promise((doneCallback, errorCallback) => {
            const request: proto.DescribeReq = {
                "type": "describe",
                "stream_id": this.#state.streamId,
                "sql": sql,
            };
            const responseCallback = (response: proto.Response): void => {
                const result = (response as proto.DescribeResp)["result"];
                doneCallback(describeResultFromProto(result));
            };
            this.#client._sendStreamRequest(this.#state, request, {responseCallback, errorCallback});
        });
    }

    /** Close the stream. */
    close(): void {
        this.#client._closeStream(this.#state, new ClientError("Stream was manually closed"));
    }

    /** True if the stream is closed. */
    get closed() {
        return this.#state.closed !== undefined;
    }
}
