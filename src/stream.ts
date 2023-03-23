import type { Client, StreamState } from "./client.js";
import { ClientError } from "./errors.js";
import { Batch } from "./batch.js";
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
            this.#client._execute(this.#state, {
                stmt,
                resultCallback(result) {
                    doneCallback(fromProto(result));
                },
                errorCallback,
            });
        });
    }

    /** Return a builder for creating and executing a batch. */
    batch(): Batch {
        return new Batch(this.#client, this.#state);
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
