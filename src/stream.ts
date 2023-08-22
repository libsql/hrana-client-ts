import { Batch } from "./batch.js";
import type { Client } from "./client.js";
import type { Cursor } from "./cursor.js";
import type { DescribeResult } from "./describe.js";
import { describeResultFromProto } from "./describe.js";
import type { RowsResult, RowResult, ValueResult, StmtResult } from "./result.js";
import {
    stmtResultFromProto, rowsResultFromProto,
    rowResultFromProto, valueResultFromProto,
} from "./result.js";
import type * as proto from "./shared/proto.js";
import type { InSql, SqlOwner, ProtoSql } from "./sql.js";
import { sqlToProto } from "./sql.js";
import type { InStmt } from "./stmt.js";
import { stmtToProto } from "./stmt.js";
import type { IntMode } from "./value.js";

/** A stream for executing SQL statements (a "database connection"). */
export abstract class Stream {
    /** @private */
    constructor(intMode: IntMode) {
        this.intMode = intMode;
    }

    /** Get the client object that this stream belongs to. */
    abstract client(): Client;

    /** @private */
    abstract _sqlOwner(): SqlOwner;
    /** @private */
    abstract _execute(stmt: proto.Stmt): Promise<proto.StmtResult>;
    /** @private */
    abstract _batch(batch: proto.Batch): Promise<proto.BatchResult>;
    /** @private */
    abstract _describe(protoSql: ProtoSql): Promise<proto.DescribeResult>;
    /** @private */
    abstract _sequence(protoSql: ProtoSql): Promise<void>;
    /** @private */
    abstract _openCursor(batch: proto.Batch): Promise<Cursor>;

    /** Execute a statement and return rows. */
    query(stmt: InStmt): Promise<RowsResult> {
        return this.#execute(stmt, true, rowsResultFromProto);
    }

    /** Execute a statement and return at most a single row. */
    queryRow(stmt: InStmt): Promise<RowResult> {
        return this.#execute(stmt, true, rowResultFromProto);
    }

    /** Execute a statement and return at most a single value. */
    queryValue(stmt: InStmt): Promise<ValueResult> {
        return this.#execute(stmt, true, valueResultFromProto);
    }

    /** Execute a statement without returning rows. */
    run(stmt: InStmt): Promise<StmtResult> {
        return this.#execute(stmt, false, stmtResultFromProto);
    }

    #execute<T>(
        inStmt: InStmt,
        wantRows: boolean,
        fromProto: (result: proto.StmtResult, intMode: IntMode) => T,
    ): Promise<T> {
        const stmt = stmtToProto(this._sqlOwner(), inStmt, wantRows);
        return this._execute(stmt).then((r) => fromProto(r, this.intMode));
    }

    /** Return a builder for creating and executing a batch.
     *
     * If `useCursor` is true, the batch will be executed using a Hrana cursor, which will stream results from
     * the server to the client, which consumes less memory on the server. This requires protocol version 3 or
     * higher.
     */
    batch(useCursor: boolean = false): Batch {
        return new Batch(this, useCursor);
    }

    /** Parse and analyze a statement. This requires protocol version 2 or higher. */
    describe(inSql: InSql): Promise<DescribeResult> {
        const protoSql = sqlToProto(this._sqlOwner(), inSql);
        return this._describe(protoSql).then(describeResultFromProto);
    }

    /** Execute a sequence of statements separated by semicolons. This requires protocol version 2 or higher.
     * */
    sequence(inSql: InSql): Promise<void> {
        const protoSql = sqlToProto(this._sqlOwner(), inSql);
        return this._sequence(protoSql);
    }

    /** Check whether the SQL connection underlying this stream is in autocommit state (i.e., outside of an
     * explicit transaction). This requires protocol version 3 or higher.
     */
    abstract getAutocommit(): Promise<boolean>;

    /** Immediately close the stream.
     *
     * This closes the stream immediately, aborting any pending operations.
     */
    abstract close(): void;

    /** Gracefully close the stream.
     *
     * After calling this method, you will not be able to start new operations, but existing operations will
     * complete.
     */
    abstract closeGracefully(): void;

    /** True if the stream is closed or closing.
     *
     * If you call {@link closeGracefully}, this will become true immediately, even if the underlying stream
     * is not physically closed yet.
     */
    abstract get closed(): boolean;

    /** Representation of integers returned from the database. See {@link IntMode}.
     *
     * This value affects the results of all operations on this stream.
     */
    intMode: IntMode;
}
