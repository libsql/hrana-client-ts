import { ClientConfig } from "./client.js";
import { ClientError, ProtoError, ResponseError } from "./errors.js";
import type * as proto from "./shared/proto.js";
import type { Value, IntMode } from "./value.js";
import { valueFromProto } from "./value.js";

/** Result of executing a database statement. */
export interface StmtResult {
    /** Number of rows that were changed by the statement. This is meaningful only if the statement was an
     * INSERT, UPDATE or DELETE, and the value is otherwise undefined. */
    affectedRowCount: number;
    /** The ROWID of the last successful insert into a rowid table. This is a 64-big signed integer. For other
     * statements than INSERTs into a rowid table, the value is not specified. */
    lastInsertRowid: bigint | undefined;
    /** Names of columns in the result. */
    columnNames: Array<string | undefined>;
    /** Declared types of columns in the result. */
    columnDecltypes: Array<string | undefined>;
}

/** An array of rows returned by a database statement. */
export interface RowsResult extends StmtResult {
    /** The returned rows. */
    rows: Array<Row>;
}

/** A single row returned by a database statement. */
export interface RowResult extends StmtResult {
    /** The returned row. If the query produced zero rows, this is `undefined`. */
    row: Row | undefined,
}

/** A single value returned by a database statement. */
export interface ValueResult extends StmtResult {
    /** The returned value. If the query produced zero rows or zero columns, this is `undefined`. */
    value: Value | undefined,
}

/** Row returned from the database. This is an Array-like object (it has `length` and can be indexed with a
 * number), and in addition, it has enumerable properties from the named columns. */
export interface Row {
    length: number;
    [index: number]: Value;
    [name: string]: Value;
}

export function stmtResultFromProto(result: proto.StmtResult): StmtResult {
    return {
        affectedRowCount: result.affectedRowCount,
        lastInsertRowid: result.lastInsertRowid,
        columnNames: result.cols.map(col => col.name),
        columnDecltypes: result.cols.map(col => col.decltype),
    };
}

export function rowsResultFromProto(result: proto.StmtResult, intMode: IntMode, config: ClientConfig): RowsResult {
    const stmtResult = stmtResultFromProto(result);
    const rows = result.rows.map(row => rowFromProto(stmtResult.columnNames, row, intMode, stmtResult.columnDecltypes, config));
    return {...stmtResult, rows};
}

export function rowResultFromProto(result: proto.StmtResult, intMode: IntMode, config: ClientConfig): RowResult {
    const stmtResult = stmtResultFromProto(result);
    let row: Row | undefined;
    if (result.rows.length > 0) {
        row = rowFromProto(stmtResult.columnNames, result.rows[0], intMode, stmtResult.columnDecltypes, config);
    }
    return {...stmtResult, row};
}

export function valueResultFromProto(result: proto.StmtResult, intMode: IntMode): ValueResult {
    const stmtResult = stmtResultFromProto(result);
    let value: Value | undefined;
    if (result.rows.length > 0 && stmtResult.columnNames.length > 0) {
        // TODO: How do we solve this? AFAICS we don't have column data when fetching a single value, so we don't know when to cast ints to booleans
        value = valueFromProto(result.rows[0][0], intMode);
    }
    return {...stmtResult, value};
}

function rowFromProto(
    colNames: Array<string | undefined>,
    values: Array<proto.Value>,
    intMode: IntMode,
    colDecltypes: Array<string | undefined>,
    config: ClientConfig
): Row {
    const row = {};
    // make sure that the "length" property is not enumerable
    Object.defineProperty(row, "length", { value: values.length });
    for (let i = 0; i < values.length; ++i) {
        const value = valueFromProto(values[i], intMode, colDecltypes[i], config.castBooleans);
        Object.defineProperty(row, i, { value });

        const colName = colNames[i];
        if (colName !== undefined && !Object.hasOwn(row, colName)) {
            Object.defineProperty(row, colName, { value, enumerable: true, configurable: true, writable: true });
        }
    }
    return row as Row;
}

export function errorFromProto(error: proto.Error): ResponseError {
    return new ResponseError(error.message, error);
}
