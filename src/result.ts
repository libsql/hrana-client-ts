import { ClientError, ProtoError, ResponseError } from "./errors.js";
import type { Value } from "./value.js";
import { valueFromProto } from "./value.js";
import type * as proto from "./proto.js";

/** Result of executing a database statement. */
export interface StmtResult {
    /** Number of rows that were changed by the statement. This is meaningful only if the statement was an
     * INSERT, UPDATE or DELETE, and the value is otherwise undefined. */
    affectedRowCount: number;
    /** The ROWID of the last successful insert into a rowid table. This is a 64-big signed integer encoded as
     * a string. For other statements than INSERTs into a rowid table, the value is not specified. */
    lastInsertRowid: string | undefined;
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
        affectedRowCount: result["affected_row_count"],
        lastInsertRowid: result["last_insert_rowid"] ?? undefined,
        columnNames: result["cols"].map(col => col["name"] ?? undefined),
        columnDecltypes: result["cols"].map(col => col["decltype"] ?? undefined),
    };
}

export function rowsResultFromProto(result: proto.StmtResult): RowsResult {
    const stmtResult = stmtResultFromProto(result);
    const rows = result["rows"].map(row => rowFromProto(stmtResult.columnNames, row));
    return {...stmtResult, rows};
}

export function rowResultFromProto(result: proto.StmtResult): RowResult {
    const stmtResult = stmtResultFromProto(result);
    let row: Row | undefined;
    if (result.rows.length > 0) {
        row = rowFromProto(stmtResult.columnNames, result.rows[0]);
    }
    return {...stmtResult, row};
}

export function valueResultFromProto(result: proto.StmtResult): ValueResult {
    const stmtResult = stmtResultFromProto(result);
    let value: Value | undefined;
    if (result.rows.length > 0 && stmtResult.columnNames.length > 0) {
        value = valueFromProto(result.rows[0][0]);
    }
    return {...stmtResult, value};
}

function rowFromProto(colNames: Array<string | undefined>, values: Array<proto.Value>): Row {
    const row = {};
    // make sure that the "length" property is not enumerable
    Object.defineProperty(row, "length", { value: values.length });
    for (let i = 0; i < values.length; ++i) {
        const value = valueFromProto(values[i]);
        Object.defineProperty(row, i, { value });

        const colName = colNames[i];
        if (colName !== undefined && !Object.hasOwn(row, colName)) {
            Object.defineProperty(row, colName, { value, enumerable: true });
        }
    }
    return row as Row;
}

export function errorFromProto(error: proto.Error): ResponseError {
    return new ResponseError(error["message"], error);
}
