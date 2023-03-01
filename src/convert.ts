import type * as proto from "./proto.js";
import { ClientError, ProtoError, ResponseError } from "./errors.js";

/** A statement that you can send to the database. Either a plain SQL string, or an SQL string together with
 * values for the `?` parameters.
 */
export type Stmt =
    | string
    | [string, StmtArgs];

/** Arguments for a statement. Either an array that is bound to parameters by position, or an object with
* values that are bound to parameters by name. */
export type StmtArgs = Array<Value> | Record<string, Value>;

export function stmtToProto(stmt: Stmt, wantRows: boolean): proto.Stmt {
    let sql;
    let args: Array<proto.Value> = [];
    let namedArgs: Array<proto.NamedArg> = [];
    if (typeof stmt === "string") {
        sql = stmt;
    } else {
        sql = stmt[0];
        if (Array.isArray(stmt[1])) {
            args = stmt[1].map(valueToProto);
        } else {
            namedArgs = Object.entries(stmt[1]).map((entry) => {
                const [key, value] = entry;
                return {"name": key, "value": valueToProto(value)};
            });
        }
    }
    return {"sql": sql, "args": args, "named_args": namedArgs, "want_rows": wantRows};
}

/** JavaScript values that you can get from the database. */
export type Value =
    | null
    | string
    | number
    | ArrayBuffer;

export function valueToProto(value: Value): proto.Value {
    if (value === null) {
        return {"type": "null"};
    } else if (typeof value === "number") {
        return {"type": "float", "value": +value};
    } else if (value instanceof ArrayBuffer) {
        throw new ClientError("ArrayBuffer is not yet supported");
    } else {
        return {"type": "text", "value": ""+value};
    }
}

export function valueFromProto(value: proto.Value): Value {
    if (value["type"] === "null") {
        return null;
    } else if (value["type"] === "integer") {
        return parseInt(value["value"], 10);
    } else if (value["type"] === "float") {
        return value["value"];
    } else if (value["type"] === "text") {
        return value["value"];
    } else if (value["type"] === "blob") {
        throw new ClientError("blob is not yet supported");
    } else {
        throw new ProtoError("Unexpected value type");
    }
}

export function stmtResultFromProto(result: proto.StmtResult): StmtResult {
    return {rowsAffected: result["affected_row_count"]};
}

export function rowArrayFromProto(result: proto.StmtResult): RowArray {
    const array = new RowArray(result["affected_row_count"]);
    for (const row of result["rows"]) {
        array.push(rowFromProto(result, row));
    }
    return array;
}

export function rowFromProto(result: proto.StmtResult, row: Array<proto.Value>): Row {
    const array = row.map((value) => valueFromProto(value));

    for (let i = 0; i < result["cols"].length; ++i) {
        const colName = result["cols"][i]["name"];
        if (colName && !Object.hasOwn(array, colName)) {
            Object.defineProperty(array, colName, {
                value: array[i],
                enumerable: true,
            });
        }
    }

    return array;
}

export interface StmtResult {
    rowsAffected: number;
}

export class RowArray extends Array<Row> implements StmtResult {
    constructor(public rowsAffected: number) {
        super();
        Object.setPrototypeOf(this, RowArray.prototype);
    }
}

export type Row = any;

export function errorFromProto(error: proto.Error): ResponseError {
    return new ResponseError(error["message"], error);
}

