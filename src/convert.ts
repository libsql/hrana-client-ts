import { Base64 } from "js-base64";

import type * as proto from "./proto.js";
import { ClientError, ProtoError, ResponseError } from "./errors.js";

/** A statement that you can send to the database. Statements are represented by the {@link Stmt} class, but
 * as a shorthand, you can specify an SQL string without arguments, or a tuple with the SQL string and
 * positional or named arguments.
 */
export type InStmt =
    | Stmt
    | string
    | [string, InStmtArgs];

/** Arguments for a statement. Either an array that is bound to parameters by position, or an object with
* values that are bound to parameters by name. */
export type InStmtArgs = Array<InValue> | Record<string, InValue>;

/** A statement that can be evaluated by the database. Besides the SQL text, it also contains the positional
 * and named arguments. */
export class Stmt {
    /** The SQL statement string. */
    readonly sql: string;
    /** @private */
    _args: Array<proto.Value>;
    /** @private */
    _namedArgs: Map<string, proto.Value>;

    /** Initialize the statement with given SQL text. */
    constructor(sql: string) {
        this.sql = sql;
        this._args = [];
        this._namedArgs = new Map();
    }

    /** Binds positional parameters from the given `values`. All previous positional bindings are cleared. */
    bindIndexes(values: Iterable<InValue>): this {
        this._args.length = 0;
        for (const value of values) {
            this._args.push(valueToProto(value));
        }
        return this;
    }

    /** Binds a parameter by a 1-based index. */
    bindIndex(index: number, value: InValue): this {
        if (index !== (index|0) || index <= 0) {
            throw new RangeError("Index of a positional argument must be positive integer");
        }

        while (this._args.length < index) {
            this._args.push(protoNull);
        }
        this._args[index - 1] = valueToProto(value);

        return this;
    }

    /** Binds a parameter by name. */
    bindName(name: string, value: InValue): this {
        this._namedArgs.set(name, valueToProto(value));
        return this;
    }

    /** Clears all bindings. */
    unbindAll(): this {
        this._args.length = 0;
        this._namedArgs.clear();
        return this;
    }
}

export function stmtToProto(stmt: InStmt, wantRows: boolean): proto.Stmt {
    let sql;
    let args: Array<proto.Value> = [];
    let namedArgs: Array<proto.NamedArg> = [];
    if (stmt instanceof Stmt) {
        sql = stmt.sql;
        args = stmt._args;
        for (const [name, value] of stmt._namedArgs.entries()) {
            namedArgs.push({"name": name, "value": value});
        }
    } else if (Array.isArray(stmt)) {
        sql = stmt[0];
        if (Array.isArray(stmt[1])) {
            args = stmt[1].map(valueToProto);
        } else {
            namedArgs = Object.entries(stmt[1]).map((entry) => {
                const [key, value] = entry;
                return {"name": key, "value": valueToProto(value)};
            });
        }
    } else {
        sql = ""+stmt;
    }
    return {"sql": sql, "args": args, "named_args": namedArgs, "want_rows": wantRows};
}

/** JavaScript values that you can receive from the database in statement result. */
export type OutValue =
    | null
    | string
    | number
    | ArrayBuffer

/** JavaScript values that you can send to the database as an argument. */
export type InValue =
    | OutValue
    | Uint8Array
    | bigint
    | proto.Value;

export function valueToProto(value: InValue): proto.Value {
    if (value === null) {
        return protoNull;
    } else if (typeof value === "string") {
        return {"type": "text", "value": value};
    } else if (typeof value === "number") {
        return {"type": "float", "value": +value};
    } else if (typeof value === "bigint") {
        return {"type": "text", "value": ""+value};
    } else if (value instanceof ArrayBuffer) {
        return {"type": "blob", "base64": Base64.fromUint8Array(new Uint8Array(value))};
    } else if (value instanceof Uint8Array) {
        return {"type": "blob", "base64": Base64.fromUint8Array(value)};
    } else if (typeof value === "object" && typeof value["type"] === "string") {
        return value;
    } else {
        throw new TypeError("Unsupported type of value");
    }
}

const protoNull: proto.Value = {"type": "null"};

export function valueFromProto(value: proto.Value): OutValue {
    if (value["type"] === "null") {
        return null;
    } else if (value["type"] === "integer") {
        const int = parseInt(value["value"], 10);
        if (!Number.isSafeInteger(int)) {
            throw new RangeError(`Received integer ${value["value"]} which cannot be ` +
                "safely represented as a JavaScript number");
        }
        return int;
    } else if (value["type"] === "float") {
        return value["value"];
    } else if (value["type"] === "text") {
        return value["value"];
    } else if (value["type"] === "blob") {
        return Base64.toUint8Array(value["base64"]).buffer;
    } else {
        throw new ProtoError("Unexpected value type");
    }
}

export function stmtResultFromProto(result: proto.StmtResult): StmtResult {
    return {
        rowsAffected: result["affected_row_count"],
        lastInsertRowid: result["last_insert_rowid"] ?? null,
        columnNames: result["cols"].map(col => col.name),
    };
}

export function rowArrayFromProto(result: proto.StmtResult): RowArray {
    const array = new RowArray(stmtResultFromProto(result));
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
    lastInsertRowid: string | null;
    columnNames: Array<string | null>;
}

export class RowArray extends Array<Row> implements StmtResult {
    rowsAffected: number;
    lastInsertRowid: string | null;
    columnNames: Array<string | null>;

    constructor(result: StmtResult) {
        super();
        this.rowsAffected = result.rowsAffected;
        this.lastInsertRowid = result.lastInsertRowid;
        this.columnNames = result.columnNames;
        Object.setPrototypeOf(this, RowArray.prototype);
    }
}

export type Row = any;

export function errorFromProto(error: proto.Error): ResponseError {
    return new ResponseError(error["message"], error);
}

