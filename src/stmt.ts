import type * as proto from "./proto.js";
import type { InSql } from "./sql.js";
import { sqlToProto } from "./sql.js";
import type { InValue } from "./value.js";
import { valueToProto, protoNull } from "./value.js";

/** A statement that you can send to the database. Statements are represented by the {@link Stmt} class, but
 * as a shorthand, you can specify an SQL text without arguments, or a tuple with the SQL text and positional
 * or named arguments.
 */
export type InStmt =
    | Stmt
    | InSql
    | [InSql, InStmtArgs];

/** Arguments for a statement. Either an array that is bound to parameters by position, or an object with
* values that are bound to parameters by name. */
export type InStmtArgs = Array<InValue> | Record<string, InValue>;

/** A statement that can be evaluated by the database. Besides the SQL text, it also contains the positional
 * and named arguments. */
export class Stmt {
    /** The SQL statement text. */
    sql: InSql;
    /** @private */
    _args: Array<proto.Value>;
    /** @private */
    _namedArgs: Map<string, proto.Value>;

    /** Initialize the statement with given SQL text. */
    constructor(sql: InSql) {
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
    let inSql: InSql;
    let args: Array<proto.Value> = [];
    let namedArgs: Array<proto.NamedArg> = [];
    if (stmt instanceof Stmt) {
        inSql = stmt.sql;
        args = stmt._args;
        for (const [name, value] of stmt._namedArgs.entries()) {
            namedArgs.push({"name": name, "value": value});
        }
    } else if (Array.isArray(stmt)) {
        inSql = stmt[0];
        if (Array.isArray(stmt[1])) {
            args = stmt[1].map(valueToProto);
        } else {
            namedArgs = Object.entries(stmt[1]).map((entry) => {
                const [key, value] = entry;
                return {"name": key, "value": valueToProto(value)};
            });
        }
    } else {
        inSql = stmt;
    }

    const {sql, sqlId} = sqlToProto(inSql);
    return {
        "sql": sql,
        "sql_id": sqlId,
        "args": args,
        "named_args": namedArgs,
        "want_rows": wantRows,
    };
}

