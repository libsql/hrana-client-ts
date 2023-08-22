import { ClientError, ClosedError, MisuseError } from "./errors.js";

/** A SQL text that you can send to the database. Either a string or a reference to SQL text that is cached on
 * the server. */
export type InSql = string | Sql;

export interface SqlOwner {
    /** Cache a SQL text on the server. This requires protocol version 2 or higher. */
    storeSql(sql: string): Sql;

    /** @private */
    _closeSql(sqlId: number): void;
}

/** Text of an SQL statement cached on the server. */
export class Sql {
    #owner: SqlOwner;
    #sqlId: number;
    #closed: Error | undefined;

    /** @private */
    constructor(owner: SqlOwner, sqlId: number) {
        this.#owner = owner;
        this.#sqlId = sqlId;
        this.#closed = undefined;
    }

    /** @private */
    _getSqlId(owner: SqlOwner): number {
        if (this.#owner !== owner) {
            throw new MisuseError("Attempted to use SQL text opened with other object");
        } else if (this.#closed !== undefined) {
            throw new ClosedError("SQL text is closed", this.#closed);
        }
        return this.#sqlId;
    }

    /** Remove the SQL text from the server, releasing resouces. */
    close(): void {
        this._setClosed(new ClientError("SQL text was manually closed"));
    }

    /** @private */
    _setClosed(error: Error): void {
        if (this.#closed === undefined) {
            this.#closed = error;
            this.#owner._closeSql(this.#sqlId);
        }
    }

    /** True if the SQL text is closed (removed from the server). */
    get closed() {
        return this.#closed !== undefined;
    }
}

export type ProtoSql = {
    sql?: string,
    sqlId?: number,
};

export function sqlToProto(owner: SqlOwner, sql: InSql): ProtoSql {
    if (sql instanceof Sql) {
        return {sqlId: sql._getSqlId(owner)};
    } else {
        return {sql: ""+sql};
    }
}
