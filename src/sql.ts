import { ClientError, ClosedError, MisuseError } from "./errors.js";

/** A SQL text that you can send to the database. Either a string or a reference to SQL text that is cached on
 * the server. */
export type InSql = string | Sql;

export interface SqlOwner {
    /** Cache a SQL text on the server. This requires protocol version 2 or higher. */
    storeSql(sql: string): Sql;

    /** @private */
    _closeSql(sqlState: SqlState, error: Error): void;
}

export interface SqlState {
    sqlId: number;
    closed: Error | undefined;
}

/** Text of an SQL statement cached on the server. */
export class Sql {
    #owner: SqlOwner;
    #state: SqlState;

    /** @private */
    constructor(owner: SqlOwner, state: SqlState) {
        this.#owner = owner;
        this.#state = state;
    }

    /** @private */
    _getSqlId(owner: SqlOwner): number {
        if (this.#owner !== owner) {
            throw new MisuseError("Attempted to use SQL text opened with other object");
        } else if (this.#state.closed !== undefined) {
            throw new ClosedError("SQL text is closed", this.#state.closed);
        }
        return this.#state.sqlId;
    }

    /** Remove the SQL text from the server, releasing resouces. */
    close(): void {
        this.#owner._closeSql(this.#state, new ClientError("SQL was manually closed"));
    }

    /** True if the SQL text is closed (removed from the server). */
    get closed() {
        return this.#state.closed !== undefined;
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
