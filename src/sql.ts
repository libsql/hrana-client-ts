import type { Client, SqlState } from "./client.js";
import { ClientError, ClosedError } from "./errors.js";

/** A SQL text that you can send to the database. Either a string or a reference to SQL text that is cached on
 * the server. */
export type InSql = string | Sql;

/** Text of an SQL statement cached on the server. */
export class Sql {
    #client: Client;
    #state: SqlState;

    /** @private */
    constructor(client: Client, state: SqlState) {
        this.#client = client;
        this.#state = state;
    }

    /** @private */
    _getSqlId(): number {
        if (this.#state.closed !== undefined) {
            throw new ClosedError("SQL text is closed", this.#state.closed);
        }
        return this.#state.sqlId;
    }

    /** Remove the SQL text from the server, releasing resouces. */
    close(): void {
        this.#client._closeSql(this.#state, new ClientError("SQL was manually closed"));
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

export function sqlToProto(sql: InSql): ProtoSql {
    if (sql instanceof Sql) {
        return {sqlId: sql._getSqlId()};
    } else {
        return {sql: ""+sql};
    }
}
