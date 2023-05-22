import { WebSocket } from "@libsql/isomorphic-ws";

import { Client, protocolVersions } from "./client.js";
import { WebSocketUnsupportedError } from "./errors.js";
import type * as proto from "./proto.js";

import { WsClient } from "./ws/client.js";

export type { ProtocolVersion } from "./client.js";
export { Client } from "./client.js";
export * from "./errors.js";
export { Batch, BatchStep, BatchCond } from "./batch.js";
export type { ParsedLibsqlUrl } from "./libsql_url.js";
export { parseLibsqlUrl } from "./libsql_url.js";
export type { StmtResult, RowsResult, RowResult, ValueResult, Row } from "./result.js";
export type { InSql, SqlOwner } from "./sql.js";
export { Sql } from "./sql.js";
export type { InStmt, InStmtArgs } from "./stmt.js";
export { Stmt } from "./stmt.js";
export { Stream } from "./stream.js";
export type { Value, InValue } from "./value.js";

export { WsClient } from "./ws/client.js";
export { WsStream } from "./ws/stream.js";

/** Open a Hrana client connected to the given `url`. */
export function open(url: string | URL, jwt?: string): WsClient {
    if (typeof WebSocket === "undefined") {
        throw new WebSocketUnsupportedError("WebSockets are not supported in this environment");
    }
    const socket = new WebSocket(url, Array.from(protocolVersions.keys()));
    return new WsClient(socket, jwt ?? null);
}
