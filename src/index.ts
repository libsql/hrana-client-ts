import { WebSocket } from "@libsql/isomorphic-ws";

import { Client, protocolVersions } from "./client.js";
import { WebSocketUnsupportedError } from "./errors.js";
import type * as proto from "./proto.js";

import { HttpClient } from "./http/client.js";
import { WsClient } from "./ws/client.js";

export type { ProtocolVersion } from "./client.js";
export { Client } from "./client.js";
export type { DescribeResult, DescribeColumn } from "./describe.js";
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
export type { Value, InValue, IntMode } from "./value.js";

export { HttpClient } from "./http/client.js";
export { HttpStream } from "./http/stream.js";
export { WsClient } from "./ws/client.js";
export { WsStream } from "./ws/stream.js";

/** Open a Hrana client over WebSocket connected to the given `url`. */
export function openWs(url: string | URL, jwt?: string): WsClient {
    if (typeof WebSocket === "undefined") {
        throw new WebSocketUnsupportedError("WebSockets are not supported in this environment");
    }
    const socket = new WebSocket(url, Array.from(protocolVersions.keys()));
    return new WsClient(socket, jwt ?? null);
}

/** Open a Hrana client over HTTP connected to the given `url`.
 *
 * If the `customFetch` argument is passed and not `undefined`, it is used in place of the `fetch` function
 * from `@libsql/isomorphic-fetch`. This function is always called with a `Request` object from
 * `@libsql/isomorphic-fetch`.
 */
export function openHttp(url: string | URL, jwt?: string, customFetch?: unknown | undefined): HttpClient {
    return new HttpClient(url instanceof URL ? url : new URL(url), jwt ?? null, customFetch);
}
