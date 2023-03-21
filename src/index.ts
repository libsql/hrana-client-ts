import WebSocket from "isomorphic-ws";

import { Client } from "./client.js";
import type * as proto from "./proto.js";

export { Client } from "./client.js";
export * from "./errors.js";
export { Prog, ProgExecute, ProgOp, ProgExpr, ProgVar } from "./prog.js";
export type { StmtResult, RowsResult, RowResult, ValueResult, Row } from "./result.js";
export type { InStmt, InStmtArgs } from "./stmt.js";
export { Stmt } from "./stmt.js";
export { Stream } from "./stream.js";
export type { Value, InValue } from "./value.js";
export type { proto };

/** Open a Hrana client connected to the given `url`. */
export function open(url: string, jwt?: string): Client {
    const socket = new WebSocket(url, ["hrana1"]);
    return new Client(socket, jwt ?? null);
}
