// Types for the structures specific to Hrana over WebSockets.

export * from "../shared/proto.js";
import {
    int32, uint32, Error, Stmt, StmtResult,
    Batch, BatchResult, CursorEntry, DescribeResult,
} from "../shared/proto.js";

export type ClientMsg =
    | HelloMsg
    | RequestMsg

export type ServerMsg =
    | { type: "none" }
    | HelloOkMsg
    | HelloErrorMsg
    | ResponseOkMsg
    | ResponseErrorMsg

// Hello

export type HelloMsg = {
    type: "hello",
    jwt: string | undefined,
}

export type HelloOkMsg = {
    type: "hello_ok",
}

export type HelloErrorMsg = {
    type: "hello_error",
    error: Error,
}

// Request/response

export type RequestMsg = {
    type: "request",
    requestId: int32,
    request: Request,
}

export type ResponseOkMsg = {
    type: "response_ok",
    requestId: int32,
    response: Response,
}

export type ResponseErrorMsg = {
    type: "response_error",
    requestId: int32,
    error: Error,
}

// Requests

export type Request =
    | OpenStreamReq
    | CloseStreamReq
    | ExecuteReq
    | BatchReq
    | OpenCursorReq
    | CloseCursorReq
    | FetchCursorReq
    | SequenceReq
    | DescribeReq
    | StoreSqlReq
    | CloseSqlReq
    | GetAutocommitReq

export type Response =
    | { type: "none" }
    | OpenStreamResp
    | CloseStreamResp
    | ExecuteResp
    | BatchResp
    | OpenCursorResp
    | CloseCursorResp
    | FetchCursorResp
    | SequenceResp
    | DescribeResp
    | StoreSqlResp
    | CloseSqlResp
    | GetAutocommitResp

// Open stream

export type OpenStreamReq = {
    type: "open_stream",
    streamId: int32,
}

export type OpenStreamResp = {
    type: "open_stream",
}

// Close stream

export type CloseStreamReq = {
    type: "close_stream",
    streamId: int32,
}

export type CloseStreamResp = {
    type: "close_stream",
}

// Execute a statement

export type ExecuteReq = {
    type: "execute",
    streamId: int32,
    stmt: Stmt,
}

export type ExecuteResp = {
    type: "execute",
    result: StmtResult,
}

// Execute a batch

export type BatchReq = {
    type: "batch",
    streamId: int32,
    batch: Batch,
}

export type BatchResp = {
    type: "batch",
    result: BatchResult,
}

// Open a cursor executing a batch

export type OpenCursorReq = {
    type: "open_cursor",
    streamId: int32,
    cursorId: int32,
    batch: Batch,
}

export type OpenCursorResp = {
    type: "open_cursor",
}

// Close a cursor

export type CloseCursorReq = {
    type: "close_cursor",
    cursorId: int32,
}

export type CloseCursorResp = {
    type: "close_cursor",
}


// Fetch entries from a cursor

export type FetchCursorReq = {
    type: "fetch_cursor",
    cursorId: int32,
    maxCount: uint32,
}

export type FetchCursorResp = {
    type: "fetch_cursor",
    entries: Array<CursorEntry>,
    done: boolean,
}

// Describe a statement

export type DescribeReq = {
    type: "describe",
    streamId: int32,
    sql: string | undefined,
    sqlId: int32 | undefined,
}

export type DescribeResp = {
    type: "describe",
    result: DescribeResult,
}

// Execute a sequence of SQL statements

export type SequenceReq = {
    type: "sequence",
    streamId: int32,
    sql: string | undefined,
    sqlId: int32 | undefined,
}

export type SequenceResp = {
    type: "sequence",
}

// Store an SQL text on the server

export type StoreSqlReq = {
    type: "store_sql",
    sqlId: int32,
    sql: string,
}

export type StoreSqlResp = {
    type: "store_sql",
}

// Close a stored SQL text

export type CloseSqlReq = {
    type: "close_sql",
    sqlId: int32,
}

export type CloseSqlResp = {
    type: "close_sql",
}

// Get the autocommit state

export type GetAutocommitReq = {
    type: "get_autocommit",
    streamId: int32,
}

export type GetAutocommitResp = {
    type: "get_autocommit",
    isAutocommit: boolean,
}
