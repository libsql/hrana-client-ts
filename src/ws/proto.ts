// Types for the JSON structures specific to Hrana over WebSockets.

export * from "../proto.js";
import { int32, Error, Stmt, StmtResult, Batch, BatchResult, DescribeResult } from "../proto.js";

// ## Messages

export type ClientMsg =
    | HelloMsg
    | RequestMsg

export type ServerMsg =
    | HelloOkMsg
    | HelloErrorMsg
    | ResponseOkMsg
    | ResponseErrorMsg

// ### Hello

export type HelloMsg = {
    "type": "hello",
    "jwt": string | null,
}

export type HelloOkMsg = {
    "type": "hello_ok",
}

export type HelloErrorMsg = {
    "type": "hello_error",
    "error": Error,
}

// ### Request/response

export type RequestMsg = {
    "type": "request",
    "request_id": int32,
    "request": Request,
}

export type ResponseOkMsg = {
    "type": "response_ok",
    "request_id": int32,
    "response": Response,
}

export type ResponseErrorMsg = {
    "type": "response_error",
    "request_id": int32,
    "error": Error,
}

// ## Requests

export type Request =
    | OpenStreamReq
    | CloseStreamReq
    | ExecuteReq
    | BatchReq
    | DescribeReq
    | SequenceReq
    | StoreSqlReq
    | CloseSqlReq

export type Response =
    | OpenStreamResp
    | CloseStreamResp
    | ExecuteResp
    | BatchResp
    | DescribeResp
    | SequenceResp
    | StoreSqlResp
    | CloseSqlResp

// ### Open stream

export type OpenStreamReq = {
    "type": "open_stream",
    "stream_id": int32,
}

export type OpenStreamResp = {
    "type": "open_stream",
}

// ### Close stream

export type CloseStreamReq = {
    "type": "close_stream",
    "stream_id": int32,
}

export type CloseStreamResp = {
    "type": "close_stream",
}

// ### Execute a statement

export type ExecuteReq = {
    "type": "execute",
    "stream_id": int32,
    "stmt": Stmt,
}

export type ExecuteResp = {
    "type": "execute",
    "result": StmtResult,
}

// ### Execute a batch

export type BatchReq = {
    "type": "batch",
    "stream_id": int32,
    "batch": Batch,
}

export type BatchResp = {
    "type": "batch",
    "result": BatchResult,
}

// ### Describe a statement

export type DescribeReq = {
    "type": "describe",
    "stream_id": int32,
    "sql"?: string | undefined,
    "sql_id"?: int32 | undefined,
}

export type DescribeResp = {
    "type": "describe",
    "result": DescribeResult,
}

// ### Execute a sequence of SQL statements

export type SequenceReq = {
    "type": "sequence",
    "stream_id": int32,
    "sql"?: string | null,
    "sql_id"?: int32 | null,
}

export type SequenceResp = {
    "type": "sequence",
}

// ### Store an SQL text on the server

export type StoreSqlReq = {
    "type": "store_sql",
    "sql_id": int32,
    "sql": string,
}

export type StoreSqlResp = {
    "type": "store_sql",
}

// ### Close a stored SQL text

export type CloseSqlReq = {
    "type": "close_sql",
    "sql_id": int32,
}

export type CloseSqlResp = {
    "type": "close_sql",
}

