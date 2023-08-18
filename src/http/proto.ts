// Types for the structures specific to Hrana over HTTP.

export * from "../shared/proto.js";
import { int32, Error, Stmt, StmtResult, Batch, BatchResult, DescribeResult } from "../shared/proto.js";

// ## Execute requests on a stream

export type PipelineReqBody = {
    baton: string | undefined,
    requests: Array<StreamRequest>,
}

export type PipelineRespBody = {
    baton: string | undefined,
    baseUrl: string | undefined,
    results: Array<StreamResult>
}

export type StreamResult =
    | { type: "none" }
    | StreamResultOk
    | StreamResultError

export type StreamResultOk = {
    type: "ok",
    response: StreamResponse,
}

export type StreamResultError = {
    type: "error",
    error: Error,
}

// ## Execute a batch using a cursor

export type CursorReqBody = {
    baton: string | undefined,
    batch: Batch,
}

export type CursorRespBody = {
    baton: string | undefined,
    baseUrl: string | undefined,
}

// ## Requests

export type StreamRequest =
    | CloseStreamReq
    | ExecuteStreamReq
    | BatchStreamReq
    | SequenceStreamReq
    | DescribeStreamReq
    | StoreSqlStreamReq
    | CloseSqlStreamReq
    | GetAutocommitStreamReq

export type StreamResponse =
    | { type: "none" }
    | CloseStreamResp
    | ExecuteStreamResp
    | BatchStreamResp
    | SequenceStreamResp
    | DescribeStreamResp
    | StoreSqlStreamResp
    | CloseSqlStreamResp
    | GetAutocommitStreamResp

// ### Close stream

export type CloseStreamReq = {
    type: "close",
}

export type CloseStreamResp = {
    type: "close",
}

// ### Execute a statement

export type ExecuteStreamReq = {
    type: "execute",
    stmt: Stmt,
}

export type ExecuteStreamResp = {
    type: "execute",
    result: StmtResult,
}

// ### Execute a batch

export type BatchStreamReq = {
    type: "batch",
    batch: Batch,
}

export type BatchStreamResp = {
    type: "batch",
    result: BatchResult,
}

// ### Execute a sequence of SQL statements

export type SequenceStreamReq = {
    type: "sequence",
    sql: string | undefined,
    sqlId: int32 | undefined,
}

export type SequenceStreamResp = {
    type: "sequence",
}

// ### Describe a statement

export type DescribeStreamReq = {
    type: "describe",
    sql: string | undefined,
    sqlId: int32 | undefined,
}

export type DescribeStreamResp = {
    type: "describe",
    result: DescribeResult,
}

// ### Store an SQL text on the server

export type StoreSqlStreamReq = {
    type: "store_sql",
    sqlId: int32,
    sql: string,
}

export type StoreSqlStreamResp = {
    type: "store_sql",
}

// ### Close a stored SQL text

export type CloseSqlStreamReq = {
    type: "close_sql",
    sqlId: int32,
}

export type CloseSqlStreamResp = {
    type: "close_sql",
}

// ### Get the autocommit state

export type GetAutocommitStreamReq = {
    type: "get_autocommit",
}

export type GetAutocommitStreamResp = {
    type: "get_autocommit",
    isAutocommit: boolean,
}
