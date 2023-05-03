// TypeScript types for the messages in the Hrana protocol
//
// The structure of this file follows the specification in HRANA_SPEC.md

export type int32 = number

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

// ### Errors

export type Error = {
    "message": string,
    "code"?: string | null,
}

// ## Requests

export type Request =
    | OpenStreamReq
    | CloseStreamReq
    | ExecuteReq
    | BatchReq
    | DescribeReq
    | StoreSqlReq
    | CloseSqlReq

export type Response =
    | OpenStreamResp
    | CloseStreamResp
    | ExecuteResp
    | BatchResp
    | DescribeResp
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

export type Stmt = {
    "sql"?: string | undefined,
    "sql_id"?: int32 | undefined,
    "args"?: Array<Value>,
    "named_args"?: Array<NamedArg>,
    "want_rows": boolean,
}

export type NamedArg = {
    "name": string,
    "value": Value,
}

export type StmtResult = {
    "cols": Array<Col>,
    "rows": Array<Array<Value>>,
    "affected_row_count": number,
    "last_insert_rowid"?: string | null,
}

export type Col = {
    "name": string | null,
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

export type Batch = {
    "steps": Array<BatchStep>,
}

export type BatchStep = {
    "condition"?: BatchCond | null,
    "stmt": Stmt,
}

export type BatchResult = {
    "step_results": Array<StmtResult | null>,
    "step_errors": Array<Error | null>,
}

export type BatchCond =
    | { "type": "ok", "step": int32 }
    | { "type": "error", "step": int32 }
    | { "type": "not", "cond": BatchCond }
    | { "type": "and", "conds": Array<BatchCond> }
    | { "type": "or", "conds": Array<BatchCond> }

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

export type DescribeResult = {
    "params": Array<DescribeParam>,
    "cols": Array<DescribeCol>,
    "is_explain": boolean,
    "is_readonly": boolean,
}

export type DescribeParam = {
    "name": string | null,
}

export type DescribeCol = {
    "name": string,
    "decltype": string | null,
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

// ### Values

export type Value =
    | { "type": "null" }
    | { "type": "integer", "value": string }
    | { "type": "float", "value": number }
    | { "type": "text", "value": string }
    | { "type": "blob", "base64": string }
