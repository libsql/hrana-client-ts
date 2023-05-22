// Types for the JSON structures that are common for WebSockets and HTTP
//
// The structure of this file follows the Hrana specification.

export type int32 = number

// ## Errors

export type Error = {
    "message": string,
    "code"?: string | null,
}

// ## Statements

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
    "decltype"?: string | null,
}

// ## Batches

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

// ## Describe

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

// ## Values

export type Value =
    | { "type": "null" }
    | { "type": "integer", "value": string }
    | { "type": "float", "value": number }
    | { "type": "text", "value": string }
    | { "type": "blob", "base64": string }
