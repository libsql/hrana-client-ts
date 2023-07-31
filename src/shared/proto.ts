// Types for the protocol structures that are shared for WebSocket and HTTP

export type int32 = number;
export type uint32 = number;

// Errors

export type Error = {
    message: string,
    code: string | undefined,
}

// Statements

export type Stmt = {
    sql: string | undefined,
    sqlId: int32 | undefined,
    args: Array<Value>,
    namedArgs: Array<NamedArg>,
    wantRows: boolean,
}

export type NamedArg = {
    name: string,
    value: Value,
}

export type StmtResult = {
    cols: Array<Col>,
    rows: Array<Array<Value>>,
    affectedRowCount: number,
    lastInsertRowid: bigint | undefined,
}

export type Col = {
    name: string | undefined,
    decltype: string | undefined,
}

// Batches

export type Batch = {
    steps: Array<BatchStep>,
}

export type BatchStep = {
    condition: BatchCond | undefined,
    stmt: Stmt,
}

export type BatchCond =
    | { type: "ok", step: uint32 }
    | { type: "error", step: uint32 }
    | { type: "not", cond: BatchCond }
    | { type: "and", conds: Array<BatchCond> }
    | { type: "or", conds: Array<BatchCond> }
    | { type: "is_autocommit" }

export type BatchResult = {
    stepResults: Map<uint32, StmtResult>,
    stepErrors: Map<uint32, Error>,
}

// Cursor entries

export type CursorEntry =
    | { type: "none" }
    | StepBeginEntry
    | StepEndEntry
    | StepErrorEntry
    | RowEntry
    | ErrorEntry

export type StepBeginEntry = {
    type: "step_begin",
    step: uint32,
    cols: Array<Col>,
}

export type StepEndEntry = {
    type: "step_end",
    affectedRowCount: number,
    lastInsertRowid: bigint | undefined,
}

export type StepErrorEntry = {
    type: "step_error",
    step: uint32,
    error: Error,
}

export type RowEntry = {
    type: "row",
    row: Array<Value>,
}

export type ErrorEntry = {
    type: "error",
    error: Error,
}

// Describe

export type DescribeResult = {
    params: Array<DescribeParam>,
    cols: Array<DescribeCol>,
    isExplain: boolean,
    isReadonly: boolean,
}

export type DescribeParam = {
    name: string | undefined,
}

export type DescribeCol = {
    name: string,
    decltype: string | undefined,
}

// Values

// NOTE: contrary to other enum structures, we don't wrap every `Value` in an
// object with `type` property, because there might be a lot of `Value`
// instances and we don't want to create an unnecessary object for each one
export type Value =
    | undefined
    | null
    | bigint
    | number
    | string
    | Uint8Array
