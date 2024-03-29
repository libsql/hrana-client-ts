import * as d from "../encoding/protobuf/decode.js";
import { ProtoError } from "../errors.js";

import * as proto from "./proto.js";

export const Error: d.MessageDef<proto.Error> = {
    default() { return {message: "", code: undefined}; },
    1 (r, msg) { msg.message = r.string(); },
    2 (r, msg) { msg.code = r.string(); },
};



export const StmtResult: d.MessageDef<proto.StmtResult> = {
    default() {
        return {
            cols: [],
            rows: [],
            affectedRowCount: 0,
            lastInsertRowid: undefined,
        };
    },
    1 (r, msg) { msg.cols.push(r.message(Col)) },
    2 (r, msg) { msg.rows.push(r.message(Row)) },
    3 (r, msg) { msg.affectedRowCount = Number(r.uint64()) },
    4 (r, msg) { msg.lastInsertRowid = r.sint64() },
};

const Col: d.MessageDef<proto.Col> = {
    default() { return {name: undefined, decltype: undefined} },
    1 (r, msg) { msg.name = r.string() },
    2 (r, msg) { msg.decltype = r.string() },
};

const Row: d.MessageDef<Array<proto.Value>> = {
    default() { return [] },
    1 (r, msg) { msg.push(r.message(Value)); },
};



export const BatchResult: d.MessageDef<proto.BatchResult> = {
    default() { return {stepResults: new Map(), stepErrors: new Map()} },
    1 (r, msg) {
        const [key, value] = r.message(BatchResultStepResult);
        msg.stepResults.set(key, value);
    },
    2 (r, msg) {
        const [key, value] = r.message(BatchResultStepError);
        msg.stepErrors.set(key, value);
    },
};

const BatchResultStepResult: d.MessageDef<[number, proto.StmtResult]> = {
    default() { return [0, StmtResult.default()] },
    1 (r, msg) { msg[0] = r.uint32() },
    2 (r, msg) { msg[1] = r.message(StmtResult) },
};

const BatchResultStepError: d.MessageDef<[number, proto.Error]> = {
    default() { return [0, Error.default()] },
    1 (r, msg) { msg[0] = r.uint32() },
    2 (r, msg) { msg[1] = r.message(Error) },
};



export const CursorEntry: d.MessageDef<proto.CursorEntry> = {
    default() { return {type: "none"} },
    1 (r) { return r.message(StepBeginEntry) },
    2 (r) { return r.message(StepEndEntry) },
    3 (r) { return r.message(StepErrorEntry) },
    4 (r) { return {type: "row", row: r.message(Row)} },
    5 (r) { return {type: "error", error: r.message(Error)} },
};

const StepBeginEntry: d.MessageDef<proto.StepBeginEntry> = {
    default() { return {type: "step_begin", step: 0, cols: []} },
    1 (r, msg) { msg.step = r.uint32() },
    2 (r, msg) { msg.cols.push(r.message(Col)) },
};

const StepEndEntry: d.MessageDef<proto.StepEndEntry> = {
    default() { 
        return {
            type: "step_end",
            affectedRowCount: 0,
            lastInsertRowid: undefined,
        }
    },
    1 (r, msg) { msg.affectedRowCount = r.uint32() },
    2 (r, msg) { msg.lastInsertRowid = r.uint64() },
};

const StepErrorEntry: d.MessageDef<proto.StepErrorEntry> = {
    default() {
        return {
            type: "step_error",
            step: 0,
            error: Error.default(),
        }
    },
    1 (r, msg) { msg.step = r.uint32() },
    2 (r, msg) { msg.error = r.message(Error) },
};



export const DescribeResult: d.MessageDef<proto.DescribeResult> = {
    default() {
        return {
            params: [],
            cols: [],
            isExplain: false,
            isReadonly: false,
        }
    },
    1 (r, msg) { msg.params.push(r.message(DescribeParam)) },
    2 (r, msg) { msg.cols.push(r.message(DescribeCol)) },
    3 (r, msg) { msg.isExplain = r.bool() },
    4 (r, msg) { msg.isReadonly = r.bool() },
};

const DescribeParam: d.MessageDef<proto.DescribeParam> = {
    default() { return {name: undefined} },
    1 (r, msg) { msg.name = r.string() },
};

const DescribeCol: d.MessageDef<proto.DescribeCol> = {
    default() { return {name: "", decltype: undefined} },
    1 (r, msg) { msg.name = r.string() },
    2 (r, msg) { msg.decltype = r.string() },
};



const Value: d.MessageDef<proto.Value> = {
    default() { return undefined },
    1 (r) { return null },
    2 (r) { return r.sint64() },
    3 (r) { return r.double() },
    4 (r) { return r.string() },
    5 (r) { return r.bytes() },
};
