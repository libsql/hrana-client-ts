import * as d from "../encoding/protobuf/decode.js";
import { Error, StmtResult, BatchResult, CursorEntry, DescribeResult } from "../shared/protobuf_decode.js";
import * as proto from "./proto.js";

export const PipelineRespBody: d.MessageDef<proto.PipelineRespBody> = {
    default() { return {baton: undefined, baseUrl: undefined, results: []} },
    1 (r, msg) { msg.baton = r.string() },
    2 (r, msg) { msg.baseUrl = r.string() },
    3 (r, msg) { msg.results.push(r.message(StreamResult)) },
};

const StreamResult: d.MessageDef<proto.StreamResult> = {
    default() { return {type: "none"} },
    1 (r) { return {type: "ok", response: r.message(StreamResponse)} },
    2 (r) { return {type: "error", error: r.message(Error)} },
};

const StreamResponse: d.MessageDef<proto.StreamResponse> = {
    default() { return {type: "none"} },
    1 (r) { return {type: "close"} },
    2 (r) { return r.message(ExecuteStreamResp) },
    3 (r) { return r.message(BatchStreamResp) },
    4 (r) { return {type: "sequence"} },
    5 (r) { return r.message(DescribeStreamResp) },
    6 (r) { return {type: "store_sql"} },
    7 (r) { return {type: "close_sql"} },
    8 (r) { return r.message(GetAutocommitStreamResp) },
};

const ExecuteStreamResp: d.MessageDef<proto.ExecuteStreamResp> = {
    default() { return {type: "execute", result: StmtResult.default()} },
    1 (r, msg) { msg.result = r.message(StmtResult) },
};

const BatchStreamResp: d.MessageDef<proto.BatchStreamResp> = {
    default() { return {type: "batch", result: BatchResult.default()} },
    1 (r, msg) { msg.result = r.message(BatchResult) },
};

const DescribeStreamResp: d.MessageDef<proto.DescribeStreamResp> = {
    default() { return {type: "describe", result: DescribeResult.default()} },
    1 (r, msg) { msg.result = r.message(DescribeResult) },
};

const GetAutocommitStreamResp: d.MessageDef<proto.GetAutocommitStreamResp> = {
    default() { return {type: "get_autocommit", isAutocommit: false} },
    1 (r, msg) { msg.isAutocommit = r.bool() },
};

export const CursorRespBody: d.MessageDef<proto.CursorRespBody> = {
    default() { return {baton: undefined, baseUrl: undefined} },
    1 (r, msg) { msg.baton = r.string() },
    2 (r, msg) { msg.baseUrl = r.string() },
};

