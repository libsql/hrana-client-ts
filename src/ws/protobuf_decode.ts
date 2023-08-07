import * as d from "../encoding/protobuf/decode.js";
import { Error, StmtResult, BatchResult, CursorEntry, DescribeResult } from "../shared/protobuf_decode.js";
import * as proto from "./proto.js";

export const ServerMsg: d.MessageDef<proto.ServerMsg> = {
    default() { return {type: "none"} },
    1 (r) { return {type: "hello_ok"} },
    2 (r) { return r.message(HelloErrorMsg) },
    3 (r) { return r.message(ResponseOkMsg) },
    4 (r) { return r.message(ResponseErrorMsg) },
};

const HelloErrorMsg: d.MessageDef<proto.HelloErrorMsg> = {
    default() { return {type: "hello_error", error: Error.default()} },
    1 (r, msg) { msg.error = r.message(Error) },
};

const ResponseErrorMsg: d.MessageDef<proto.ResponseErrorMsg> = {
    default() { return {type: "response_error", requestId: 0, error: Error.default()} },
    1 (r, msg) { msg.requestId = r.int32() },
    2 (r, msg) { msg.error = r.message(Error) },
};

const ResponseOkMsg: d.MessageDef<proto.ResponseOkMsg> = {
    default() {
        return {
            type: "response_ok",
            requestId: 0,
            response: {type: "none"},
        }
    },
    1 (r, msg) { msg.requestId = r.int32() },
    2 (r, msg) { msg.response = {type: "open_stream"} },
    3 (r, msg) { msg.response = {type: "close_stream"} },
    4 (r, msg) { msg.response = r.message(ExecuteResp) },
    5 (r, msg) { msg.response = r.message(BatchResp) },
    6 (r, msg) { msg.response = {type: "open_cursor"} },
    7 (r, msg) { msg.response = {type: "close_cursor"} },
    8 (r, msg) { msg.response = r.message(FetchCursorResp) },
    9 (r, msg) { msg.response = {type: "sequence"} },
    10 (r, msg) { msg.response = r.message(DescribeResp) },
    11 (r, msg) { msg.response = {type: "store_sql"} },
    12 (r, msg) { msg.response = {type: "close_sql"} },
    13 (r, msg) { msg.response = r.message(GetAutocommitResp) },
};

const ExecuteResp: d.MessageDef<proto.ExecuteResp> = {
    default() { return {type: "execute", result: StmtResult.default()} },
    1 (r, msg) { msg.result = r.message(StmtResult) },
};

const BatchResp: d.MessageDef<proto.BatchResp> = {
    default() { return {type: "batch", result: BatchResult.default()} },
    1 (r, msg) { msg.result = r.message(BatchResult) },
};

const FetchCursorResp: d.MessageDef<proto.FetchCursorResp> = {
    default() { return {type: "fetch_cursor", entries: [], done: false} },
    1 (r, msg) { msg.entries.push(r.message(CursorEntry)) },
    2 (r, msg) { msg.done = r.bool() },
};

const DescribeResp: d.MessageDef<proto.DescribeResp> = {
    default() { return {type: "describe", result: DescribeResult.default()} },
    1 (r, msg) { msg.result = r.message(DescribeResult) },
};

const GetAutocommitResp: d.MessageDef<proto.GetAutocommitResp> = {
    default() { return {type: "get_autocommit", isAutocommit: false} },
    1 (r, msg) { msg.isAutocommit = r.bool() },
};
