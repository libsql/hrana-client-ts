import * as e from "../encoding/protobuf/encode.js";
import { Stmt, Batch } from "../shared/protobuf_encode.js";
import { impossible } from "../util.js";
import * as proto from "./proto.js";

export function PipelineRequestBody(w: e.MessageWriter, msg: proto.PipelineRequestBody): void {
    if (msg.baton !== undefined) { w.string(1, msg.baton); }
    for (const req of msg.requests) { w.message(2, req, StreamRequest); }
}

function StreamRequest(w: e.MessageWriter, msg: proto.StreamRequest): void {
    if (msg.type === "close") {
        w.message(1, msg, CloseStreamReq);
    } else if (msg.type === "execute") {
        w.message(2, msg, ExecuteStreamReq);
    } else if (msg.type === "batch") {
        w.message(3, msg, BatchStreamReq);
    } else if (msg.type === "sequence") {
        w.message(4, msg, SequenceStreamReq);
    } else if (msg.type === "describe") {
        w.message(5, msg, DescribeStreamReq);
    } else if (msg.type === "store_sql") {
        w.message(6, msg, StoreSqlStreamReq);
    } else if (msg.type === "close_sql") {
        w.message(7, msg, CloseSqlStreamReq);
    } else if (msg.type === "get_autocommit") {
        w.message(8, msg, GetAutocommitStreamReq);
    } else {
        throw impossible(msg, "Impossible type of StreamRequest");
    }
}

function CloseStreamReq(_w: e.MessageWriter, _msg: proto.CloseStreamReq): void {
}

function ExecuteStreamReq(w: e.MessageWriter, msg: proto.ExecuteStreamReq): void {
    w.message(1, msg.stmt, Stmt);
}

function BatchStreamReq(w: e.MessageWriter, msg: proto.BatchStreamReq): void {
    w.message(1, msg.batch, Batch);
}

function SequenceStreamReq(w: e.MessageWriter, msg: proto.SequenceStreamReq): void {
    if (msg.sql !== undefined) { w.string(1, msg.sql); }
    if (msg.sqlId !== undefined) { w.int32(2, msg.sqlId); }
}

function DescribeStreamReq(w: e.MessageWriter, msg: proto.DescribeStreamReq): void {
    if (msg.sql !== undefined) { w.string(1, msg.sql); }
    if (msg.sqlId !== undefined) { w.int32(2, msg.sqlId); }
}

function StoreSqlStreamReq(w: e.MessageWriter, msg: proto.StoreSqlStreamReq): void {
    w.int32(1, msg.sqlId);
    w.string(2, msg.sql);
}

function CloseSqlStreamReq(w: e.MessageWriter, msg: proto.CloseSqlStreamReq): void {
    w.int32(1, msg.sqlId);
}

function GetAutocommitStreamReq(_w: e.MessageWriter, _msg: proto.GetAutocommitStreamReq): void {
}
