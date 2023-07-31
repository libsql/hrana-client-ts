import * as e from "../encoding/protobuf/encode.js";
import { Stmt, Batch } from "../shared/protobuf_encode.js";
import { impossible } from "../util.js";

import * as proto from "./proto.js";

export function ClientMsg(w: e.MessageWriter, msg: proto.ClientMsg): void {
    if (msg.type === "hello") {
        w.message(1, msg, HelloMsg);
    } else if (msg.type === "request") {
        w.message(2, msg, RequestMsg);
    } else {
        throw impossible(msg, "Impossible type of ClientMsg");
    }
}

function HelloMsg(w: e.MessageWriter, msg: proto.HelloMsg): void {
    if (msg.jwt !== undefined) { w.string(1, msg.jwt); }
}

function RequestMsg(w: e.MessageWriter, msg: proto.RequestMsg): void {
    w.int32(1, msg.requestId);

    const request = msg.request;
    if (request.type === "open_stream") {
        w.message(2, request, OpenStreamReq);
    } else if (request.type === "close_stream") {
        w.message(3, request, CloseStreamReq);
    } else if (request.type === "execute") {
        w.message(4, request, ExecuteReq);
    } else if (request.type === "batch") {
        w.message(5, request, BatchReq);
    } else if (request.type === "open_cursor") {
        w.message(6, request, OpenCursorReq);
    } else if (request.type === "close_cursor") {
        w.message(7, request, CloseCursorReq);
    } else if (request.type === "fetch_cursor") {
        w.message(8, request, FetchCursorReq);
    } else if (request.type === "sequence") {
        w.message(9, request, SequenceReq);
    } else if (request.type === "describe") {
        w.message(10, request, DescribeReq);
    } else if (request.type === "store_sql") {
        w.message(11, request, StoreSqlReq);
    } else if (request.type === "close_sql") {
        w.message(12, request, CloseSqlReq);
    } else if (request.type === "get_autocommit") {
        w.message(13, request, GetAutocommitReq);
    } else {
        throw impossible(request, "Impossible type of Request");
    }
}

function OpenStreamReq(w: e.MessageWriter, msg: proto.OpenStreamReq): void {
    w.int32(1, msg.streamId);
}

function CloseStreamReq(w: e.MessageWriter, msg: proto.CloseStreamReq): void {
    w.int32(1, msg.streamId);
}

function ExecuteReq(w: e.MessageWriter, msg: proto.ExecuteReq): void {
    w.int32(1, msg.streamId);
    w.message(2, msg.stmt, Stmt);
}

function BatchReq(w: e.MessageWriter, msg: proto.BatchReq): void {
    w.int32(1, msg.streamId);
    w.message(2, msg.batch, Batch);
}

function OpenCursorReq(w: e.MessageWriter, msg: proto.OpenCursorReq): void {
    w.int32(1, msg.streamId);
    w.int32(2, msg.cursorId);
    w.message(3, msg.batch, Batch);
}

function CloseCursorReq(w: e.MessageWriter, msg: proto.CloseCursorReq): void {
    w.int32(1, msg.cursorId);
}

function FetchCursorReq(w: e.MessageWriter, msg: proto.FetchCursorReq): void {
    w.int32(1, msg.cursorId);
    w.uint32(2, msg.maxCount);
}

function SequenceReq(w: e.MessageWriter, msg: proto.SequenceReq): void {
    w.int32(1, msg.streamId);
    if (msg.sql !== undefined) { w.string(2, msg.sql); }
    if (msg.sqlId !== undefined) { w.int32(3, msg.sqlId); }
}

function DescribeReq(w: e.MessageWriter, msg: proto.DescribeReq): void {
    w.int32(1, msg.streamId);
    if (msg.sql !== undefined) { w.string(2, msg.sql); }
    if (msg.sqlId !== undefined) { w.int32(3, msg.sqlId); }
}

function StoreSqlReq(w: e.MessageWriter, msg: proto.StoreSqlReq): void {
    w.int32(1, msg.sqlId);
    w.string(2, msg.sql);
}

function CloseSqlReq(w: e.MessageWriter, msg: proto.CloseSqlReq): void {
    w.int32(1, msg.sqlId);
}

function GetAutocommitReq(w: e.MessageWriter, msg: proto.GetAutocommitReq): void {
    w.int32(1, msg.streamId);
}
