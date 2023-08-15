import * as e from "../encoding/protobuf/encode.js";
import { impossible } from "../util.js";

import * as proto from "./proto.js"

export function Stmt(w: e.MessageWriter, msg: proto.Stmt): void {
    if (msg.sql !== undefined) { w.string(1, msg.sql); }
    if (msg.sqlId !== undefined) { w.int32(2, msg.sqlId); }
    for (const arg of msg.args) { w.message(3, arg, Value); }
    for (const arg of msg.namedArgs) { w.message(4, arg, NamedArg); }
    w.bool(5, msg.wantRows);
}

function NamedArg(w: e.MessageWriter, msg: proto.NamedArg): void {
    w.string(1, msg.name);
    w.message(2, msg.value, Value);
}

export function Batch(w: e.MessageWriter, msg: proto.Batch): void {
    for (const step of msg.steps) { w.message(1, step, BatchStep); }
}

function BatchStep(w: e.MessageWriter, msg: proto.BatchStep): void {
    if (msg.condition !== undefined) { w.message(1, msg.condition, BatchCond); }
    w.message(2, msg.stmt, Stmt);
}

function BatchCond(w: e.MessageWriter, msg: proto.BatchCond): void {
    if (msg.type === "ok") {
        w.uint32(1, msg.step);
    } else if (msg.type === "error") {
        w.uint32(2, msg.step);
    } else if (msg.type === "not") {
        w.message(3, msg.cond, BatchCond);
    } else if (msg.type === "and") {
        w.message(4, msg.conds, BatchCondList);
    } else if (msg.type === "or") {
        w.message(5, msg.conds, BatchCondList);
    } else if (msg.type === "is_autocommit") {
        w.message(6, undefined, Empty);
    } else {
        throw impossible(msg, "Impossible type of BatchCond");
    }
}

function BatchCondList(w: e.MessageWriter, msg: Array<proto.BatchCond>): void {
    for (const cond of msg) { w.message(1, cond, BatchCond); }
}

function Value(w: e.MessageWriter, msg: proto.Value): void {
    if (msg === null) {
        w.message(1, undefined, Empty);
    } else if (typeof msg === "bigint") {
        w.sint64(2, msg);
    } else if (typeof msg === "number") {
        w.double(3, msg);
    } else if (typeof msg === "string") {
        w.string(4, msg);
    } else if (msg instanceof Uint8Array) {
        w.bytes(5, msg);
    } else if (msg === undefined) {
        // do nothing
    } else {
        throw impossible(msg, "Impossible type of Value");
    }
}

function Empty(_w: e.MessageWriter, _msg: undefined): void {
    // do nothing
}
