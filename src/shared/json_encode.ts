import { Base64 } from "js-base64";

import * as e from "../encoding/json/encode.js";
import { impossible } from "../util.js";

import * as proto from "./proto.js";

export function Stmt(w: e.ObjectWriter, msg: proto.Stmt): void {
    if (msg.sql !== undefined) { w.string("sql", msg.sql); }
    if (msg.sqlId !== undefined) { w.number("sql_id", msg.sqlId); }
    w.arrayObjects("args", msg.args, Value);
    w.arrayObjects("named_args", msg.namedArgs, NamedArg);
    w.boolean("want_rows", msg.wantRows);
}

function NamedArg(w: e.ObjectWriter, msg: proto.NamedArg): void {
    w.string("name", msg.name);
    w.object("value", msg.value, Value);
}

export function Batch(w: e.ObjectWriter, msg: proto.Batch): void {
    w.arrayObjects("steps", msg.steps, BatchStep);
}

function BatchStep(w: e.ObjectWriter, msg: proto.BatchStep): void {
    if (msg.condition !== undefined) { w.object("condition", msg.condition, BatchCond); }
    w.object("stmt", msg.stmt, Stmt);
}

function BatchCond(w: e.ObjectWriter, msg: proto.BatchCond): void {
    w.stringRaw("type", msg.type);
    if (msg.type === "ok" || msg.type === "error") {
        w.number("step", msg.step);
    } else if (msg.type === "not") {
        w.object("cond", msg.cond, BatchCond);
    } else if (msg.type === "and" || msg.type === "or") {
        w.arrayObjects("conds", msg.conds, BatchCond);
    } else if (msg.type === "is_autocommit") {
        // do nothing
    } else {
        throw impossible(msg, "Impossible type of BatchCond");
    }
}

function Value(w: e.ObjectWriter, msg: proto.Value): void {
    if (msg === null) {
        w.stringRaw("type", "null");
    } else if (typeof msg === "bigint") {
        w.stringRaw("type", "integer");
        w.stringRaw("value", ""+msg);
    } else if (typeof msg === "number") {
        w.stringRaw("type", "float");
        w.number("value", msg);
    } else if (typeof msg === "string") {
        w.stringRaw("type", "text");
        w.string("value", msg);
    } else if (msg instanceof Uint8Array) {
        w.stringRaw("type", "blob");
        w.stringRaw("base64", Base64.fromUint8Array(msg));
    } else {
        throw impossible(msg, "Impossible type of Value");
    }
}
