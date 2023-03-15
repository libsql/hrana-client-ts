import { Base64 } from "js-base64";

import { ProtoError } from "./errors.js";
import type * as proto from "./proto.js";

/** JavaScript values that you can receive from the database in statement result. */
export type Value =
    | null
    | string
    | number
    | ArrayBuffer

/** JavaScript values that you can send to the database as an argument. */
export type InValue =
    | Value
    | bigint
    | boolean
    | Uint8Array
    | Date
    | RegExp
    | object

export function valueToProto(value: InValue): proto.Value {
    if (value === null) {
        return protoNull;
    } else if (typeof value === "string") {
        return {"type": "text", "value": value};
    } else if (typeof value === "number") {
        return {"type": "float", "value": +value};
    } else if (typeof value === "bigint") {
        return {"type": "text", "value": ""+value};
    } else if (typeof value === "boolean") {
        return {"type": "integer", "value": value ? "1" : "0"};
    } else if (value instanceof ArrayBuffer) {
        return {"type": "blob", "base64": Base64.fromUint8Array(new Uint8Array(value))};
    } else if (value instanceof Uint8Array) {
        return {"type": "blob", "base64": Base64.fromUint8Array(value)};
    } else if (value instanceof Date) {
        return {"type": "float", "value": value.valueOf()};
    } else if (typeof value === "object") {
        return {"type": "text", "value": value.toString()};
    } else {
        throw new TypeError("Unsupported type of value");
    }
}

export const protoNull: proto.Value = {"type": "null"};

export function valueFromProto(value: proto.Value): Value {
    if (value["type"] === "null") {
        return null;
    } else if (value["type"] === "integer") {
        const int = parseInt(value["value"], 10);
        if (!Number.isSafeInteger(int)) {
            throw new RangeError(`Received integer ${value["value"]} which cannot be ` +
                "safely represented as a JavaScript number");
        }
        return int;
    } else if (value["type"] === "float") {
        return value["value"];
    } else if (value["type"] === "text") {
        return value["value"];
    } else if (value["type"] === "blob") {
        return Base64.toUint8Array(value["base64"]).buffer;
    } else {
        throw new ProtoError("Unexpected value type");
    }
}

