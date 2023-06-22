import { Base64 } from "js-base64";

import { ClientError, ProtoError } from "./errors.js";
import type * as proto from "./proto.js";

/** JavaScript values that you can receive from the database in a statement result. */
export type Value =
    | null
    | string
    | number
    | bigint
    | ArrayBuffer

/** JavaScript values that you can send to the database as an argument. */
export type InValue =
    | Value
    | boolean
    | Uint8Array
    | Date
    | RegExp
    | object

/** Possible representations of SQLite integers in JavaScript:
 *
 * - `"number"` (default): returns SQLite integers as JavaScript `number`-s (double precision floats).
 * `number` cannot precisely represent integers larger than 2^53-1 in absolute value, so attempting to read
 * larger integers will throw a `RangeError`.
 * - `"bigint"`: returns SQLite integers as JavaScript `bigint`-s (arbitrary precision integers). Bigints can
 * precisely represent all SQLite integers.
 * - `"string"`: returns SQLite integers as strings.
 */
export type IntMode = "number" | "bigint" | "string";

export function valueToProto(value: InValue): proto.Value {
    if (value === null) {
        return protoNull;
    } else if (typeof value === "string") {
        return {"type": "text", "value": value};
    } else if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            throw new RangeError("Only finite numbers (not Infinity or NaN) can be passed as arguments");
        }
        return {"type": "float", "value": +value};
    } else if (typeof value === "bigint") {
        if (value < minInteger || value > maxInteger) {
            throw new RangeError(
                "This bigint value is too large to be represented as a 64-bit integer and passed as argument"
            );
        }
        return {"type": "integer", "value": ""+value};
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

const minInteger = -9223372036854775808n;
const maxInteger = 9223372036854775807n;

export const protoNull: proto.Value = {"type": "null"};

export function valueFromProto(value: proto.Value, intMode: IntMode): Value {
    if (value["type"] === "null") {
        return null;
    } else if (value["type"] === "integer") {
        if (intMode === "number") {
            const int = parseInt(value["value"], 10);
            if (!Number.isSafeInteger(int)) {
                throw new RangeError(
                    "Received integer which cannot be safely represented as a JavaScript number"
                );
            }
            return int;
        } else if (intMode === "bigint") {
            return BigInt(value["value"]);
        } else if (intMode === "string") {
            return ""+value["value"];
        } else {
            throw new Error("Invalid value for IntMode");
        }
    } else if (value["type"] === "float") {
        return +value["value"];
    } else if (value["type"] === "text") {
        return ""+value["value"];
    } else if (value["type"] === "blob") {
        return Base64.toUint8Array(value["base64"]).buffer;
    } else {
        throw new ProtoError("Unexpected value type");
    }
}

