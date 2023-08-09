import { ClientError, ProtoError, MisuseError } from "./errors.js";
import type * as proto from "./shared/proto.js";
import { impossible } from "./util.js";

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
        return null;
    } else if (typeof value === "string") {
        return value;
    } else if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            throw new RangeError("Only finite numbers (not Infinity or NaN) can be passed as arguments");
        }
        return value;
    } else if (typeof value === "bigint") {
        if (value < minInteger || value > maxInteger) {
            throw new RangeError(
                "This bigint value is too large to be represented as a 64-bit integer and passed as argument"
            );
        }
        return value;
    } else if (typeof value === "boolean") {
        return value ? 1n : 0n;
    } else if (value instanceof ArrayBuffer) {
        return new Uint8Array(value);
    } else if (value instanceof Uint8Array) {
        return value;
    } else if (value instanceof Date) {
        return +value.valueOf();
    } else if (typeof value === "object") {
        return ""+value.toString();
    } else {
        throw new TypeError("Unsupported type of value");
    }
}

const minInteger = -9223372036854775808n;
const maxInteger = 9223372036854775807n;

export function valueFromProto(value: proto.Value, intMode: IntMode): Value {
    if (value === null) {
        return null;
    } else if (typeof value === "number") {
        return value;
    } else if (typeof value === "string") {
        return value;
    } else if (typeof value === "bigint") {
        if (intMode === "number") {
            const num = Number(value);
            if (!Number.isSafeInteger(num)) {
                throw new RangeError(
                    "Received integer which is too large to be safely represented as a JavaScript number"
                );
            }
            return num;
        } else if (intMode === "bigint") {
            return value;
        } else if (intMode === "string") {
            return ""+value;
        } else {
            throw new MisuseError("Invalid value for IntMode");
        }
    } else if (value instanceof Uint8Array) {
        // TODO: we need to copy data from `Uint8Array` to return an `ArrayBuffer`. Perhaps we should add a
        // `blobMode` parameter, similar to `intMode`, which would allow the user to choose between receiving
        // `ArrayBuffer` (default, convenient) and `Uint8Array` (zero copy)?
        return value.slice().buffer;
    } else if (value === undefined) {
        throw new ProtoError("Received unrecognized variant of Value");
    } else {
        throw impossible(value, "Impossible type of Value");
    }
}

