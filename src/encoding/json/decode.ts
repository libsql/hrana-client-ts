import { ProtoError } from "../../errors.js";

export type Value = Obj | Array<Value> | string | number | true | false | null;
export type Obj = {[key: string]: Value | undefined};

export type ObjectFun<T> = (obj: Obj) => T;

export function string(value: Value | undefined): string {
    if (typeof value === "string") {
        return value;
    }
    throw typeError(value, "string");
}

export function stringOpt(value: Value | undefined): string | undefined {
    if (value === null || value === undefined) {
        return undefined;
    } else if (typeof value === "string") {
        return value;
    }
    throw typeError(value, "string or null");
}

export function number(value: Value | undefined): number {
    if (typeof value === "number") {
        return value;
    }
    throw typeError(value, "number");
}

export function boolean(value: Value | undefined): boolean {
    if (typeof value === "boolean") {
        return value;
    }
    throw typeError(value, "boolean");
}

export function array(value: Value | undefined): Array<Value> {
    if (Array.isArray(value)) {
        return value;
    }
    throw typeError(value, "array");
}

export function object(value: Value | undefined): Obj {
    if (value !== null && typeof value === "object" && !Array.isArray(value)) {
        return value;
    }
    throw typeError(value, "object");
}

export function arrayObjectsMap<T>(value: Value | undefined, fun: ObjectFun<T>): Array<T> {
    return array(value).map((elemValue) => fun(object(elemValue)));
}

function typeError(value: Value | undefined, expected: string): Error {
    if (value === undefined) {
        return new ProtoError(`Expected ${expected}, but the property was missing`);
    }

    let received: string = typeof value;
    if (value === null) {
        received = "null";
    } else if (Array.isArray(value)) {
        received = "array";
    }
    return new ProtoError(`Expected ${expected}, received ${received}`);
}

export function readJsonObject<T>(value: unknown, fun: ObjectFun<T>): T {
    return fun(object(value as Value));
}
