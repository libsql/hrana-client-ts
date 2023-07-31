import { ProtoError } from "../../errors.js";
import { VARINT, FIXED_64, LENGTH_DELIMITED, FIXED_32 } from "./util.js";

export interface MessageDef<T> {
    default(): T;
    [tag: number]: (r: FieldReader, msg: T) => T | void;
}

class MessageReader {
    #array: Uint8Array;
    #view: DataView;
    #pos: number;

    constructor(array: Uint8Array) {
        this.#array = array;
        this.#view = new DataView(array.buffer, array.byteOffset, array.byteLength);
        this.#pos = 0;
    }

    varint(): number {
        let value = 0;
        for (let shift = 0; ; shift += 7) {
            const byte = this.#array[this.#pos++];
            value |= (byte & 0x7f) << shift;
            if (!(byte & 0x80)) {
                break;
            }
        }
        return value;
    }

    varintBig(): bigint {
        let value = 0n;
        for (let shift = 0n; ; shift += 7n) {
            const byte = this.#array[this.#pos++];
            value |= BigInt(byte & 0x7f) << shift;
            if (!(byte & 0x80)) {
                break;
            }
        }
        return value;
    }

    bytes(length: number): Uint8Array {
        const array = new Uint8Array(
            this.#array.buffer,
            this.#array.byteOffset + this.#pos,
            length,
        )
        this.#pos += length;
        return array;
    }

    double(): number {
        const value = this.#view.getFloat64(this.#pos, true);
        this.#pos += 8;
        return value;
    }

    skipVarint(): void {
        for (;;) {
            const byte = this.#array[this.#pos++];
            if (!(byte & 0x80)) {
                break;
            }
        }
    }

    skip(count: number): void {
        this.#pos += count;
    }

    eof(): boolean {
        return this.#pos >= this.#array.byteLength;
    }
}

export class FieldReader {
    #reader: MessageReader;
    #wireType: number;

    constructor(reader: MessageReader) {
        this.#reader = reader;
        this.#wireType = -1;
    }

    setup(wireType: number): void {
        this.#wireType = wireType;
    }

    #expect(expectedWireType: number): void {
        if (this.#wireType !== expectedWireType) {
            throw new ProtoError(`Expected wire type ${expectedWireType}, got ${this.#wireType}`);
        }
        this.#wireType = -1;
    }

    bytes(): Uint8Array {
        this.#expect(LENGTH_DELIMITED);
        const length = this.#reader.varint();
        return this.#reader.bytes(length);
    }

    string(): string {
        return new TextDecoder().decode(this.bytes());
    }

    message<T>(def: MessageDef<T>): T {
        return readProtobufMessage(this.bytes(), def);
    }

    int32(): number {
        this.#expect(VARINT);
        return this.#reader.varint();
    }

    uint32(): number {
        return this.int32();
    }

    bool(): boolean {
        return this.int32() !== 0;
    }

    uint64(): bigint {
        this.#expect(VARINT);
        return this.#reader.varintBig();
    }

    sint64(): bigint {
        const value = this.uint64();
        return (value >> 1n) ^ (-(value & 1n));
    }

    double(): number {
        this.#expect(FIXED_64);
        return this.#reader.double();
    }

    maybeSkip(): void {
        if (this.#wireType < 0) {
            return;
        } else if (this.#wireType === VARINT) {
            this.#reader.skipVarint();
        } else if (this.#wireType === FIXED_64) {
            this.#reader.skip(8);
        } else if (this.#wireType === LENGTH_DELIMITED) {
            const length = this.#reader.varint();
            this.#reader.skip(length);
        } else if (this.#wireType === FIXED_32) {
            this.#reader.skip(4);
        } else {
            throw new ProtoError(`Unexpected wire type ${this.#wireType}`);
        }
        this.#wireType = -1;
    }
}

export function readProtobufMessage<T>(data: Uint8Array, def: MessageDef<T>): T {
    const msgReader = new MessageReader(data);
    const fieldReader = new FieldReader(msgReader);

    let value = def.default();
    while (!msgReader.eof()) {
        const key = msgReader.varint();
        const tag = key >> 3;
        const wireType = key & 0x7;

        fieldReader.setup(wireType);
        const tagFun = def[tag];
        if (tagFun !== undefined) {
            const returnedValue = tagFun(fieldReader, value);
            if (returnedValue !== undefined) {
                value = returnedValue;
            }
        }
        fieldReader.maybeSkip();
    }
    return value;
}
