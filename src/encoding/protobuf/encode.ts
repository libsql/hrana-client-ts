import type { WireType } from "./util.js";
import { VARINT, FIXED_64, LENGTH_DELIMITED } from "./util.js";

export type MessageFun<T> = (w: MessageWriter, msg: T) => void;

export class MessageWriter {
    #buf: ArrayBuffer;
    #array: Uint8Array;
    #view: DataView;
    #pos: number;

    constructor() {
        this.#buf = new ArrayBuffer(256);
        this.#array = new Uint8Array(this.#buf);
        this.#view = new DataView(this.#buf);
        this.#pos = 0;
    }

    #ensure(extra: number) {
        if (this.#pos + extra <= this.#buf.byteLength) {
            return;
        }

        let newCap = this.#buf.byteLength;
        while (newCap < this.#pos + extra) {
            newCap *= 2;
        }

        const newBuf = new ArrayBuffer(newCap);
        const newArray = new Uint8Array(newBuf);
        const newView = new DataView(newBuf);
        newArray.set(new Uint8Array(this.#buf, 0, this.#pos));

        this.#buf = newBuf;
        this.#array = newArray;
        this.#view = newView;
    }

    #varint(value: number): void {
        this.#ensure(5);

        value = 0|value;
        do {
            let byte = value & 0x7f;
            value >>>= 7;
            byte |= (value ? 0x80 : 0);
            this.#array[this.#pos++] = byte;
        } while (value);
    }

    #varintBig(value: bigint): void {
        this.#ensure(10);

        value = value & 0xffffffffffffffffn;
        do {
            let byte = Number(value & 0x7fn);
            value >>= 7n;
            byte |= (value ? 0x80 : 0);
            this.#array[this.#pos++] = byte;
        } while (value);
    }

    #tag(tag: number, wireType: WireType): void {
        this.#varint((tag << 3) | wireType);
    }

    bytes(tag: number, value: Uint8Array): void {
        this.#tag(tag, LENGTH_DELIMITED);
        this.#varint(value.byteLength);
        this.#ensure(value.byteLength);
        this.#array.set(value, this.#pos);
        this.#pos += value.byteLength;
    }

    string(tag: number, value: string): void {
        this.bytes(tag, new TextEncoder().encode(value));
    }

    message<T>(tag: number, value: T, fun: MessageFun<T>): void {
        const writer = new MessageWriter();
        fun(writer, value);
        this.bytes(tag, writer.data());
    }

    int32(tag: number, value: number): void {
        this.#tag(tag, VARINT);
        this.#varint(value);
    }

    uint32(tag: number, value: number): void {
        this.int32(tag, value);
    }

    bool(tag: number, value: boolean): void {
        this.int32(tag, value ? 1 : 0);
    }

    sint64(tag: number, value: bigint): void {
        this.#tag(tag, VARINT);
        this.#varintBig((value << 1n) ^ (value >> 63n));
    }

    double(tag: number, value: number): void {
        this.#tag(tag, FIXED_64);
        this.#ensure(8);
        this.#view.setFloat64(this.#pos, value, true);
        this.#pos += 8;
    }

    data(): Uint8Array {
        return new Uint8Array(this.#buf, 0, this.#pos);
    }
}

export function writeProtobufMessage<T>(value: T, fun: MessageFun<T>): Uint8Array {
    const w = new MessageWriter();
    fun(w, value);
    return w.data();
}
