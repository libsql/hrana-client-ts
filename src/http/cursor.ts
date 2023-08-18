import type { Response, ReadableStreamDefaultReader } from "@libsql/isomorphic-fetch";

import { ByteQueue } from "../byte_queue.js";
import type { ProtocolEncoding } from "../client.js";
import { Cursor } from "../cursor.js";
import * as jsond from "../encoding/json/decode.js";
import * as protobufd from "../encoding/protobuf/decode.js";
import { ProtoError } from "../errors.js";
import { impossible } from "../util.js";

import type * as proto from "./proto.js";

import { CursorRespBody as json_CursorRespBody } from "./json_decode.js";
import { CursorRespBody as protobuf_CursorRespBody } from "./protobuf_decode.js";
import { CursorEntry as json_CursorEntry } from "../shared/json_decode.js";
import { CursorEntry as protobuf_CursorEntry } from "../shared/protobuf_decode.js";

export class HttpCursor extends Cursor {
    #reader: ReadableStreamDefaultReader<Uint8Array>;
    #encoding: ProtocolEncoding;
    #queue: ByteQueue;

    closed: boolean;

    /** @private */
    constructor(reader: ReadableStreamDefaultReader, encoding: ProtocolEncoding) {
        super();
        this.#reader = reader;
        this.#encoding = encoding;
        this.#queue = new ByteQueue(16 * 1024);
        this.closed = false;
    }

    async #nextItem<T>(jsonFun: jsond.ObjectFun<T>, protobufDef: protobufd.MessageDef<T>): Promise<T | undefined> {
        for (;;) {
            if (this.#encoding === "json") {
                const jsonData = this.#parseItemJson();
                if (jsonData !== undefined) {
                    const jsonText = new TextDecoder().decode(jsonData);
                    const jsonValue = JSON.parse(jsonText);
                    return jsond.readJsonObject(jsonValue, jsonFun);
                }
            } else if (this.#encoding === "protobuf") {
                const protobufData = this.#parseItemProtobuf();
                if (protobufData !== undefined) {
                    return protobufd.readProtobufMessage(protobufData, protobufDef);
                }
            } else {
                throw impossible(this.#encoding, "Impossible encoding");
            }

            const {value, done} = await this.#reader.read();
            if (done && this.#queue.length === 0) {
                this.closed = true;
                return undefined;
            } else if (done) {
                throw new ProtoError("Unexpected end of cursor stream");
            }
            this.#queue.push(value);
        }
    }

    #parseItemJson(): Uint8Array | undefined {
        const data = this.#queue.data();
        const newlineByte = 10;
        const newlinePos = data.indexOf(newlineByte);
        if (newlinePos < 0) {
            return undefined;
        }

        const jsonData = data.slice(0, newlinePos);
        this.#queue.shift(newlinePos + 1);
        return jsonData;
    }

    #parseItemProtobuf(): Uint8Array | undefined {
        const data = this.#queue.data();

        let varintValue = 0;
        let varintLength = 0;
        for (;;) {
            if (varintLength >= data.byteLength) {
                return undefined;
            }
            const byte = data[varintLength];
            varintValue |= (byte & 0x7f) << (7*varintLength);
            varintLength += 1;
            if (!(byte & 0x80)) {
                break;
            }
        }

        if (data.byteLength < varintLength + varintValue) {
            return undefined;
        }

        const protobufData = data.slice(varintLength, varintLength + varintValue);
        this.#queue.shift(varintLength + varintValue);
        return protobufData;
    }

    static async open(
        response: Response,
        encoding: ProtocolEncoding,
    ): Promise<[HttpCursor, proto.CursorRespBody]> {
        if (response.body === null) {
            throw new ProtoError("No response body for cursor request");
        }
        const cursor = new HttpCursor(response.body.getReader(), encoding);
        const respBody = await cursor.#nextItem(json_CursorRespBody, protobuf_CursorRespBody);
        if (respBody === undefined) {
            throw new ProtoError("Empty response to cursor request");
        }
        return [cursor, respBody];
    }

    /** Fetch the next entry from the cursor. */
    override next(): Promise<proto.CursorEntry | undefined> {
        return this.#nextItem(json_CursorEntry, protobuf_CursorEntry);
    }

    /** Close the cursor. */
    override close(): void {
        this.closed = true;
        this.#reader.cancel();
    }
}
