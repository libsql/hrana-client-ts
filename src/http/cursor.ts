import { ByteQueue } from "../byte_queue.js";
import type { ProtocolEncoding } from "../client.js";
import { Cursor } from "../cursor.js";
import * as jsond from "../encoding/json/decode.js";
import * as protobufd from "../encoding/protobuf/decode.js";
import { ClientError, ClosedError, ProtoError, InternalError } from "../errors.js";
import { impossible } from "../util.js";

import type * as proto from "./proto.js";
import type { HttpStream } from "./stream.js";

import { CursorRespBody as json_CursorRespBody } from "./json_decode.js";
import { CursorRespBody as protobuf_CursorRespBody } from "./protobuf_decode.js";
import { CursorEntry as json_CursorEntry } from "../shared/json_decode.js";
import { CursorEntry as protobuf_CursorEntry } from "../shared/protobuf_decode.js";

export class HttpCursor extends Cursor {
    #stream: HttpStream;
    #encoding: ProtocolEncoding;

    #reader: any | undefined;
    #queue: ByteQueue;
    #closed: Error | undefined;
    #done: boolean;

    /** @private */
    constructor(stream: HttpStream, encoding: ProtocolEncoding) {
        super();
        this.#stream = stream;
        this.#encoding = encoding;

        this.#reader = undefined;
        this.#queue = new ByteQueue(16 * 1024);
        this.#closed = undefined;
        this.#done = false;
    }

    async open(response: Response): Promise<proto.CursorRespBody> {
        if (response.body === null) {
            throw new ProtoError("No response body for cursor request");
        }

        // node-fetch do not fully support WebStream API, especially getReader() function
        // see https://github.com/node-fetch/node-fetch/issues/387
        // so, we are using async iterator which behaves similarly here instead
        this.#reader = (response.body as any)[Symbol.asyncIterator]();
        const respBody = await this.#nextItem(json_CursorRespBody, protobuf_CursorRespBody);
        if (respBody === undefined) {
            throw new ProtoError("Empty response to cursor request");
        }
        return respBody;
    }

    /** Fetch the next entry from the cursor. */
    override next(): Promise<proto.CursorEntry | undefined> {
        return this.#nextItem(json_CursorEntry, protobuf_CursorEntry);
    }

    /** Close the cursor. */
    override close(): void {
        this._setClosed(new ClientError("Cursor was manually closed"));
    }

    /** @private */
    _setClosed(error: Error): void {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;
        this.#stream._cursorClosed(this);

        if (this.#reader !== undefined) {
            this.#reader.return();
        }
    }

    /** True if the cursor is closed. */
    override get closed(): boolean {
        return this.#closed !== undefined;
    }

    async #nextItem<T>(jsonFun: jsond.ObjectFun<T>, protobufDef: protobufd.MessageDef<T>): Promise<T | undefined> {
        for (; ;) {
            if (this.#done) {
                return undefined;
            } else if (this.#closed !== undefined) {
                throw new ClosedError("Cursor is closed", this.#closed);
            }

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

            if (this.#reader === undefined) {
                throw new InternalError("Attempted to read from HTTP cursor before it was opened");
            }

            const { value, done } = await this.#reader.next();
            if (done && this.#queue.length === 0) {
                this.#done = true;
            } else if (done) {
                throw new ProtoError("Unexpected end of cursor stream");
            } else {
                this.#queue.push(value);
            }
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
        for (; ;) {
            if (varintLength >= data.byteLength) {
                return undefined;
            }
            const byte = data[varintLength];
            varintValue |= (byte & 0x7f) << (7 * varintLength);
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
}
