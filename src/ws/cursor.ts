import { ClientError, ClosedError } from "../errors.js";
import { Cursor } from "../cursor.js";
import { Queue } from "../queue.js";

import type { WsClient } from "./client.js";
import type * as proto from "./proto.js";
import type { StreamState } from "./stream.js";

export interface CursorState {
    cursorId: number;
    closed: Error | undefined;
}

const fetchChunkSize = 1000;
const fetchQueueSize = 10;

export class WsCursor extends Cursor {
    #client: WsClient;
    #streamState: StreamState;
    #state: CursorState;

    #done: boolean;
    #entryQueue: Queue<proto.CursorEntry>;
    #fetchQueue: Queue<Promise<proto.FetchCursorResp | undefined>>;

    /** @private */
    constructor(client: WsClient, streamState: StreamState, state: CursorState) {
        super();
        this.#client = client;
        this.#streamState = streamState;
        this.#state = state;

        this.#done = false;
        this.#entryQueue = new Queue();
        this.#fetchQueue = new Queue();
    }

    /** Fetch the next entry from the cursor. */
    override async next(): Promise<proto.CursorEntry | undefined> {
        for (;;) {
            if (this.#state.closed !== undefined) {
                throw new ClosedError("Cursor is closed", this.#state.closed);
            }

            while (!this.#done && this.#fetchQueue.length < fetchQueueSize) {
                this.#fetchQueue.push(this.#fetch());
            }

            const entry = this.#entryQueue.shift();
            if (this.#done || entry !== undefined) {
                return entry;
            }

            // we assume that `Cursor.next()` is never called concurrently
            await this.#fetchQueue.shift()!.then(
                (response) => {
                    if (response === undefined) {
                        return;
                    }
                    for (const entry of response.entries) {
                        this.#entryQueue.push(entry);
                    }
                    this.#done ||= response.done;
                }
            );
        }
    }

    #fetch(): Promise<proto.FetchCursorResp | undefined> {
        return new Promise((responseCallback, errorCallback) => {
            const request: proto.FetchCursorReq = {
                type: "fetch_cursor",
                cursorId: this.#state.cursorId,
                maxCount: fetchChunkSize,
            };
            this.#client._sendStreamRequest(this.#streamState, request, {
                responseCallback: responseCallback as (_: proto.Response) => void,
                errorCallback: (error) => {
                    this.#client._closeCursor(this.#streamState, this.#state, error);
                    responseCallback(undefined);
                },
            });
        });
    }

    /** Close the cursor. */
    override close(): void {
        this.#client._closeCursor(
            this.#streamState, this.#state,
            new ClientError("Cursor was manually closed"),
        );
    }

    /** True if the cursor is closed. */
    override get closed(): boolean {
        return this.#state.closed !== undefined;
    }
}
