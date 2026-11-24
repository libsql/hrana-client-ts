import { readProtobufMessage } from "../encoding/protobuf/decode.js";
import { CursorEntry } from "../shared/protobuf_decode.js";

describe("CursorEntry protobuf decoding", () => {
    // CursorEntry { step_end = 2: StepEndEntry { affected_row_count = 1, last_insert_rowid = 2 } }.
    // The spec encodes last_insert_rowid as sint64 (zigzag).
    function stepEnd(lastInsertRowidZigzag: number[]): Uint8Array {
        const body = [0x08, 0x01, 0x10, ...lastInsertRowidZigzag];
        return new Uint8Array([0x12, body.length, ...body]);
    }

    test("StepEndEntry.last_insert_rowid is sint64", () => {
        expect(readProtobufMessage(stepEnd([0x0a]), CursorEntry)).toStrictEqual({
            type: "step_end",
            affectedRowCount: 1,
            lastInsertRowid: 5n,
        });
        expect(readProtobufMessage(stepEnd([0x01]), CursorEntry)).toStrictEqual({
            type: "step_end",
            affectedRowCount: 1,
            lastInsertRowid: -1n,
        });
    });
});
