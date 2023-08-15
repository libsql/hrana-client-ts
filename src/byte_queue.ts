export class ByteQueue {
    #array: Uint8Array;
    #shiftPos: number;
    #pushPos: number;

    constructor(initialCap: number) {
        this.#array = new Uint8Array(new ArrayBuffer(initialCap));
        this.#shiftPos = 0;
        this.#pushPos = 0;
    }

    get length(): number {
        return this.#pushPos - this.#shiftPos;
    }

    data(): Uint8Array {
        return this.#array.slice(this.#shiftPos, this.#pushPos);
    }

    push(chunk: Uint8Array): void {
        this.#ensurePush(chunk.byteLength);
        this.#array.set(chunk, this.#pushPos);
        this.#pushPos += chunk.byteLength;
    }

    #ensurePush(pushLength: number): void {
        if (this.#pushPos + pushLength <= this.#array.byteLength) {
            return;
        }

        const filledLength = this.#pushPos - this.#shiftPos;
        if (
            filledLength + pushLength <= this.#array.byteLength &&
            2*this.#pushPos >= this.#array.byteLength
        ) {
            this.#array.copyWithin(0, this.#shiftPos, this.#pushPos);
        } else {
            let newCap = this.#array.byteLength;
            do {
                newCap *= 2;
            } while (filledLength + pushLength > newCap);

            const newArray = new Uint8Array(new ArrayBuffer(newCap));
            newArray.set(this.#array.slice(this.#shiftPos, this.#pushPos), 0);
            this.#array = newArray;
        }

        this.#pushPos = filledLength;
        this.#shiftPos = 0;
    }

    shift(length: number): void {
        this.#shiftPos += length;
    }
}
