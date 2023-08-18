export type ObjectFun<T> = (w: ObjectWriter, value: T) => void;

export class ObjectWriter {
    #output: Array<string>;
    #isFirst: boolean;

    constructor(output: Array<string>) {
        this.#output = output;
        this.#isFirst = false;
    }

    begin(): void {
        this.#output.push('{');
        this.#isFirst = true;
    }

    end(): void {
        this.#output.push('}');
        this.#isFirst = false;
    }

    #key(name: string): void {
        if (this.#isFirst) {
            this.#output.push('"');
            this.#isFirst = false;
        } else {
            this.#output.push(',"');
        }
        this.#output.push(name);
        this.#output.push('":');
    }

    string(name: string, value: string): void {
        this.#key(name);
        this.#output.push(JSON.stringify(value));
    }

    stringRaw(name: string, value: string): void {
        this.#key(name);
        this.#output.push('"');
        this.#output.push(value);
        this.#output.push('"');
    }

    number(name: string, value: number): void {
        this.#key(name);
        this.#output.push(""+value);
    }

    boolean(name: string, value: boolean): void {
        this.#key(name);
        this.#output.push(value ? "true" : "false");
    }

    object<T>(name: string, value: T, valueFun: ObjectFun<T>): void {
        this.#key(name);

        this.begin();
        valueFun(this, value);
        this.end();
    }

    arrayObjects<T>(name: string, values: Array<T>, valueFun: ObjectFun<T>): void {
        this.#key(name);
        this.#output.push('[');

        for (let i = 0; i < values.length; ++i) {
            if (i !== 0) {
                this.#output.push(',');
            }
            this.begin();
            valueFun(this, values[i]);
            this.end();
        }

        this.#output.push(']');
    }
}

export function writeJsonObject<T>(value: T, fun: ObjectFun<T>): string {
    const output: Array<string> = [];
    const writer = new ObjectWriter(output);
    writer.begin();
    fun(writer, value);
    writer.end();
    return output.join("");
}
