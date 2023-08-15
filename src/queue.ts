export class Queue<T> {
    #pushStack: Array<T>;
    #shiftStack: Array<T>;

    constructor() {
        this.#pushStack = [];
        this.#shiftStack = [];
    }

    get length(): number {
        return this.#pushStack.length + this.#shiftStack.length;
    }

    push(elem: T): void {
        this.#pushStack.push(elem);
    }

    shift(): T | undefined {
        if (this.#shiftStack.length === 0 && this.#pushStack.length > 0) {
            this.#shiftStack = this.#pushStack.reverse();
            this.#pushStack = [];
        }
        return this.#shiftStack.pop();
    }

    first(): T | undefined {
        return this.#shiftStack.length !== 0
            ? this.#shiftStack[this.#shiftStack.length - 1]
            : this.#pushStack[0];
    }
}
