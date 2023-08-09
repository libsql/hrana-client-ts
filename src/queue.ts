export class Queue<T> {
    #pushStack: Array<T>;
    #popStack: Array<T>;

    constructor() {
        this.#pushStack = [];
        this.#popStack = [];
    }

    get length(): number {
        return this.#pushStack.length + this.#popStack.length;
    }

    push(elem: T): void {
        this.#pushStack.push(elem);
    }

    shift(): T | undefined {
        if (this.#popStack.length === 0 && this.#pushStack.length > 0) {
            this.#popStack = this.#pushStack.reverse();
            this.#pushStack = [];
        }
        return this.#popStack.pop();
    }
}
