import type { Client, StreamState } from "./client.js";
import { ProtoError } from "./errors.js";
import { IdAlloc } from "./id_alloc.js";
import type * as proto from "./proto.js";
import type { RowsResult, RowResult, ValueResult, StmtResult } from "./result.js";
import {
    stmtResultFromProto, rowsResultFromProto,
    rowResultFromProto, valueResultFromProto,
    errorFromProto,
} from "./result.js";
import type { InStmt } from "./stmt.js";
import { stmtToProto } from "./stmt.js";
import type { Value, InValue } from "./value.js";
import { valueToProto, valueFromProto } from "./value.js";

/** A builder for creating a program and executing it on the server. */
export class Prog {
    #client: Client;
    #streamState: StreamState;

    /** @private */
    _steps: Array<proto.ProgStep>;
    /** @private */
    _resultCallbacks: Array<(_: proto.ProgResult) => void>;
    /** @private */
    _errorCallbacks: Array<(_: Error) => void>;

    /** @private */
    _executeCount: number;
    #outputCount: number;
    #varAlloc: IdAlloc;

    /** @private */
    constructor(client: Client, streamState: StreamState) {
        this.#client = client;
        this.#streamState = streamState;

        this._steps = [];
        this._resultCallbacks = [];
        this._errorCallbacks = [];

        this._executeCount = 0;
        this.#outputCount = 0;
        this.#varAlloc = new IdAlloc();
    }

    /** Return a builder for executing a statement in the program. */
    execute(): ProgExecute {
        return new ProgExecute(this);
    }

    /** Add an expression to the program. */
    output(expr: ProgExpr): Promise<Value> {
        const outputIdx = this.#outputCount++;
        this._steps.push({
            "type": "output",
            "expr": expr._proto,
        });

        return new Promise((valueCallback, errorCallback) => {
            this._resultCallbacks.push((result) => {
                valueCallback(valueFromProto(result["outputs"][outputIdx]));
            });
            this._errorCallbacks.push(errorCallback);
        });
    }

    /** Add a sequence of operations to the program. */
    ops(ops: Array<ProgOp>): void {
        this._steps.push({
            "type": "op",
            "ops": ops.map(op => op._proto),
        });
    }

    /** Add a single operation to the program. */
    op(op: ProgOp): void {
        this.ops([op]);
    }

    /** Allocate a fresh var. */
    allocVar(): ProgVar {
        return new ProgVar(this.#varAlloc.alloc());
    }

    /** Free a var allocated with `this.allocVar()`. */
    freeVar(var_: ProgVar): void {
        this.#varAlloc.free(var_._proto);
    }

    /** Run the program. */
    run(): Promise<void> {
        const promise = new Promise<void>((doneCallback, errorCallback) => {
            this._resultCallbacks.push((_result) => doneCallback(undefined));
            this._errorCallbacks.push(errorCallback);
        });

        const progState = {
            prog: {
                "steps": this._steps,
            },
            resultCallbacks: this._resultCallbacks,
            errorCallbacks: this._errorCallbacks,
        };
        this.#client._prog(this.#streamState, progState);

        return promise;
    }
}

/** A builder for adding a statement in a program. */
export class ProgExecute {
    #prog: Prog
    #condition: proto.ProgExpr | null;
    #onOk: Array<proto.ProgOp>;
    #onError: Array<proto.ProgOp>;

    /** @private */
    constructor(prog: Prog) {
        this.#prog = prog;
        this.#condition = null;
        this.#onOk = [];
        this.#onError = [];
    }

    /** Set the condition that needs to be satisfied to execute the statement. */
    condition(expr: ProgExpr): this {
        this.#condition = expr._proto;
        return this;
    }

    /** Add an operation to evaluate when the statement executed successfully. */
    onOk(op: ProgOp): this {
        this.#onOk.push(op._proto);
        return this;
    }

    /** Add an operation to evaluate when the statement failed to execute. */
    onError(op: ProgOp): this {
        this.#onError.push(op._proto);
        return this;
    }

    /** Execute a statement and return rows. */
    query(stmt: InStmt): Promise<RowsResult | undefined> {
        return this.#execute(stmtToProto(stmt, true), rowsResultFromProto);
    }

    /** Execute a statement and return at most a single row. */
    queryRow(stmt: InStmt): Promise<RowResult | undefined> {
        return this.#execute(stmtToProto(stmt, true), rowResultFromProto);
    }

    /** Execute a statement and return at most a single value. */
    queryValue(stmt: InStmt): Promise<ValueResult | undefined> {
        return this.#execute(stmtToProto(stmt, true), valueResultFromProto);
    }

    /** Execute a statement without returning rows. */
    run(stmt: InStmt): Promise<StmtResult | undefined> {
        return this.#execute(stmtToProto(stmt, false), stmtResultFromProto);
    }

    #execute<T>(stmt: proto.Stmt, fromProto: (result: proto.StmtResult) => T): Promise<T | undefined> {
        const executeIdx = this.#prog._executeCount++;
        this.#prog._steps.push({
            "type": "execute",
            "stmt": stmt,
            "condition": this.#condition,
            "on_ok": this.#onOk,
            "on_error": this.#onError,
        });

        return new Promise((outputCallback, errorCallback) => {
            this.#prog._resultCallbacks.push((result) => {
                const executeResult = result["execute_results"][executeIdx];
                const executeError = result["execute_errors"][executeIdx];
                if (executeResult === undefined || executeError === undefined) {
                    errorCallback(new ProtoError("Server returned fewer results or errors than expected"));
                } else if (executeResult !== null && executeError !== null) {
                    errorCallback(new ProtoError("Server returned both result and error"));
                } else if (executeError !== null) {
                    errorCallback(errorFromProto(executeError));
                } else if (executeResult !== null) {
                    outputCallback(fromProto(executeResult));
                } else {
                    outputCallback(undefined);
                }
            });
            this.#prog._errorCallbacks.push(errorCallback);
        });
    }
}

export class ProgOp {
    /** @private */
    _proto: proto.ProgOp;

    /** @private */
    constructor(proto: proto.ProgOp) {
        this._proto = proto;
    }

    static set(var_: ProgVar, expr: ProgExpr): ProgOp {
        return new ProgOp({"type": "set", "var": var_._proto, "expr": expr._proto});
    }
}

export class ProgExpr {
    /** @private */
    _proto: proto.ProgExpr;

    /** @private */
    constructor(proto: proto.ProgExpr) {
        this._proto = proto;
    }

    static value(value: InValue): ProgExpr {
        return new ProgExpr(valueToProto(value));
    }

    static var_(var_: ProgVar): ProgExpr {
        return new ProgExpr({"type": "var", "var": var_._proto});
    }

    static not(expr: ProgExpr): ProgExpr {
        return new ProgExpr({"type": "not", "expr": expr._proto});
    }

    static and(exprs: Array<ProgExpr>): ProgExpr {
        return new ProgExpr({"type": "and", "exprs": exprs.map(e => e._proto)});
    }

    static or(exprs: Array<ProgExpr>): ProgExpr {
        return new ProgExpr({"type": "or", "exprs": exprs.map(e => e._proto)});
    }
}

export class ProgVar {
    /** @private */
    _proto: number;

    /** @private */
    constructor(proto: number) {
        this._proto = proto;
    }
}
