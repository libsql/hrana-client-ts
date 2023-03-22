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

/** A builder for creating a batch and executing it on the server. */
export class Batch {
    #client: Client;
    #streamState: StreamState;

    /** @private */
    _steps: Array<proto.BatchStep>;
    /** @private */
    _resultCallbacks: Array<(_: proto.BatchResult) => void>;
    /** @private */
    _errorCallbacks: Array<(_: Error) => void>;

    /** @private */
    constructor(client: Client, streamState: StreamState) {
        this.#client = client;
        this.#streamState = streamState;

        this._steps = [];
        this._resultCallbacks = [];
        this._errorCallbacks = [];
    }

    /** Return a builder for adding a step to the batch. */
    step(): BatchStep {
        return new BatchStep(this);
    }

    /** Execute the batch. */
    execute(): Promise<void> {
        const promise = new Promise<void>((doneCallback, errorCallback) => {
            this._resultCallbacks.push((_result) => doneCallback(undefined));
            this._errorCallbacks.push(errorCallback);
        });

        const batchState = {
            batch: {
                "steps": this._steps,
            },
            resultCallbacks: this._resultCallbacks,
            errorCallbacks: this._errorCallbacks,
        };
        this.#client._batch(this.#streamState, batchState);

        return promise;
    }
}

/** A builder for adding a step to the batch. */
export class BatchStep {
    #batch: Batch;
    #conditions: Array<proto.BatchCond>;
    _index: number | undefined;

    /** @private */
    constructor(batch: Batch) {
        this.#batch = batch;
        this.#conditions = [];
        this._index = undefined;
    }

    /** Add the condition that needs to be satisfied to execute the statement. If you use this method multiple
    * times, we join them with a logical AND. */
    condition(cond: BatchCond): this {
        this.#conditions.push(cond._proto);
        return this;
    }

    /** Add a statement that returns rows. */
    query(stmt: InStmt): Promise<RowsResult | undefined> {
        return this.#add(stmtToProto(stmt, true), rowsResultFromProto);
    }

    /** Add a statement that returns at most a single row. */
    queryRow(stmt: InStmt): Promise<RowResult | undefined> {
        return this.#add(stmtToProto(stmt, true), rowResultFromProto);
    }

    /** Add a statement returns at most a single value. */
    queryValue(stmt: InStmt): Promise<ValueResult | undefined> {
        return this.#add(stmtToProto(stmt, true), valueResultFromProto);
    }

    /** Add a statement without returning rows. */
    run(stmt: InStmt): Promise<StmtResult | undefined> {
        return this.#add(stmtToProto(stmt, false), stmtResultFromProto);
    }

    #add<T>(stmt: proto.Stmt, fromProto: (result: proto.StmtResult) => T): Promise<T | undefined> {
        if (this._index !== undefined) {
            throw new Error("This step has already been added to the batch");
        }
        const index = this.#batch._steps.length;
        this._index = index;

        let condition: proto.BatchCond | null;
        if (this.#conditions.length === 0) {
            condition = null;
        } else if (this.#conditions.length === 1) {
            condition = this.#conditions[0];
        } else {
            condition = {"type": "and", "conds": this.#conditions};
        }

        this.#batch._steps.push({
            "stmt": stmt,
            "condition": condition,
        });

        return new Promise((outputCallback, errorCallback) => {
            this.#batch._resultCallbacks.push((result) => {
                const stepResult = result["step_results"][index];
                const stepError = result["step_errors"][index];
                if (stepResult === undefined || stepError === undefined) {
                    errorCallback(new ProtoError("Server returned fewer step results than expected"));
                } else if (stepResult !== null && stepError !== null) {
                    errorCallback(new ProtoError("Server returned both result and error"));
                } else if (stepError !== null) {
                    errorCallback(errorFromProto(stepError));
                } else if (stepResult !== null) {
                    outputCallback(fromProto(stepResult));
                } else {
                    outputCallback(undefined);
                }
            });
            this.#batch._errorCallbacks.push(errorCallback);
        });
    }
}

export class BatchCond {
    /** @private */
    _proto: proto.BatchCond;

    /** @private */
    constructor(proto: proto.BatchCond) {
        this._proto = proto;
    }

    static ok(step: BatchStep): BatchCond {
        return new BatchCond({"type": "ok", "step": stepIndex(step)});
    }

    static error(step: BatchStep): BatchCond {
        return new BatchCond({"type": "error", "step": stepIndex(step)});
    }

    static not(cond: BatchCond): BatchCond {
        return new BatchCond({"type": "not", "cond": cond._proto});
    }

    static and(conds: Array<BatchCond>): BatchCond {
        return new BatchCond({"type": "and", "conds": conds.map(e => e._proto)});
    }

    static or(conds: Array<BatchCond>): BatchCond {
        return new BatchCond({"type": "or", "conds": conds.map(e => e._proto)});
    }
}

function stepIndex(step: BatchStep): number {
    if (step._index === undefined) {
        throw new Error("Cannot add a condition referencing a step that has not been added to the batch");
    }
    return step._index;
}
