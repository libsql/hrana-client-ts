import { ProtoError } from "./errors.js";
import { IdAlloc } from "./id_alloc.js";
import type { RowsResult, RowResult, ValueResult, StmtResult } from "./result.js";
import {
    stmtResultFromProto, rowsResultFromProto,
    rowResultFromProto, valueResultFromProto,
    errorFromProto,
} from "./result.js";
import type * as proto from "./shared/proto.js";
import type { InStmt } from "./stmt.js";
import { stmtToProto } from "./stmt.js";
import { Stream } from "./stream.js";
import type { Value, InValue, IntMode } from "./value.js";
import { valueToProto, valueFromProto } from "./value.js";

/** A builder for creating a batch and executing it on the server. */
export class Batch {
    /** @private */
    _stream: Stream;

    #executed: boolean;
    /** @private */
    _steps: Array<proto.BatchStep>;
    /** @private */
    _resultCallbacks: Array<(_: proto.BatchResult) => void>;

    /** @private */
    constructor(stream: Stream) {
        this._stream = stream;
        this.#executed = false;

        this._steps = [];
        this._resultCallbacks = [];
    }

    /** Return a builder for adding a step to the batch. */
    step(): BatchStep {
        return new BatchStep(this);
    }

    /** Execute the batch. */
    execute(): Promise<void> {
        if (this.#executed) {
            throw new Error("This batch has already been executed");
        }
        this.#executed = true;

        const batch: proto.Batch = {
            steps: this._steps,
        };
        return this._stream._batch(batch).then((result) => {
            for (const callback of this._resultCallbacks) {
                callback(result);
            }
        });
    }
}

/** A builder for adding a step to the batch. */
export class BatchStep {
    #batch: Batch;
    #conditions: Array<proto.BatchCond>;
    /** @private */
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
        return this.#add(stmt, true, rowsResultFromProto);
    }

    /** Add a statement that returns at most a single row. */
    queryRow(stmt: InStmt): Promise<RowResult | undefined> {
        return this.#add(stmt, true, rowResultFromProto);
    }

    /** Add a statement returns at most a single value. */
    queryValue(stmt: InStmt): Promise<ValueResult | undefined> {
        return this.#add(stmt, true, valueResultFromProto);
    }

    /** Add a statement without returning rows. */
    run(stmt: InStmt): Promise<StmtResult | undefined> {
        return this.#add(stmt, false, stmtResultFromProto);
    }

    #add<T>(
        inStmt: InStmt,
        wantRows: boolean,
        fromProto: (result: proto.StmtResult, intMode: IntMode) => T,
    ): Promise<T | undefined> {
        const stmt = stmtToProto(this.#batch._stream._sqlOwner(), inStmt, wantRows);

        if (this._index !== undefined) {
            throw new Error("This step has already been added to the batch");
        }
        const index = this.#batch._steps.length;
        this._index = index;

        let condition: proto.BatchCond | undefined;
        if (this.#conditions.length === 0) {
            condition = undefined;
        } else if (this.#conditions.length === 1) {
            condition = this.#conditions[0];
        } else {
            condition = {type: "and", conds: this.#conditions};
        }

        this.#batch._steps.push({stmt, condition});

        return new Promise((outputCallback, errorCallback) => {
            this.#batch._resultCallbacks.push((result) => {
                const stepResult = result.stepResults.get(index);
                const stepError = result.stepErrors.get(index);
                if (stepResult !== undefined && stepError !== undefined) {
                    errorCallback(new ProtoError("Server returned both result and error"));
                } else if (stepError !== undefined) {
                    errorCallback(errorFromProto(stepError));
                } else if (stepResult !== undefined) {
                    outputCallback(fromProto(stepResult, this.#batch._stream.intMode));
                } else {
                    outputCallback(undefined);
                }
            });
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
        return new BatchCond({type: "ok", step: stepIndex(step)});
    }

    static error(step: BatchStep): BatchCond {
        return new BatchCond({type: "error", step: stepIndex(step)});
    }

    static not(cond: BatchCond): BatchCond {
        return new BatchCond({type: "not", cond: cond._proto});
    }

    static and(conds: Array<BatchCond>): BatchCond {
        return new BatchCond({type: "and", conds: conds.map(e => e._proto)});
    }

    static or(conds: Array<BatchCond>): BatchCond {
        return new BatchCond({type: "or", conds: conds.map(e => e._proto)});
    }
}

function stepIndex(step: BatchStep): number {
    if (step._index === undefined) {
        throw new Error("Cannot add a condition referencing a step that has not been added to the batch");
    }
    return step._index;
}
