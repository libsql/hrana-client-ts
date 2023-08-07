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
    /** @private */
    _batch: Batch;
    #conds: Array<proto.BatchCond>;
    /** @private */
    _index: number | undefined;

    /** @private */
    constructor(batch: Batch) {
        this._batch = batch;
        this.#conds = [];
        this._index = undefined;
    }

    /** Add the condition that needs to be satisfied to execute the statement. If you use this method multiple
    * times, we join the conditions with a logical AND. */
    condition(cond: BatchCond): this {
        this.#conds.push(cond._proto);
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

    /** Add a statement that returns at most a single value. */
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
        const stmt = stmtToProto(this._batch._stream._sqlOwner(), inStmt, wantRows);

        if (this._index !== undefined) {
            throw new Error("This step has already been added to the batch");
        }
        const index = this._batch._steps.length;
        this._index = index;

        let condition: proto.BatchCond | undefined;
        if (this.#conds.length === 0) {
            condition = undefined;
        } else if (this.#conds.length === 1) {
            condition = this.#conds[0];
        } else {
            condition = {type: "and", conds: this.#conds};
        }

        this._batch._steps.push({stmt, condition});

        return new Promise((outputCallback, errorCallback) => {
            this._batch._resultCallbacks.push((result) => {
                const stepResult = result.stepResults.get(index);
                const stepError = result.stepErrors.get(index);
                if (stepResult !== undefined && stepError !== undefined) {
                    errorCallback(new ProtoError("Server returned both result and error"));
                } else if (stepError !== undefined) {
                    errorCallback(errorFromProto(stepError));
                } else if (stepResult !== undefined) {
                    outputCallback(fromProto(stepResult, this._batch._stream.intMode));
                } else {
                    outputCallback(undefined);
                }
            });
        });
    }
}

export class BatchCond {
    /** @private */
    _batch: Batch;
    /** @private */
    _proto: proto.BatchCond;

    /** @private */
    constructor(batch: Batch, proto: proto.BatchCond) {
        this._batch = batch;
        this._proto = proto;
    }

    /** Create a condition that evaluates to true when the given step executes successfully.
     *
     * If the given step fails error or is skipped because its condition evaluated to false, this
     * condition evaluates to false.
     */
    static ok(step: BatchStep): BatchCond {
        return new BatchCond(step._batch, {type: "ok", step: stepIndex(step)});
    }

    /** Create a condition that evaluates to true when the given step fails.
     *
     * If the given step succeeds or is skipped because its condition evaluated to false, this condition
     * evaluates to false.
     */
    static error(step: BatchStep): BatchCond {
        return new BatchCond(step._batch, {type: "error", step: stepIndex(step)});
    }

    /** Create a condition that is a logical negation of another condition.
     */
    static not(cond: BatchCond): BatchCond {
        return new BatchCond(cond._batch, {type: "not", cond: cond._proto});
    }

    /** Create a condition that is a logical AND of other conditions. 
     */
    static and(batch: Batch, conds: Array<BatchCond>): BatchCond {
        for (const cond of conds) {
            checkCondBatch(batch, cond);
        }
        return new BatchCond(batch, {type: "and", conds: conds.map(e => e._proto)});
    }

    /** Create a condition that is a logical OR of other conditions. 
     */
    static or(batch: Batch, conds: Array<BatchCond>): BatchCond {
        for (const cond of conds) {
            checkCondBatch(batch, cond);
        }
        return new BatchCond(batch, {type: "or", conds: conds.map(e => e._proto)});
    }

    /** Create a condition that evaluates to true when the SQL connection is in autocommit mode (not inside an
     * explicit transaction). This requires protocol version 3 or higher.
     */
    static isAutocommit(batch: Batch): BatchCond {
        batch._stream.client()._ensureVersion(3, "BatchCond.isAutocommit()");
        return new BatchCond(batch, {type: "is_autocommit"});
    }
}

function stepIndex(step: BatchStep): number {
    if (step._index === undefined) {
        throw new Error("Cannot add a condition referencing a step that has not been added to the batch");
    }
    return step._index;
}

function checkCondBatch(expectedBatch: Batch, cond: BatchCond): void {
    if (cond._batch !== expectedBatch) {
        throw new Error("Cannot mix BatchCond objects for different Batch objects");
    }
}
