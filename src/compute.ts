import type * as proto from "./proto.js";
import type { InValue } from "./value.js";

import { valueToProto } from "./value.js";

export class Op {
    /** @private */
    _proto: proto.ComputeOp;

    /** @private */
    constructor(proto: proto.ComputeOp) {
        this._proto = proto;
    }

    static set(var_: Var, expr: Expr): Op {
        return new Op({"type": "set", "var": var_._proto, "expr": expr._proto});
    }

    static unset(var_: Var): Op {
        return new Op({"type": "unset", "var": var_._proto});
    }

    static eval(expr: Expr): Op {
        return new Op({"type": "eval", "expr": expr._proto});
    }
}

export class Expr {
    /** @private */
    _proto: proto.ComputeExpr;

    /** @private */
    constructor(proto: proto.ComputeExpr) {
        this._proto = proto;
    }

    static value(value: InValue): Expr {
        return new Expr(valueToProto(value));
    }

    static var_(var_: Var): Expr {
        return new Expr({"type": "var", "var": var_._proto});
    }

    static not(expr: Expr): Expr {
        return new Expr({"type": "not", "expr": expr._proto});
    }
}

export class Var {
    /** @private */
    _proto: number;

    /** @private */
    constructor(proto: number) {
        this._proto = proto;
    }
}
