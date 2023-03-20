import type * as proto from "./proto.js";
import type { InValue } from "./value.js";

import { valueToProto } from "./value.js";

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
}

export class ProgVar {
    /** @private */
    _proto: number;

    /** @private */
    constructor(proto: number) {
        this._proto = proto;
    }
}
