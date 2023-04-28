import type * as proto from "./proto.js";

export interface DescribeResult {
    paramNames: Array<string | undefined>;
    columns: Array<DescribeColumn>;
    isExplain: boolean;
    isReadonly: boolean;
}

export interface DescribeColumn {
    name: string;
    decltype: string | undefined;
}

export function describeResultFromProto(result: proto.DescribeResult): DescribeResult {
    return {
        paramNames: result["params"].map((p) => p.name ?? undefined),
        columns: result["cols"].map((c) => {
            return {
                name: c["name"],
                decltype: c["decltype"] ?? undefined,
            };
        }),
        isExplain: result["is_explain"],
        isReadonly: result["is_readonly"],
    };
}
