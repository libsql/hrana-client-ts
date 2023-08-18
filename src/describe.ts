import type * as proto from "./shared/proto.js";

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
        paramNames: result.params.map((p) => p.name),
        columns: result.cols,
        isExplain: result.isExplain,
        isReadonly: result.isReadonly,
    };
}
