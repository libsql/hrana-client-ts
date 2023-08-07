import { ProtoError } from "../errors.js";
import * as d from "../encoding/json/decode.js";
import { Error, StmtResult, BatchResult, CursorEntry, DescribeResult } from "../shared/json_decode.js";
import * as proto from "./proto.js";


export function PipelineResponseBody(obj: d.Obj): proto.PipelineResponseBody {
    const baton = d.stringOpt(obj["baton"]);
    const baseUrl = d.stringOpt(obj["base_url"]);
    const results = d.arrayObjectsMap(obj["results"], StreamResult);
    return {baton, baseUrl, results};
}

function StreamResult(obj: d.Obj): proto.StreamResult {
    const type = d.string(obj["type"]);
    if (type === "ok") {
        const response = StreamResponse(d.object(obj["response"]));
        return {type: "ok", response};
    } else if (type === "error") {
        const error = Error(d.object(obj["error"]));
        return {type: "error", error};
    } else {
        throw new ProtoError("Unexpected type of StreamResult");
    }
}

function StreamResponse(obj: d.Obj): proto.StreamResponse {
    const type = d.string(obj["type"]);
    if (type === "close") {
        return {type: "close"};
    } else if (type === "execute") {
        const result = StmtResult(d.object(obj["result"]));
        return {type: "execute", result};
    } else if (type === "batch") {
        const result = BatchResult(d.object(obj["result"]));
        return {type: "batch", result};
    } else if (type === "sequence") {
        return {type: "sequence"};
    } else if (type === "describe") {
        const result = DescribeResult(d.object(obj["result"]));
        return {type: "describe", result};
    } else if (type === "store_sql") {
        return {type: "store_sql"};
    } else if (type === "close_sql") {
        return {type: "close_sql"};
    } else if (type === "get_autocommit") {
        const isAutocommit = d.boolean(obj["is_autocommit"]);
        return {type: "get_autocommit", isAutocommit};
    } else {
        throw new ProtoError("Unexpected type of StreamResponse");
    }
}
