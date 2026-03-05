import type { CompareOp } from "@/core/engine/predicates/compare";

export type PushdownOp = "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "between" | "in" | "notIn" | "contains" | "startsWith" | "endsWith" | "matches" | "isNull" | "notNull";

export type PushdownPredicate = {
    field: string;
    op: PushdownOp;
    value?: unknown;
    ignoreCase?: boolean;
};

export type PushdownOrder = {
    direction: "asc" | "desc";
    field: string;
    nulls?: "first" | "last";
};

export type PushdownQuery = {
    fields?: Array<string>;
    limit?: number;
    offset?: number;
    orders?: Array<PushdownOrder>;
    predicates?: Array<PushdownPredicate>;
};

export function toCompareOp(op: CompareOp): PushdownOp {
    if (op === "gt") {return "gt";}
    if (op === "gte") {return "gte";}
    if (op === "lt") {return "lt";}
    return "lte";
}
