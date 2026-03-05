import type { ResolvePredicate, ResolveValue } from "@/types";

export type CompareOp = "gt" | "gte" | "lt" | "lte";

export function createComparePredicate(
    value: ResolveValue,
    op: CompareOp
): ResolvePredicate {
    if (typeof value === "number") {
        const right = value;
        if (op === "gt") {return (candidate) => typeof candidate === "number" && candidate > right;}
        if (op === "gte") {return (candidate) => typeof candidate === "number" && candidate >= right;}
        if (op === "lt") {return (candidate) => typeof candidate === "number" && candidate < right;}
        return (candidate) => typeof candidate === "number" && candidate <= right;
    }

    if (typeof value === "string") {
        const right = value;
        if (op === "gt") {return (candidate) => typeof candidate === "string" && candidate > right;}
        if (op === "gte") {return (candidate) => typeof candidate === "string" && candidate >= right;}
        if (op === "lt") {return (candidate) => typeof candidate === "string" && candidate < right;}
        return (candidate) => typeof candidate === "string" && candidate <= right;
    }

    if (typeof value === "bigint") {
        const right = value;
        if (op === "gt") {return (candidate) => typeof candidate === "bigint" && candidate > right;}
        if (op === "gte") {return (candidate) => typeof candidate === "bigint" && candidate >= right;}
        if (op === "lt") {return (candidate) => typeof candidate === "bigint" && candidate < right;}
        return (candidate) => typeof candidate === "bigint" && candidate <= right;
    }

    if (value instanceof Date) {
        const right = value.getTime();
        if (Number.isNaN(right)) {return () => false;}
        if (op === "gt") {
            return (candidate) => candidate instanceof Date && candidate.getTime() > right;
        }
        if (op === "gte") {
            return (candidate) => candidate instanceof Date && candidate.getTime() >= right;
        }
        if (op === "lt") {
            return (candidate) => candidate instanceof Date && candidate.getTime() < right;
        }
        return (candidate) => candidate instanceof Date && candidate.getTime() <= right;
    }

    return () => false;
}
