import type { ResolvePredicate, ResolveValue } from "@/types";

export function createBetweenPredicate(min: ResolveValue, max: ResolveValue): ResolvePredicate {
    if (typeof min !== typeof max) {return () => false;}

    if (typeof min === "number") {
        const minValue = min;
        const maxValue = max as number;
        if (Number.isNaN(minValue) || Number.isNaN(maxValue)) {return () => false;}
        return (candidate) =>
            typeof candidate === "number" && candidate >= minValue && candidate <= maxValue;
    }

    if (typeof min === "string") {
        const minValue = min;
        const maxValue = max as string;
        return (candidate) =>
            typeof candidate === "string" && candidate >= minValue && candidate <= maxValue;
    }

    if (typeof min === "bigint") {
        const minValue = min;
        const maxValue = max as bigint;
        return (candidate) =>
            typeof candidate === "bigint" && candidate >= minValue && candidate <= maxValue;
    }

    if (min instanceof Date && max instanceof Date) {
        const minValue = min.getTime();
        const maxValue = max.getTime();
        if (Number.isNaN(minValue) || Number.isNaN(maxValue)) {return () => false;}
        return (candidate) =>
            candidate instanceof Date &&
            candidate.getTime() >= minValue &&
            candidate.getTime() <= maxValue;
    }

    return () => false;
}
