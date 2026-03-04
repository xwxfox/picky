import type { ResolvePredicate } from "@/types";
import { toTimestamp } from "@/core/shared/cache";

export function createDateEqualsPredicate(
    parseIsoDate: (value: string) => number | null,
    right: Date | string | number
): ResolvePredicate | null {
    const rightTimestamp = toTimestamp(right, parseIsoDate);
    if (rightTimestamp === null || Number.isNaN(rightTimestamp)) {return null;}
    return (value) => {
        const leftTimestamp = toTimestamp(value, parseIsoDate);
        if (leftTimestamp === null) {return false;}
        return leftTimestamp === rightTimestamp;
    };
}

export function createDateComparePredicate(
    parseIsoDate: (value: string) => number | null,
    right: Date | string | number,
    op: "gt" | "gte" | "lt" | "lte"
): ResolvePredicate | null {
    const rightTimestamp = toTimestamp(right, parseIsoDate);
    if (rightTimestamp === null || Number.isNaN(rightTimestamp)) {return null;}

    if (op === "gt") {
        return (value) => {
            const leftTimestamp = toTimestamp(value, parseIsoDate);
            if (leftTimestamp === null) {return false;}
            return leftTimestamp > rightTimestamp;
        };
    }

    if (op === "gte") {
        return (value) => {
            const leftTimestamp = toTimestamp(value, parseIsoDate);
            if (leftTimestamp === null) {return false;}
            return leftTimestamp >= rightTimestamp;
        };
    }

    if (op === "lt") {
        return (value) => {
            const leftTimestamp = toTimestamp(value, parseIsoDate);
            if (leftTimestamp === null) {return false;}
            return leftTimestamp < rightTimestamp;
        };
    }

    return (value) => {
        const leftTimestamp = toTimestamp(value, parseIsoDate);
        if (leftTimestamp === null) {return false;}
        return leftTimestamp <= rightTimestamp;
    };
}

export function createDateBetweenPredicate(
    parseIsoDate: (value: string) => number | null,
    min: Date | string | number,
    max: Date | string | number
): ResolvePredicate | null {
    const minTimestamp = toTimestamp(min, parseIsoDate);
    const maxTimestamp = toTimestamp(max, parseIsoDate);
    if (
        minTimestamp === null ||
        maxTimestamp === null ||
        Number.isNaN(minTimestamp) ||
        Number.isNaN(maxTimestamp)
    ) {
        return null;
    }

    return (value) => {
        const valueTimestamp = toTimestamp(value, parseIsoDate);
        if (valueTimestamp === null) {return false;}
        return valueTimestamp >= minTimestamp && valueTimestamp <= maxTimestamp;
    };
}
