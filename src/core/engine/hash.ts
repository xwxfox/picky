import type { Predicate } from "@/types";

export function hashPlanId<T>(
    predicates: Array<Predicate<T>>,
    searchKey: string
): string {
    const size = predicates.length;
    return `plan_${size}_${searchKey}`;
}
