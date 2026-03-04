import type { SortKey, OrderSpec } from "@/types";

export type Orderable1<T> = {
    index: number;
    item: T;
    k0: SortKey | null;
};

export type Orderable2<T> = {
    index: number;
    item: T;
    k0: SortKey | null;
    k1: SortKey | null;
};

export type Orderable3<T> = {
    index: number;
    item: T;
    k0: SortKey | null;
    k1: SortKey | null;
    k2: SortKey | null;
};

export type OrderableN<T> = {
    index: number;
    item: T;
    keys: Array<SortKey | null>;
};

export type Orderable<T> = Orderable1<T> | Orderable2<T> | Orderable3<T> | OrderableN<T>;

export function compareNullable(
    left: SortKey | null,
    right: SortKey | null,
    direction: 1 | -1,
    nullsFirst: boolean
): number {
    if (left === right) {return 0;}
    const leftNull = left === null;
    const rightNull = right === null;
    if (leftNull || rightNull) {
        if (leftNull && rightNull) {return 0;}
        if (leftNull) {return nullsFirst ? -1 : 1;}
        return nullsFirst ? 1 : -1;
    }

    if (typeof left === "number") {return (left - (right as number)) * direction;}
    if (typeof left === "string") {return (left < (right as string) ? -1 : (left > (right as string) ? 1 : 0)) * direction;}
    if (typeof left === "bigint") {return (left < (right as bigint) ? -1 : (left > (right as bigint) ? 1 : 0)) * direction;}
    return 0;
}

export function createComparator<T>(
    orders: ReadonlyArray<OrderSpec>
): (
    a: Orderable1<T> | Orderable2<T> | Orderable3<T> | OrderableN<T>,
    b: Orderable1<T> | Orderable2<T> | Orderable3<T> | OrderableN<T>
) => number {
    const count = orders.length;
    if (count === 1) {
        const direction0 = orders[0]!.direction;
        const nullsFirst0 = orders[0]!.nullsFirst;
        return (a, b) => {
            const diff = compareNullable(
                (a as Orderable1<T>).k0,
                (b as Orderable1<T>).k0,
                direction0,
                nullsFirst0
            );
            return diff === 0 ? a.index - b.index : diff;
        };
    }
    if (count === 2) {
        const direction0 = orders[0]!.direction;
        const nullsFirst0 = orders[0]!.nullsFirst;
        const direction1 = orders[1]!.direction;
        const nullsFirst1 = orders[1]!.nullsFirst;
        return (a, b) => {
            const orderA = a as Orderable2<T>;
            const orderB = b as Orderable2<T>;
            const diff0 = compareNullable(orderA.k0, orderB.k0, direction0, nullsFirst0);
            if (diff0 !== 0) {return diff0;}
            const diff1 = compareNullable(orderA.k1, orderB.k1, direction1, nullsFirst1);
            return diff1 === 0 ? a.index - b.index : diff1;
        };
    }
    if (count === 3) {
        const direction0 = orders[0]!.direction;
        const nullsFirst0 = orders[0]!.nullsFirst;
        const direction1 = orders[1]!.direction;
        const nullsFirst1 = orders[1]!.nullsFirst;
        const direction2 = orders[2]!.direction;
        const nullsFirst2 = orders[2]!.nullsFirst;
        return (a, b) => {
            const orderA = a as Orderable3<T>;
            const orderB = b as Orderable3<T>;
            const diff0 = compareNullable(orderA.k0, orderB.k0, direction0, nullsFirst0);
            if (diff0 !== 0) {return diff0;}
            const diff1 = compareNullable(orderA.k1, orderB.k1, direction1, nullsFirst1);
            if (diff1 !== 0) {return diff1;}
            const diff2 = compareNullable(orderA.k2, orderB.k2, direction2, nullsFirst2);
            return diff2 === 0 ? a.index - b.index : diff2;
        };
    }

    return (a, b) => {
        const orderA = a as OrderableN<T>;
        const orderB = b as OrderableN<T>;
        for (let i = 0; i < count; i++) {
            const diff = compareNullable(
                orderA.keys[i] as SortKey | null,
                orderB.keys[i] as SortKey | null,
                orders[i]!.direction,
                orders[i]!.nullsFirst
            );
            if (diff !== 0) {return diff;}
        }
        return a.index - b.index;
    };
}

export function heapPush<T>(
    heap: Array<T>,
    value: T,
    compare: (a: T, b: T) => number
): void {
    heap.push(value);
    let index = heap.length - 1;
    while (index > 0) {
        const parent = (index - 1) >> 1;
        if (compare(heap[index]!, heap[parent]!) <= 0) {break;}
        const temp = heap[parent]!;
        heap[parent] = heap[index]!;
        heap[index] = temp;
        index = parent;
    }
}

export function heapReplaceRoot<T>(
    heap: Array<T>,
    value: T,
    compare: (a: T, b: T) => number
): void {
    heap[0] = value;
    let index = 0;
    const length = heap.length;
    while (true) {
        const left = (index << 1) + 1;
        if (left >= length) {return;}
        const right = left + 1;
        let nextIndex = left;
        if (right < length && compare(heap[right]!, heap[left]!) > 0) {
            nextIndex = right;
        }
        if (compare(heap[nextIndex]!, heap[index]!) <= 0) {return;}
        const temp = heap[index]!;
        heap[index] = heap[nextIndex]!;
        heap[nextIndex] = temp;
        index = nextIndex;
    }
}
