type Predicate<T> = (item: T) => boolean;

type ScalarValue =
    | string
    | number
    | boolean
    | bigint
    | symbol
    | null
    | undefined
    | Date;

// note: even though Date isn't a JS primitive, this is the "leaf" value set we accept.

type Comparable =
    | number
    | string
    | bigint
    | Date;

type SortKey = number | string | bigint;

type OrderDirection = "asc" | "desc";
type NullOrder = "first" | "last";

type GroupableValue =
    | string
    | number
    | boolean
    | bigint
    | symbol
    | Date;

type GroupKeyValue =
    | string
    | number
    | boolean
    | bigint
    | symbol;

type ResolveValue =
    | ScalarValue
    | { [key: string]: ResolveValue }
    | ResolveValue[];

// note: runtime shape is intentionally loose; type params keep callers honest.
type ResolveObject = { [key: string]: ResolveValue };

type Prev = [never, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

type PathValue<T, P extends string, SeenArray extends boolean = false, Depth extends number = 5> =
    Depth extends 0
    ? never
    : P extends `${infer K}.${infer Rest}`
    ? K extends keyof T
    ? T[K] extends readonly (infer U)[]
    ? SeenArray extends true
    ? never
    : PathValue<NonNullable<U>, Rest, true, Prev[Depth]>
    : PathValue<NonNullable<T[K]>, Rest, SeenArray, Prev[Depth]>
    : never
    : P extends keyof T
    ? T[P] extends readonly (infer U)[]
    ? SeenArray extends true
    ? never
    : U
    : T[P]
    : never;

type Path<T, P extends string> = PathValue<T, P> extends never ? never : P;

type ArrayPathValue<T, P extends string, SeenArray extends boolean = false> =
    P extends `${infer K}.${infer Rest}`
    ? K extends keyof T
    ? T[K] extends readonly (infer U)[]
    ? SeenArray extends true
    ? never
    : ArrayPathValue<NonNullable<U>, Rest, true>
    : ArrayPathValue<NonNullable<T[K]>, Rest, SeenArray>
    : never
    : P extends keyof T
    ? T[P] extends readonly (infer U)[]
    ? SeenArray extends true
    ? never
    : U
    : never
    : never;

type ArrayPath<T, P extends string> = ArrayPathValue<T, P> extends never ? never : P;

type ArrayPathItem<T, P extends string> = Extract<ArrayPathValue<T, P>, ResolveObject>;

type ArrayItem<T> = T extends readonly (infer U)[] ? U : never;

type Paths<T, SeenArray extends boolean = false, Depth extends number = 5> = Depth extends 0
    ? never
    : {
    [K in keyof T & string]:
        T[K] extends readonly (infer U)[]
        ? SeenArray extends true
            ? never
            : K | `${K}.${Paths<NonNullable<U>, true, Prev[Depth]>}`
        : NonNullable<T[K]> extends ResolveObject
            ? K | `${K}.${Paths<NonNullable<T[K]>, SeenArray, Prev[Depth]>}`
            : K
}[keyof T & string];

type NonDatePaths<T> = {
    [P in Paths<T>]: Extract<PathValue<T, P>, Date> extends never ? P : never
}[Paths<T>];

type DatePaths<T> = {
    [P in Paths<T>]: Extract<PathValue<T, P>, Date | string | number> extends never ? never : P
}[Paths<T>];

type SortablePaths<T> = {
    [P in Paths<T>]: Extract<PathValue<T, P>, Comparable> extends never ? never : P
}[Paths<T>];

type GroupablePaths<T> = {
    [P in Paths<T>]: Extract<PathValue<T, P>, GroupableValue> extends never ? never : P
}[Paths<T>];

type NonNullablePathValue<T, P extends string> = Exclude<PathValue<T, P>, null | undefined>;

type GroupKey<T, P extends string> = NonNullablePathValue<T, P> extends infer U
    ? U extends Date
        ? number
        : U
    : never;

type ArrayPaths<T, SeenArray extends boolean = false, Depth extends number = 5> = Depth extends 0
    ? never
    : {
    [K in keyof T & string]:
        T[K] extends readonly (infer U)[]
        ? SeenArray extends true
            ? never
            // note: reuse Paths for array items; keeps the "one array max" rule consistent.
            : K | `${K}.${Paths<NonNullable<U>, true, Prev[Depth]>}`
        : NonNullable<T[K]> extends ResolveObject
            ? `${K}.${ArrayPaths<NonNullable<T[K]>, SeenArray, Prev[Depth]>}`
            : never
}[keyof T & string];

type StableEntry<T> = {
    index: number;
    item: T;
};

type Orderable1<T> = {
    item: T;
    index: number;
    k0: SortKey | null;
};

type Orderable2<T> = {
    item: T;
    index: number;
    k0: SortKey | null;
    k1: SortKey | null;
};

type Orderable3<T> = {
    item: T;
    index: number;
    k0: SortKey | null;
    k1: SortKey | null;
    k2: SortKey | null;
};

type OrderableN<T> = {
    item: T;
    index: number;
    keys: (SortKey | null)[];
};

type Orderable<T> = Orderable1<T> | Orderable2<T> | Orderable3<T> | OrderableN<T>;

// note: date parsing is intentionally permissive; use date* methods explicitly.
const isoDateRegex = /^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?$/;

type CacheOptions = {
    maxDateCache: number;
    maxPathCache: number;
};

type CacheState = {
    maxDateCache: number;
    maxPathCache: number;
    pathSegmentsCache: Map<string, string[]>;
    dateCache: Map<string, number | null>;
    parseIsoDate: (value: string) => number | null;
    orderResolver: ResolveSortKey;
    orderResolverDate: ResolveSortKey;
    groupKeyConverter: (value: ResolveValue) => GroupKeyValue | null;
    groupKeyConverterDate: (value: ResolveValue) => GroupKeyValue | null;
};

const defaultCacheOptions: CacheOptions = {
    maxDateCache: 2048,
    maxPathCache: 2048,
};

let useSharedCache = false;
let sharedCacheState: CacheState | null = null;

function createDateCache(cache: Map<string, number | null>, maxDateCache: number) {
    return (value: string): number | null => {
        const cached = cache.get(value);
        if (cached !== undefined) return cached;

        if (value.length < 10 || !isoDateRegex.test(value)) {
            cache.set(value, null);
            return null;
        }

        const time = Date.parse(value);
        const result = Number.isNaN(time) ? null : time;
        cache.set(value, result);

        while (cache.size > maxDateCache) {
            // note: fifo is fine here; cache is capped + hot dates are cheap to recompute.
            const firstKey = cache.keys().next().value as string | undefined;
            if (firstKey !== undefined) cache.delete(firstKey);
        }

        return result;
    };
}

function createCacheState(options: CacheOptions): CacheState {
    const pathSegmentsCache = new Map<string, string[]>();
    const dateCache = new Map<string, number | null>();
    const parseIsoDate = createDateCache(dateCache, options.maxDateCache);
    const orderResolver = createOrderResolver();
    const orderResolverDate = createOrderResolverDate(parseIsoDate);
    const groupKeyConverter = createGroupKeyConverter();
    const groupKeyConverterDate = createGroupKeyConverterDate(parseIsoDate);

    return {
        maxDateCache: options.maxDateCache,
        maxPathCache: options.maxPathCache,
        pathSegmentsCache,
        dateCache,
        parseIsoDate,
        orderResolver,
        orderResolverDate,
        groupKeyConverter,
        groupKeyConverterDate,
    };
}

function toTimestamp(value: unknown, parseIsoDate: (value: string) => number | null): number | null {
    if (value instanceof Date) {
        const time = value.getTime();
        return Number.isNaN(time) ? null : time;
    }

    if (typeof value === "number") return value;

    if (typeof value !== "string") return null;
    return parseIsoDate(value);
}

function getSegments(cache: CacheState, path: string): string[] {
    const cached = cache.pathSegmentsCache.get(path);
    if (cached) return cached;
    const segments = path.split(".");
    cache.pathSegmentsCache.set(path, segments);
    while (cache.pathSegmentsCache.size > cache.maxPathCache) {
        // note: fifo eviction is enough; this keeps memory bounded for long-lived servers.
        const firstKey = cache.pathSegmentsCache.keys().next().value as string | undefined;
        if (firstKey !== undefined) cache.pathSegmentsCache.delete(firstKey);
    }
    return segments;
}


type ResolvePredicate = (value: ResolveValue) => boolean;

type ResolveSortKey = (value: ResolveValue) => SortKey | null;

type OrderSpec = {
    segments: string[];
    direction: 1 | -1;
    nullsFirst: boolean;
    resolve: ResolveSortKey;
};

type GroupSpec = {
    segments: string[];
    convert: (value: ResolveValue) => GroupKeyValue | null;
};

function compareNullable(
    left: SortKey | null,
    right: SortKey | null,
    direction: 1 | -1,
    nullsFirst: boolean
): number {
    if (left === right) return 0;
    const leftNull = left === null;
    const rightNull = right === null;
    if (leftNull || rightNull) {
        if (leftNull && rightNull) return 0;
        if (leftNull) return nullsFirst ? -1 : 1;
        return nullsFirst ? 1 : -1;
    }

    if (typeof left === "number") return (left - (right as number)) * direction;
    if (typeof left === "string") return (left < (right as string) ? -1 : left > (right as string) ? 1 : 0) * direction;
    if (typeof left === "bigint") return (left < (right as bigint) ? -1 : left > (right as bigint) ? 1 : 0) * direction;
    return 0;
}

function createComparator<T>(
    orders: readonly OrderSpec[]
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
            if (diff0 !== 0) return diff0;
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
            if (diff0 !== 0) return diff0;
            const diff1 = compareNullable(orderA.k1, orderB.k1, direction1, nullsFirst1);
            if (diff1 !== 0) return diff1;
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
            if (diff !== 0) return diff;
        }
        return a.index - b.index;
    };
}

function heapPush<T>(
    heap: T[],
    value: T,
    compare: (a: T, b: T) => number
): void {
    heap.push(value);
    let index = heap.length - 1;
    while (index > 0) {
        const parent = (index - 1) >> 1;
        if (compare(heap[index]!, heap[parent]!) <= 0) break;
        const temp = heap[parent]!;
        heap[parent] = heap[index]!;
        heap[index] = temp;
        index = parent;
    }
}

function heapReplaceRoot<T>(
    heap: T[],
    value: T,
    compare: (a: T, b: T) => number
): void {
    heap[0] = value;
    let index = 0;
    const length = heap.length;
    while (true) {
        const left = (index << 1) + 1;
        if (left >= length) return;
        const right = left + 1;
        let nextIndex = left;
        if (right < length && compare(heap[right]!, heap[left]!) > 0) {
            nextIndex = right;
        }
        if (compare(heap[nextIndex]!, heap[index]!) <= 0) return;
        const temp = heap[index]!;
        heap[index] = heap[nextIndex]!;
        heap[nextIndex] = temp;
        index = nextIndex;
    }
}

type PaginationTotalMode = "none" | "lazy" | "full";

type PaginationOptions = {
    pageSize: number;
    page?: number;
    total?: PaginationTotalMode;
};

type PaginationCursor<T> = {
    data: T[];
    page: number;
    total?: number;
    next: () => PaginationCursor<T>;
    previous: () => PaginationCursor<T>;
};

type OrderOptions = {
    direction?: OrderDirection;
    nulls?: NullOrder;
};

function someResolvedWithSegments(
    obj: ResolveObject,
    segments: string[],
    predicate: ResolvePredicate
): boolean {
    if (obj == null || typeof obj !== "object") return false;

    if (segments.length === 1) {
        const resolved = (obj as ResolveObject)[segments[0]!];
        if (Array.isArray(resolved)) {
            for (let i = 0; i < resolved.length; i++) {
                if (predicate(resolved[i] as ResolveValue)) return true;
            }
            return false;
        }
        return predicate(resolved as ResolveValue);
    }

    if (segments.length === 2) {
        const first = segments[0]!;
        const second = segments[1]!;
        const resolved = (obj as ResolveObject)[first];
        if (Array.isArray(resolved)) {
            for (let i = 0; i < resolved.length; i++) {
                const entry = resolved[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const value = (entry as ResolveObject)[second];
                if (Array.isArray(value)) continue;
                if (predicate(value as ResolveValue)) {
                    return true;
                }
            }
            return false;
        }
        if (resolved == null || typeof resolved !== "object") return false;
        const value = (resolved as ResolveObject)[second];
        if (Array.isArray(value)) {
            for (let i = 0; i < value.length; i++) {
                if (predicate(value[i] as ResolveValue)) return true;
            }
            return false;
        }
        return predicate(value as ResolveValue);
    }

    if (segments.length === 3) {
        const first = segments[0]!;
        const second = segments[1]!;
        const third = segments[2]!;
        const resolved = (obj as ResolveObject)[first];
        if (Array.isArray(resolved)) {
            for (let i = 0; i < resolved.length; i++) {
                const entry = resolved[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const value = (entry as ResolveObject)[second];
                if (Array.isArray(value)) continue;
                if (value == null || typeof value !== "object") continue;
                const leaf = (value as ResolveObject)[third];
                if (Array.isArray(leaf)) continue;
                if (predicate(leaf as ResolveValue)) {
                    return true;
                }
            }
            return false;
        }
        if (resolved == null || typeof resolved !== "object") return false;
        const value = (resolved as ResolveObject)[second];
        if (Array.isArray(value)) {
            for (let i = 0; i < value.length; i++) {
                const entry = value[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const leaf = (entry as ResolveObject)[third];
                if (Array.isArray(leaf)) continue;
                if (predicate(leaf as ResolveValue)) {
                    return true;
                }
            }
            return false;
        }
        if (value == null || typeof value !== "object") return false;
        const leaf = (value as ResolveObject)[third];
        if (Array.isArray(leaf)) {
            for (let i = 0; i < leaf.length; i++) {
                if (predicate(leaf[i] as ResolveValue)) return true;
            }
            return false;
        }
        return predicate(leaf as ResolveValue);
    }

    let current: ResolveValue[] = [obj as ResolveValue];
    let next: ResolveValue[] = [];
    let seenArray = false;

    for (let i = 0; i < segments.length; i++) {
        const segment = segments[i]!;
        const isLast = i === segments.length - 1;
        let nextIndex = 0;

        for (let j = 0; j < current.length; j++) {
            const value = current[j];
            if (value == null) continue;
            if (typeof value !== "object") continue;

            const resolved = (value as ResolveObject)[segment];

            if (Array.isArray(resolved)) {
                if (seenArray) continue;
                if (!isLast) seenArray = true;
                for (let k = 0; k < resolved.length; k++) {
                    const entry = resolved[k] as ResolveValue;
                    if (isLast) {
                        if (predicate(entry)) return true;
                    } else {
                        next[nextIndex++] = entry;
                    }
                }
            } else if (isLast) {
                if (predicate(resolved as ResolveValue)) return true;
            } else {
                next[nextIndex++] = resolved as ResolveValue;
            }
        }

        if (isLast) return false;

        next.length = nextIndex;
        const temp = current;
        current = next;
        next = temp;
    }

    return false;
}

function resolveFirstWithSegments(
    obj: ResolveObject,
    segments: string[]
): ResolveValue | undefined {
    if (obj == null || typeof obj !== "object") return undefined;
    if (segments.length === 1) {
        const resolved = (obj as ResolveObject)[segments[0]!];
        if (Array.isArray(resolved)) return resolved.length > 0 ? (resolved[0] as ResolveValue) : undefined;
        return resolved as ResolveValue;
    }

    if (segments.length === 2) {
        const first = segments[0]!;
        const second = segments[1]!;
        const resolved = (obj as ResolveObject)[first];
        if (Array.isArray(resolved)) {
            for (let i = 0; i < resolved.length; i++) {
                const entry = resolved[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const value = (entry as ResolveObject)[second];
                if (Array.isArray(value)) continue;
                return value as ResolveValue;
            }
            return undefined;
        }
        if (resolved == null || typeof resolved !== "object") return undefined;
        const value = (resolved as ResolveObject)[second];
        if (Array.isArray(value)) {
            return value.length > 0 ? (value[0] as ResolveValue) : undefined;
        }
        return value as ResolveValue;
    }

    if (segments.length === 3) {
        const first = segments[0]!;
        const second = segments[1]!;
        const third = segments[2]!;
        const resolved = (obj as ResolveObject)[first];
        if (Array.isArray(resolved)) {
            for (let i = 0; i < resolved.length; i++) {
                const entry = resolved[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const value = (entry as ResolveObject)[second];
                if (Array.isArray(value)) continue;
                if (value == null || typeof value !== "object") continue;
                const leaf = (value as ResolveObject)[third];
                if (Array.isArray(leaf)) continue;
                return leaf as ResolveValue;
            }
            return undefined;
        }
        if (resolved == null || typeof resolved !== "object") return undefined;
        const value = (resolved as ResolveObject)[second];
        if (Array.isArray(value)) {
            for (let i = 0; i < value.length; i++) {
                const entry = value[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const leaf = (entry as ResolveObject)[third];
                if (Array.isArray(leaf)) continue;
                return leaf as ResolveValue;
            }
            return undefined;
        }
        if (value == null || typeof value !== "object") return undefined;
        const leaf = (value as ResolveObject)[third];
        if (Array.isArray(leaf)) return leaf.length > 0 ? (leaf[0] as ResolveValue) : undefined;
        return leaf as ResolveValue;
    }

    let current: ResolveValue[] = [obj as ResolveValue];
    let next: ResolveValue[] = [];
    let seenArray = false;

    for (let i = 0; i < segments.length; i++) {
        const segment = segments[i]!;
        const isLast = i === segments.length - 1;
        let nextIndex = 0;

        for (let j = 0; j < current.length; j++) {
            const value = current[j];
            if (value == null) continue;
            if (typeof value !== "object") continue;

            const resolved = (value as ResolveObject)[segment];

            if (Array.isArray(resolved)) {
                if (seenArray) continue;
                if (isLast) {
                    return resolved.length > 0 ? (resolved[0] as ResolveValue) : undefined;
                }
                seenArray = true;
                for (let k = 0; k < resolved.length; k++) {
                    next[nextIndex++] = resolved[k] as ResolveValue;
                }
            } else if (isLast) {
                return resolved as ResolveValue;
            } else {
                next[nextIndex++] = resolved as ResolveValue;
            }
        }

        if (isLast) return undefined;

        next.length = nextIndex;
        const temp = current;
        current = next;
        next = temp;
    }

    return undefined;
}

function forEachResolvedWithSegments(
    obj: ResolveObject,
    segments: string[],
    visit: (value: ResolveValue) => void
): void {
    if (obj == null || typeof obj !== "object") return;

    if (segments.length === 1) {
        const resolved = (obj as ResolveObject)[segments[0]!];
        if (Array.isArray(resolved)) {
            for (let i = 0; i < resolved.length; i++) {
                visit(resolved[i] as ResolveValue);
            }
            return;
        }
        visit(resolved as ResolveValue);
        return;
    }

    if (segments.length === 2) {
        const first = segments[0]!;
        const second = segments[1]!;
        const resolved = (obj as ResolveObject)[first];
        if (Array.isArray(resolved)) {
            for (let i = 0; i < resolved.length; i++) {
                const entry = resolved[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const value = (entry as ResolveObject)[second];
                if (Array.isArray(value)) continue;
                visit(value as ResolveValue);
            }
            return;
        }
        if (resolved == null || typeof resolved !== "object") return;
        const value = (resolved as ResolveObject)[second];
        if (Array.isArray(value)) {
            for (let i = 0; i < value.length; i++) {
                visit(value[i] as ResolveValue);
            }
            return;
        }
        visit(value as ResolveValue);
        return;
    }

    if (segments.length === 3) {
        const first = segments[0]!;
        const second = segments[1]!;
        const third = segments[2]!;
        const resolved = (obj as ResolveObject)[first];
        if (Array.isArray(resolved)) {
            for (let i = 0; i < resolved.length; i++) {
                const entry = resolved[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const value = (entry as ResolveObject)[second];
                if (Array.isArray(value)) continue;
                if (value == null || typeof value !== "object") continue;
                const leaf = (value as ResolveObject)[third];
                if (Array.isArray(leaf)) continue;
                visit(leaf as ResolveValue);
            }
            return;
        }
        if (resolved == null || typeof resolved !== "object") return;
        const value = (resolved as ResolveObject)[second];
        if (Array.isArray(value)) {
            for (let i = 0; i < value.length; i++) {
                const entry = value[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const leaf = (entry as ResolveObject)[third];
                if (Array.isArray(leaf)) continue;
                visit(leaf as ResolveValue);
            }
            return;
        }
        if (value == null || typeof value !== "object") return;
        const leaf = (value as ResolveObject)[third];
        if (Array.isArray(leaf)) {
            for (let i = 0; i < leaf.length; i++) {
                visit(leaf[i] as ResolveValue);
            }
            return;
        }
        visit(leaf as ResolveValue);
        return;
    }

    let current: ResolveValue[] = [obj as ResolveValue];
    let next: ResolveValue[] = [];
    let seenArray = false;

    for (let i = 0; i < segments.length; i++) {
        const segment = segments[i]!;
        const isLast = i === segments.length - 1;
        let nextIndex = 0;

        for (let j = 0; j < current.length; j++) {
            const value = current[j];
            if (value == null) continue;
            if (typeof value !== "object") continue;

            const resolved = (value as ResolveObject)[segment];

            if (Array.isArray(resolved)) {
                if (seenArray) continue;
                if (isLast) {
                    for (let k = 0; k < resolved.length; k++) {
                        visit(resolved[k] as ResolveValue);
                    }
                } else {
                    seenArray = true;
                    for (let k = 0; k < resolved.length; k++) {
                        next[nextIndex++] = resolved[k] as ResolveValue;
                    }
                }
            } else if (isLast) {
                visit(resolved as ResolveValue);
            } else {
                next[nextIndex++] = resolved as ResolveValue;
            }
        }

        if (isLast) return;

        next.length = nextIndex;
        const temp = current;
        current = next;
        next = temp;
    }
}

function resolveOrderValueWithSegments(
    obj: ResolveObject,
    segments: string[],
    resolve: ResolveSortKey
): SortKey | null {
    const value = resolveFirstWithSegments(obj, segments);
    if (value === undefined || value === null) return null;
    return resolve(value);
}

function createOrderResolver(): ResolveSortKey {
    return (value) => {
        if (typeof value === "number") return Number.isNaN(value) ? null : value;
        if (typeof value === "string") return value;
        if (typeof value === "bigint") return value;
        if (value instanceof Date) {
            const time = value.getTime();
            return Number.isNaN(time) ? null : time;
        }
        return null;
    };
}

function createOrderResolverDate(parseIsoDate: (value: string) => number | null): ResolveSortKey {
    return (value) => {
        if (value instanceof Date) {
            const time = value.getTime();
            return Number.isNaN(time) ? null : time;
        }
        if (typeof value === "number") return Number.isNaN(value) ? null : value;
        if (typeof value === "string") return parseIsoDate(value);
        return null;
    };
}

function createGroupKeyConverter() {
    return (value: ResolveValue): GroupKeyValue | null => {
        if (value == null) return null;
        if (typeof value === "string") return value;
        if (typeof value === "number") return Number.isNaN(value) ? null : value;
        if (typeof value === "boolean") return value;
        if (typeof value === "bigint") return value;
        if (typeof value === "symbol") return value;
        if (value instanceof Date) {
            const time = value.getTime();
            return Number.isNaN(time) ? null : time;
        }
        return null;
    };
}

function createGroupKeyConverterDate(parseIsoDate: (value: string) => number | null) {
    return (value: ResolveValue): GroupKeyValue | null => {
        if (value == null) return null;
        if (value instanceof Date) {
            const time = value.getTime();
            return Number.isNaN(time) ? null : time;
        }
        if (typeof value === "number") return Number.isNaN(value) ? null : value;
        if (typeof value === "string") return parseIsoDate(value);
        if (typeof value === "boolean") return value;
        if (typeof value === "bigint") return value;
        if (typeof value === "symbol") return value;
        return null;
    };
}

function everyResolvedWithSegments(
    obj: ResolveObject,
    segments: string[],
    predicate: ResolvePredicate
): boolean {
    if (obj == null || typeof obj !== "object") return false;

    if (segments.length === 1) {
        const resolved = (obj as ResolveObject)[segments[0]!];
        if (Array.isArray(resolved)) {
            if (resolved.length === 0) return false;
            for (let i = 0; i < resolved.length; i++) {
                if (!predicate(resolved[i] as ResolveValue)) return false;
            }
            return true;
        }
        return predicate(resolved as ResolveValue);
    }

    if (segments.length === 2) {
        const first = segments[0]!;
        const second = segments[1]!;
        const resolved = (obj as ResolveObject)[first];
        let found = false;
        if (Array.isArray(resolved)) {
            for (let i = 0; i < resolved.length; i++) {
                const entry = resolved[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const value = (entry as ResolveObject)[second];
                if (Array.isArray(value)) continue;
                found = true;
                if (!predicate(value as ResolveValue)) return false;
            }
            return found;
        }
        if (resolved == null || typeof resolved !== "object") return false;
        const value = (resolved as ResolveObject)[second];
        if (Array.isArray(value)) {
            if (value.length === 0) return false;
            for (let i = 0; i < value.length; i++) {
                found = true;
                if (!predicate(value[i] as ResolveValue)) return false;
            }
            return found;
        }
        return predicate(value as ResolveValue);
    }

    if (segments.length === 3) {
        const first = segments[0]!;
        const second = segments[1]!;
        const third = segments[2]!;
        const resolved = (obj as ResolveObject)[first];
        let found = false;
        if (Array.isArray(resolved)) {
            for (let i = 0; i < resolved.length; i++) {
                const entry = resolved[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const value = (entry as ResolveObject)[second];
                if (Array.isArray(value)) continue;
                if (value == null || typeof value !== "object") continue;
                const leaf = (value as ResolveObject)[third];
                if (Array.isArray(leaf)) continue;
                found = true;
                if (!predicate(leaf as ResolveValue)) return false;
            }
            return found;
        }
        if (resolved == null || typeof resolved !== "object") return false;
        const value = (resolved as ResolveObject)[second];
        if (Array.isArray(value)) {
            for (let i = 0; i < value.length; i++) {
                const entry = value[i] as ResolveValue;
                if (entry == null || typeof entry !== "object") continue;
                const leaf = (entry as ResolveObject)[third];
                if (Array.isArray(leaf)) continue;
                found = true;
                if (!predicate(leaf as ResolveValue)) return false;
            }
            return found;
        }
        if (value == null || typeof value !== "object") return false;
        const leaf = (value as ResolveObject)[third];
        if (Array.isArray(leaf)) {
            if (leaf.length === 0) return false;
            for (let i = 0; i < leaf.length; i++) {
                if (!predicate(leaf[i] as ResolveValue)) return false;
            }
            return true;
        }
        return predicate(leaf as ResolveValue);
    }

    let current: ResolveValue[] = [obj as ResolveValue];
    let next: ResolveValue[] = [];
    let seenArray = false;
    let found = false;

    for (let i = 0; i < segments.length; i++) {
        const segment = segments[i]!;
        const isLast = i === segments.length - 1;
        let nextIndex = 0;

        for (let j = 0; j < current.length; j++) {
            const value = current[j];
            if (value == null) continue;
            if (typeof value !== "object") continue;

            const resolved = (value as ResolveObject)[segment];

            if (Array.isArray(resolved)) {
                if (seenArray) continue;
                if (!isLast) seenArray = true;
                if (isLast) {
                    if (resolved.length === 0) continue;
                    for (let k = 0; k < resolved.length; k++) {
                        found = true;
                        if (!predicate(resolved[k] as ResolveValue)) return false;
                    }
                } else {
                    for (let k = 0; k < resolved.length; k++) {
                        next[nextIndex++] = resolved[k] as ResolveValue;
                    }
                }
            } else if (isLast) {
                found = true;
                if (!predicate(resolved as ResolveValue)) return false;
            } else {
                next[nextIndex++] = resolved as ResolveValue;
            }
        }

        if (isLast) return found;

        next.length = nextIndex;
        const temp = current;
        current = next;
        next = temp;
    }

    return found;
}

function pathExistsWithSegments(obj: ResolveObject, segments: string[]): boolean {
    if (segments.length === 1) {
        const segment = segments[0]!;
        if (obj == null || typeof obj !== "object") return false;
        return Object.prototype.hasOwnProperty.call(obj as ResolveObject, segment);
    }
    let current: ResolveValue[] = [obj as ResolveValue];
    let next: ResolveValue[] = [];
    let seenArray = false;

    for (let i = 0; i < segments.length; i++) {
        const segment = segments[i]!;
        const isLast = i === segments.length - 1;
        let nextIndex = 0;

        for (let j = 0; j < current.length; j++) {
            const value = current[j];
            if (value == null) continue;
            if (typeof value !== "object") continue;

            const objValue = value as ResolveObject;
            if (!Object.prototype.hasOwnProperty.call(objValue, segment)) continue;
            const resolved = objValue[segment];
            if (isLast) return true;

            if (Array.isArray(resolved)) {
                if (seenArray) continue;
                seenArray = true;
                for (let k = 0; k < resolved.length; k++) {
                    next[nextIndex++] = resolved[k] as ResolveValue;
                }
            } else {
                next[nextIndex++] = resolved as ResolveValue;
            }
        }

        next.length = nextIndex;
        const temp = current;
        current = next;
        next = temp;
    }

    return false;
}


function createComparePredicate(
    value: ResolveValue,
    op: "gt" | "gte" | "lt" | "lte"
): ResolvePredicate {
    if (typeof value === "number") {
        const right = value;
        if (op === "gt") return (candidate) => typeof candidate === "number" && candidate > right;
        if (op === "gte") return (candidate) => typeof candidate === "number" && candidate >= right;
        if (op === "lt") return (candidate) => typeof candidate === "number" && candidate < right;
        return (candidate) => typeof candidate === "number" && candidate <= right;
    }

    if (typeof value === "string") {
        const right = value;
        if (op === "gt") return (candidate) => typeof candidate === "string" && candidate > right;
        if (op === "gte") return (candidate) => typeof candidate === "string" && candidate >= right;
        if (op === "lt") return (candidate) => typeof candidate === "string" && candidate < right;
        return (candidate) => typeof candidate === "string" && candidate <= right;
    }

    if (typeof value === "bigint") {
        const right = value;
        if (op === "gt") return (candidate) => typeof candidate === "bigint" && candidate > right;
        if (op === "gte") return (candidate) => typeof candidate === "bigint" && candidate >= right;
        if (op === "lt") return (candidate) => typeof candidate === "bigint" && candidate < right;
        return (candidate) => typeof candidate === "bigint" && candidate <= right;
    }

    if (value instanceof Date) {
        const right = value.getTime();
        if (Number.isNaN(right)) return () => false;
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

function createBetweenPredicate(min: ResolveValue, max: ResolveValue): ResolvePredicate {
    if (typeof min !== typeof max) return () => false;

    if (typeof min === "number") {
        const minValue = min;
        const maxValue = max as number;
        if (Number.isNaN(minValue) || Number.isNaN(maxValue)) return () => false;
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
        if (Number.isNaN(minValue) || Number.isNaN(maxValue)) return () => false;
        return (candidate) =>
            candidate instanceof Date &&
            candidate.getTime() >= minValue &&
            candidate.getTime() <= maxValue;
    }

    return () => false;
}

function createDateEqualsPredicate(
    parseIsoDate: (value: string) => number | null,
    right: Date | string | number
): ResolvePredicate | null {
    const rightTimestamp = toTimestamp(right, parseIsoDate);
    if (rightTimestamp === null || Number.isNaN(rightTimestamp)) return null;
    return (value) => {
        const leftTimestamp = toTimestamp(value, parseIsoDate);
        if (leftTimestamp === null) return false;
        return leftTimestamp === rightTimestamp;
    };
}

function createDateComparePredicate(
    parseIsoDate: (value: string) => number | null,
    right: Date | string | number,
    op: "gt" | "gte" | "lt" | "lte"
): ResolvePredicate | null {
    const rightTimestamp = toTimestamp(right, parseIsoDate);
    if (rightTimestamp === null || Number.isNaN(rightTimestamp)) return null;

    if (op === "gt") {
        return (value) => {
            const leftTimestamp = toTimestamp(value, parseIsoDate);
            if (leftTimestamp === null) return false;
            return leftTimestamp > rightTimestamp;
        };
    }

    if (op === "gte") {
        return (value) => {
            const leftTimestamp = toTimestamp(value, parseIsoDate);
            if (leftTimestamp === null) return false;
            return leftTimestamp >= rightTimestamp;
        };
    }

    if (op === "lt") {
        return (value) => {
            const leftTimestamp = toTimestamp(value, parseIsoDate);
            if (leftTimestamp === null) return false;
            return leftTimestamp < rightTimestamp;
        };
    }

    return (value) => {
        const leftTimestamp = toTimestamp(value, parseIsoDate);
        if (leftTimestamp === null) return false;
        return leftTimestamp <= rightTimestamp;
    };
}

function createDateBetweenPredicate(
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
        if (valueTimestamp === null) return false;
        return valueTimestamp >= minTimestamp && valueTimestamp <= maxTimestamp;
    };
}

// note: stricter compile-time shape could also work
// type Resolvable<T> = T extends object ? { [K in keyof T]: Resolvable<T[K]> } : T;
// pros: catches non-resolvable shapes early; cons: heavy on TS for deep/recursive types.
export class FilterEngine<T extends Record<string, unknown>> {
    private constructor(
        private readonly data: readonly T[],
        private readonly predicates: readonly Predicate<T>[],
        private readonly cache: CacheState,
        private readonly orders: readonly OrderSpec[] = [],
        private readonly limitCount: number | null = null,
        private readonly offsetCount: number = 0
    ) { }

    static from<T extends Record<string, unknown>>(data: readonly T[]): FilterEngine<T> {
        const cache = useSharedCache
            ? (sharedCacheState ?? (sharedCacheState = createCacheState(defaultCacheOptions)))
            : createCacheState(defaultCacheOptions);
        return new FilterEngine<T>(data, [], cache);
    }

    static configure(options: {
        maxDateCache?: number;
        maxPathCache?: number;
        sharedCache?: boolean;
    }): void {
        if (typeof options.sharedCache === "boolean") useSharedCache = options.sharedCache;
        if (typeof options.maxDateCache === "number") defaultCacheOptions.maxDateCache = options.maxDateCache;
        if (typeof options.maxPathCache === "number") defaultCacheOptions.maxPathCache = options.maxPathCache;
        if (useSharedCache) {
            sharedCacheState = createCacheState(defaultCacheOptions);
        }
    }

    static clearCaches(): void {
        if (sharedCacheState) {
            sharedCacheState.pathSegmentsCache.clear();
            sharedCacheState.dateCache.clear();
        }
    }

    private withPredicate(predicate: Predicate<T>): FilterEngine<T> {
        return new FilterEngine(
            this.data,
            [...this.predicates, predicate],
            this.cache,
            this.orders,
            this.limitCount,
            this.offsetCount
        );
    }

    private andPredicate(condition: Predicate<T>): FilterEngine<T> {
        return this.withPredicate(condition);
    }

    private applyPipeline(): T[] {
        const predicate = this.compile();
        const predicateCount = this.predicates.length;
        const hasOrder = this.orders.length > 0;
        const limitCount = this.limitCount;
        const offsetCount = this.offsetCount;
        if (predicateCount === 0) {
            if (!hasOrder) {
                const start = offsetCount > 0 ? offsetCount : 0;
                if (limitCount === null) return start > 0 ? this.data.slice(start) : this.data.slice();
                if (limitCount <= 0) return [];
                return this.data.slice(start, start + limitCount);
            }
        }

        if (!hasOrder && (limitCount !== null || offsetCount > 0)) {
            const result: T[] = [];
            const start = offsetCount > 0 ? offsetCount : 0;
            const max = limitCount === null ? Number.POSITIVE_INFINITY : limitCount;
            if (max <= 0) return result;

            let matched = 0;
            for (let i = 0; i < this.data.length; i++) {
                const item = this.data[i]!;
                if (!predicate(item)) continue;
                if (matched < start) {
                    matched++;
                    continue;
                }
                result.push(item);
                matched++;
                if (result.length >= max) break;
            }
            return result;
        }

        if (!hasOrder) {
            const filtered: T[] = [];
            for (let i = 0; i < this.data.length; i++) {
                const item = this.data[i]!;
                if (predicate(item)) filtered.push(item);
            }

            if (offsetCount > 0 || limitCount !== null) {
                const start = offsetCount > 0 ? offsetCount : 0;
                const end = limitCount === null ? filtered.length : start + limitCount;
                if (start >= filtered.length) return [];
                return filtered.slice(start, end);
            }
            return filtered;
        }

        const orderCount = this.orders.length;
        const compare = createComparator<T>(this.orders);

        if (orderCount === 1 || orderCount === 2 || orderCount === 3) {
            const entries: (Orderable1<T> | Orderable2<T> | Orderable3<T>)[] = [];
            for (let i = 0; i < this.data.length; i++) {
                const item = this.data[i]!;
                if (!predicate(item)) continue;
                if (orderCount === 1) {
                    const order0 = this.orders[0]!;
                    entries.push({
                        item,
                        index: entries.length,
                        k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                    } as Orderable1<T>);
                } else if (orderCount === 2) {
                    const order0 = this.orders[0]!;
                    const order1 = this.orders[1]!;
                    entries.push({
                        item,
                        index: entries.length,
                        k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                        k1: resolveOrderValueWithSegments(item as ResolveObject, order1.segments, order1.resolve),
                    } as Orderable2<T>);
                } else {
                    const order0 = this.orders[0]!;
                    const order1 = this.orders[1]!;
                    const order2 = this.orders[2]!;
                    entries.push({
                        item,
                        index: entries.length,
                        k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                        k1: resolveOrderValueWithSegments(item as ResolveObject, order1.segments, order1.resolve),
                        k2: resolveOrderValueWithSegments(item as ResolveObject, order2.segments, order2.resolve),
                    } as Orderable3<T>);
                }
            }

            const start = offsetCount > 0 ? offsetCount : 0;
            const end = limitCount === null ? entries.length : start + limitCount;
            if (start >= entries.length) return [];

            if (limitCount !== null && limitCount > 0) {
                const desired = start + limitCount;
                if (desired > 0 && desired < entries.length) {
                    const topCount = desired;
                    const heap: (Orderable1<T> | Orderable2<T> | Orderable3<T>)[] = [];
                    for (let i = 0; i < entries.length; i++) {
                        const entry = entries[i]!;
                        if (heap.length < topCount) {
                            heapPush(heap, entry, compare);
                        } else if (compare(entry, heap[0]!) < 0) {
                            heapReplaceRoot(heap, entry, compare);
                        }
                    }
                    heap.sort(compare);
                    const result: T[] = [];
                    const last = end < heap.length ? end : heap.length;
                    for (let i = start; i < last; i++) result.push(heap[i]!.item);
                    return result;
                }
            }

            entries.sort(compare);
            const result: T[] = [];
            const last = end < entries.length ? end : entries.length;
            for (let i = start; i < last; i++) result.push(entries[i]!.item);
            return result;
        }

        const entries: OrderableN<T>[] = [];
        for (let i = 0; i < this.data.length; i++) {
            const item = this.data[i]!;
            if (!predicate(item)) continue;
            const keys = new Array<SortKey | null>(orderCount);
            for (let j = 0; j < orderCount; j++) {
                const order = this.orders[j]!;
                keys[j] = resolveOrderValueWithSegments(item as ResolveObject, order.segments, order.resolve);
            }
            entries.push({ item, index: entries.length, keys });
        }

        const start = offsetCount > 0 ? offsetCount : 0;
        const end = limitCount === null ? entries.length : start + limitCount;
        if (start >= entries.length) return [];

        if (limitCount !== null && limitCount > 0) {
            const desired = start + limitCount;
            if (desired > 0 && desired < entries.length) {
                const topCount = desired;
                const heap: OrderableN<T>[] = [];
                for (let i = 0; i < entries.length; i++) {
                    const entry = entries[i]!;
                    if (heap.length < topCount) {
                        heapPush(heap, entry, compare);
                    } else if (compare(entry, heap[0]!) < 0) {
                        heapReplaceRoot(heap, entry, compare);
                    }
                }
                heap.sort(compare);
                const result: T[] = [];
                const last = end < heap.length ? end : heap.length;
                for (let i = start; i < last; i++) result.push(heap[i]!.item);
                return result;
            }
        }

        entries.sort(compare);

        const result: T[] = [];
        const last = end < entries.length ? end : entries.length;
        for (let i = start; i < last; i++) {
            result.push(entries[i]!.item);
        }
        return result;
    }

    private groupItems(
        items: readonly T[],
        segments: string[],
        convert: (value: ResolveValue) => GroupKeyValue | null
    ): Map<GroupKeyValue, T[]> {
        const result = new Map<GroupKeyValue, T[]>();
        for (let i = 0; i < items.length; i++) {
            const item = items[i]!;
            forEachResolvedWithSegments(item as ResolveObject, segments, (value) => {
                const key = convert(value);
                if (key === null || key === undefined) return;
                const bucket = result.get(key);
                if (bucket) bucket.push(item);
                else result.set(key, [item]);
            });
        }
        return result;
    }

    private createCursorUnordered(
        predicate: Predicate<T>,
        pageSize: number,
        startPage: number,
        totalMode: PaginationTotalMode
    ): PaginationCursor<T> {
        const data = this.data;
        const length = data.length;
        const predicateCount = this.predicates.length;
        const history: number[] = [];
        let scanIndex = 0;
        let matchedCount = 0;
        let page = startPage > 0 ? startPage : 1;
        let cachedTotal: number | undefined;

        if (totalMode === "full") {
            if (predicateCount === 0) {
                cachedTotal = length;
            } else {
                let total = 0;
                for (let i = 0; i < length; i++) {
                    if (predicate(data[i]!)) total++;
                }
                cachedTotal = total;
            }
        }

        const totalFn = () => {
            if (totalMode === "none") return undefined;
            if (cachedTotal !== undefined) return cachedTotal;
            if (predicateCount === 0) {
                cachedTotal = length;
                return cachedTotal;
            }
            let total = 0;
            for (let i = 0; i < length; i++) {
                if (predicate(data[i]!)) total++;
            }
            cachedTotal = total;
            return total;
        };

        const cursor: PaginationCursor<T> = {
            data: [],
            page,
            total: totalMode === "none" ? undefined : cachedTotal,
            next: () => {
                page++;
                fillPage(cursor.data, page);
                cursor.page = page;
                cursor.total = totalFn();
                return cursor;
            },
            previous: () => {
                if (page <= 1) return cursor;
                page--;
                restorePageStart(page);
                fillPage(cursor.data, page);
                cursor.page = page;
                cursor.total = totalFn();
                return cursor;
            },
        };

        const restorePageStart = (targetPage: number) => {
            const targetIndex = targetPage - 1;
            const idx = targetIndex > 0 ? history[targetIndex - 1] : 0;
            scanIndex = idx ?? 0;
            matchedCount = (targetPage - 1) * pageSize;
        };

        const fillPage = (buffer: T[], targetPage: number) => {
            const targetMatched = (targetPage - 1) * pageSize;
            if (matchedCount > targetMatched) {
                restorePageStart(targetPage);
            } else if (matchedCount < targetMatched) {
                if (predicateCount === 0) {
                    scanIndex = targetMatched;
                    matchedCount = targetMatched;
                } else {
                    while (matchedCount < targetMatched && scanIndex < length) {
                        const item = data[scanIndex++]!;
                        if (predicate(item)) matchedCount++;
                    }
                }
            }

            const pageStart = scanIndex;
            buffer.length = 0;

            let collected = 0;
            if (predicateCount === 0) {
                while (scanIndex < length && collected < pageSize) {
                    buffer[collected++] = data[scanIndex++]!;
                    matchedCount++;
                }
            } else {
                while (scanIndex < length && collected < pageSize) {
                    const item = data[scanIndex++]!;
                    if (!predicate(item)) continue;
                    buffer[collected++] = item;
                    matchedCount++;
                }
            }

            if (history.length < targetPage) history.push(pageStart);
        };

        fillPage(cursor.data, page);
        cursor.total = totalFn();
        return cursor;
    }

    private createCursorOrdered(
        predicate: Predicate<T>,
        pageSize: number,
        startPage: number,
        totalMode: PaginationTotalMode
    ): PaginationCursor<T> {
        const data = this.data;
        const orderCount = this.orders.length;
        const compare = createComparator<T>(this.orders);
        const predicateCount = this.predicates.length;

        const entries: Orderable<T>[] = [];
        for (let i = 0; i < data.length; i++) {
            const item = data[i]!;
            if (predicateCount > 0 && !predicate(item)) continue;
            if (orderCount === 1) {
                const order0 = this.orders[0]!;
                entries.push({
                    item,
                    index: entries.length,
                    k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                } as Orderable1<T>);
            } else if (orderCount === 2) {
                const order0 = this.orders[0]!;
                const order1 = this.orders[1]!;
                entries.push({
                    item,
                    index: entries.length,
                    k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                    k1: resolveOrderValueWithSegments(item as ResolveObject, order1.segments, order1.resolve),
                } as Orderable2<T>);
            } else if (orderCount === 3) {
                const order0 = this.orders[0]!;
                const order1 = this.orders[1]!;
                const order2 = this.orders[2]!;
                entries.push({
                    item,
                    index: entries.length,
                    k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                    k1: resolveOrderValueWithSegments(item as ResolveObject, order1.segments, order1.resolve),
                    k2: resolveOrderValueWithSegments(item as ResolveObject, order2.segments, order2.resolve),
                } as Orderable3<T>);
            } else {
                const keys = new Array<SortKey | null>(orderCount);
                for (let j = 0; j < orderCount; j++) {
                    const order = this.orders[j]!;
                    keys[j] = resolveOrderValueWithSegments(item as ResolveObject, order.segments, order.resolve);
                }
                entries.push({ item, index: entries.length, keys } as OrderableN<T>);
            }
        }

        entries.sort(compare);

        const total = entries.length;
        const cursor: PaginationCursor<T> = {
            data: [],
            page: startPage > 0 ? startPage : 1,
            total: totalMode === "none" ? undefined : total,
            next: () => {
                cursor.page++;
                fillPage(cursor.data, cursor.page);
                return cursor;
            },
            previous: () => {
                if (cursor.page <= 1) return cursor;
                cursor.page--;
                fillPage(cursor.data, cursor.page);
                return cursor;
            },
        };

        const fillPage = (buffer: T[], page: number) => {
            const start = (page - 1) * pageSize;
            buffer.length = 0;
            if (start >= entries.length) return;
            const end = start + pageSize;
            const last = end < entries.length ? end : entries.length;
            for (let i = start; i < last; i++) {
                buffer.push(entries[i]!.item);
            }
        };

        fillPage(cursor.data, cursor.page);
        return cursor;
    }

    public result(): T[] {
        return this.applyPipeline();
    }

    public groupBy<P extends GroupablePaths<T>>(
        field: P,
        options?: { date?: boolean }
    ): Map<GroupKey<T, P>, T[]> {
        const segments = getSegments(this.cache, String(field));
        const convert = options?.date ? this.cache.groupKeyConverterDate : this.cache.groupKeyConverter;

        if (this.orders.length > 0) {
            const items = this.applyPipeline();
            return this.groupItems(items, segments, convert) as Map<GroupKey<T, P>, T[]>;
        }

        if (this.predicates.length === 0) {
            const result = new Map<GroupKeyValue, T[]>();
            const limitCount = this.limitCount;
            const offsetCount = this.offsetCount;
            const start = offsetCount > 0 ? offsetCount : 0;
            const max = limitCount === null ? Number.POSITIVE_INFINITY : limitCount;
            if (max <= 0) return result as Map<GroupKey<T, P>, T[]>;
            const end = max === Number.POSITIVE_INFINITY ? this.data.length : start + max;
            const last = end < this.data.length ? end : this.data.length;
            for (let i = start; i < last; i++) {
                const item = this.data[i]!;
                forEachResolvedWithSegments(item as ResolveObject, segments, (value) => {
                    const key = convert(value);
                    if (key === null || key === undefined) return;
                    const bucket = result.get(key);
                    if (bucket) bucket.push(item);
                    else result.set(key, [item]);
                });
            }
            return result as Map<GroupKey<T, P>, T[]>;
        }

        const predicate = this.compile();
        const result = new Map<GroupKeyValue, T[]>();
        const limitCount = this.limitCount;
        const offsetCount = this.offsetCount;
        const start = offsetCount > 0 ? offsetCount : 0;
        const max = limitCount === null ? Number.POSITIVE_INFINITY : limitCount;
        if (max <= 0) return result as Map<GroupKey<T, P>, T[]>;

        let matched = 0;
        for (let i = 0; i < this.data.length; i++) {
            const item = this.data[i]!;
            if (!predicate(item)) continue;
            if (matched < start) {
                matched++;
                continue;
            }
            if (matched - start >= max) break;
            matched++;

            forEachResolvedWithSegments(item as ResolveObject, segments, (value) => {
                const key = convert(value);
                if (key === null || key === undefined) return;
                const bucket = result.get(key);
                if (bucket) bucket.push(item);
                else result.set(key, [item]);
            });
        }

        return result as Map<GroupKey<T, P>, T[]>;
    }

    public resultPaginated(options: PaginationOptions): PaginationCursor<T> {
        const safeSize = Number.isFinite(options.pageSize) && options.pageSize > 0
            ? Math.floor(options.pageSize)
            : 1;
        const safePage = Number.isFinite(options.page) && options.page! > 0
            ? Math.floor(options.page!)
            : 1;
        const totalMode: PaginationTotalMode = options.total ?? "none";

        const predicate = this.compile();
        const hasOrder = this.orders.length > 0;
        const startPage = safePage;
        const pageSize = safeSize;

        if (!hasOrder) {
            return this.createCursorUnordered(predicate, pageSize, startPage, totalMode);
        }

        return this.createCursorOrdered(predicate, pageSize, startPage, totalMode);
    }

    public orderBy<P extends SortablePaths<T>>(
        field: P,
        options?: OrderOptions
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const direction = options?.direction === "desc" ? -1 : 1;
        const nullsFirst = options?.nulls === "first";
        const order: OrderSpec = {
            segments,
            direction,
            nullsFirst,
            resolve: this.cache.orderResolver,
        };
        return new FilterEngine(
            this.data,
            this.predicates,
            this.cache,
            [...this.orders, order],
            this.limitCount,
            this.offsetCount
        );
    }

    public orderByDate<P extends DatePaths<T>>(
        field: P,
        options?: OrderOptions
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const direction = options?.direction === "desc" ? -1 : 1;
        const nullsFirst = options?.nulls === "first";
        const order: OrderSpec = {
            segments,
            direction,
            nullsFirst,
            resolve: this.cache.orderResolverDate,
        };
        return new FilterEngine(
            this.data,
            this.predicates,
            this.cache,
            [...this.orders, order],
            this.limitCount,
            this.offsetCount
        );
    }

    public thenBy<P extends SortablePaths<T>>(
        field: P,
        options?: OrderOptions
    ): FilterEngine<T> {
        return this.orderBy(field, options);
    }

    public thenByDate<P extends DatePaths<T>>(
        field: P,
        options?: OrderOptions
    ): FilterEngine<T> {
        return this.orderByDate(field, options);
    }

    public limit(count: number): FilterEngine<T> {
        const safeCount = Number.isFinite(count) ? Math.max(0, Math.floor(count)) : 0;
        return new FilterEngine(
            this.data,
            this.predicates,
            this.cache,
            this.orders,
            safeCount,
            this.offsetCount
        );
    }

    public offset(count: number): FilterEngine<T> {
        const safeCount = Number.isFinite(count) ? Math.max(0, Math.floor(count)) : 0;
        return new FilterEngine(
            this.data,
            this.predicates,
            this.cache,
            this.orders,
            this.limitCount,
            safeCount
        );
    }

    public page(page: number, pageSize: number): FilterEngine<T> {
        const safePage = Number.isFinite(page) && page > 0 ? Math.floor(page) : 1;
        const safeSize = Number.isFinite(pageSize) && pageSize > 0 ? Math.floor(pageSize) : 1;
        const offset = (safePage - 1) * safeSize;
        return new FilterEngine(
            this.data,
            this.predicates,
            this.cache,
            this.orders,
            safeSize,
            offset
        );
    }

    public compile(): Predicate<T> {
        const count = this.predicates.length;
        const predicates = this.predicates;
        if (count === 0) return () => true;
        if (count === 1) return predicates[0]!;
        if (count === 2) {
            const p0 = predicates[0]!;
            const p1 = predicates[1]!;
            return (item) => p0(item) && p1(item);
        }
        if (count === 3) {
            const p0 = predicates[0]!;
            const p1 = predicates[1]!;
            const p2 = predicates[2]!;
            return (item) => p0(item) && p1(item) && p2(item);
        }

        return (item) => {
            for (let i = 0; i < count; i++) {
                if (!predicates[i]!(item)) return false;
            }
            return true;
        };
    }

    public and(
        builder: (q: FilterEngine<T>) => FilterEngine<T>
    ): FilterEngine<T> {
        const group = builder(new FilterEngine(this.data, [], this.cache));
        return this.andPredicate(group.compile());
    }

    public or(
        builder: (q: FilterEngine<T>) => FilterEngine<T>
    ): FilterEngine<T> {
        const groupEngine = builder(new FilterEngine(this.data, [], this.cache));
        if (this.predicates.length === 0) return groupEngine;
        const group = groupEngine.compile();
        const left = this.compile();
        // note: or() collapses prior predicates into one group; keeps semantics simple.
        return new FilterEngine(this.data, [
            (item) => left(item) || group(item)
        ], this.cache, this.orders, this.limitCount, this.offsetCount);
    }

    public not(
        builder: (q: FilterEngine<T>) => FilterEngine<T>
    ): FilterEngine<T> {
        const group = builder(new FilterEngine(this.data, [], this.cache)).compile();
        return this.andPredicate((item) => !group(item));
    }

    public equals<P extends NonDatePaths<T>>(
        field: P,
        value: PathValue<T, P>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const target = value as ResolveValue;
        return this.andPredicate(item =>
            someResolvedWithSegments(
                item as ResolveObject,
                segments,
                (candidate) => candidate === target
            )
        );
    }

    public dateEquals<P extends DatePaths<T>>(
        field: P,
        value: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateEqualsPredicate(this.cache.parseIsoDate, value);
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public notEquals<P extends NonDatePaths<T>>(
        field: P,
        value: PathValue<T, P>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const target = value as ResolveValue;
        return this.andPredicate(item =>
            !someResolvedWithSegments(
                item as ResolveObject,
                segments,
                (candidate) => candidate === target
            )
        );
    }

    public greaterThan<P extends NonDatePaths<T>>(
        field: P,
        value: Extract<PathValue<T, P>, Comparable>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "gt");
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public dateAfter<P extends DatePaths<T>>(
        field: P,
        value: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateComparePredicate(this.cache.parseIsoDate, value, "gt");
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public greaterThanOrEqual<P extends NonDatePaths<T>>(
        field: P,
        value: Extract<PathValue<T, P>, Comparable>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "gte");
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public dateAfterOrEqual<P extends DatePaths<T>>(
        field: P,
        value: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateComparePredicate(this.cache.parseIsoDate, value, "gte");
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public lessThan<P extends NonDatePaths<T>>(
        field: P,
        value: Extract<PathValue<T, P>, Comparable>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "lt");
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public dateBefore<P extends DatePaths<T>>(
        field: P,
        value: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateComparePredicate(this.cache.parseIsoDate, value, "lt");
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public lessThanOrEqual<P extends NonDatePaths<T>>(
        field: P,
        value: Extract<PathValue<T, P>, Comparable>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "lte");
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public dateBeforeOrEqual<P extends DatePaths<T>>(
        field: P,
        value: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateComparePredicate(this.cache.parseIsoDate, value, "lte");
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public between<P extends NonDatePaths<T>>(
        field: P,
        min: Extract<PathValue<T, P>, Comparable>,
        max: Extract<PathValue<T, P>, Comparable>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createBetweenPredicate(min as ResolveValue, max as ResolveValue);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public dateBetween<P extends DatePaths<T>>(
        field: P,
        min: Date | string | number,
        max: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateBetweenPredicate(this.cache.parseIsoDate, min, max);
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public in<P extends NonDatePaths<T>>(
        field: P,
        values: PathValue<T, P>[]
    ): FilterEngine<T> {
        if (values.length === 0) return this.andPredicate(() => false);
        const valueSet = new Set(values as ResolveValue[]);
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(
                item as ResolveObject,
                segments,
                (candidate) => valueSet.has(candidate)
            )
        );
    }

    public notIn<P extends NonDatePaths<T>>(
        field: P,
        values: PathValue<T, P>[]
    ): FilterEngine<T> {
        if (values.length === 0) return this.andPredicate(() => true);
        const valueSet = new Set(values as ResolveValue[]);
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            !someResolvedWithSegments(
                item as ResolveObject,
                segments,
                (candidate) => valueSet.has(candidate)
            )
        );
    }

    public contains<P extends Paths<T>>(
        field: P,
        substring: string,
        ignoreCase = false
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        if (ignoreCase) {
            const target = substring.toLowerCase();
            return this.andPredicate(item =>
                someResolvedWithSegments(item as ResolveObject, segments, (candidate) => {
                    if (typeof candidate !== "string") return false;
                    return candidate.toLowerCase().includes(target);
                })
            );
        }

        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                typeof candidate === "string" && candidate.includes(substring)
            )
        );
    }

    public startsWith<P extends Paths<T>>(
        field: P,
        prefix: string,
        ignoreCase = false
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        if (ignoreCase) {
            const target = prefix.toLowerCase();
            return this.andPredicate(item =>
                someResolvedWithSegments(item as ResolveObject, segments, (candidate) => {
                    if (typeof candidate !== "string") return false;
                    return candidate.toLowerCase().startsWith(target);
                })
            );
        }

        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                typeof candidate === "string" && candidate.startsWith(prefix)
            )
        );
    }

    public endsWith<P extends Paths<T>>(
        field: P,
        suffix: string,
        ignoreCase = false
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        if (ignoreCase) {
            const target = suffix.toLowerCase();
            return this.andPredicate(item =>
                someResolvedWithSegments(item as ResolveObject, segments, (candidate) => {
                    if (typeof candidate !== "string") return false;
                    return candidate.toLowerCase().endsWith(target);
                })
            );
        }

        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                typeof candidate === "string" && candidate.endsWith(suffix)
            )
        );
    }

    public matches<P extends Paths<T>>(
        field: P,
        regex: RegExp
    ): FilterEngine<T> {
        const safeRegex = new RegExp(regex.source, regex.flags.replace(/[gy]/g, ""));
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                typeof candidate === "string" && safeRegex.test(candidate)
            )
        );
    }

    public isNull<P extends Paths<T>>(field: P): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) => candidate === null)
        );
    }

    public valueNotNull<P extends Paths<T>>(field: P): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) => candidate != null)
        );
    }

    public pathExists<P extends Paths<T>>(field: P): FilterEngine<T> {
        const path = String(field);
        const segments = getSegments(this.cache, path);
        return this.andPredicate(item => {
            // note: path existence ignores null/undefined values.
            return pathExistsWithSegments(item as ResolveObject, segments);
        });
    }

    public pathExistsNullable<P extends Paths<T>>(field: P): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) => candidate !== undefined)
        );
    }


    public arraySome<P extends Paths<T>>(
        field: P,
        predicate: (value: PathValue<T, P>) => boolean
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                predicate(candidate as PathValue<T, P>)
            )
        );
    }

    public arrayEvery<P extends Paths<T>>(
        field: P,
        predicate: (value: PathValue<T, P>) => boolean
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            everyResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                predicate(candidate as PathValue<T, P>)
            )
        );
    }

    public arrayNone<P extends Paths<T>>(
        field: P,
        predicate: (value: PathValue<T, P>) => boolean
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            !someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                predicate(candidate as PathValue<T, P>)
            )
        );
    }

    public nested<P extends ArrayPaths<T>>(
        field: P,
        builder: (q: FilterEngine<ArrayPathItem<T, P>>) => FilterEngine<ArrayPathItem<T, P>>
    ): FilterEngine<T> {
        const nestedPredicate = builder(
            new FilterEngine([] as ArrayPathItem<T, P>[], [], this.cache)
        ).compile();
        const segments = getSegments(this.cache, String(field));

        return this.andPredicate(item => {
            return someResolvedWithSegments(item as ResolveObject, segments, (candidate) => {
                if (Array.isArray(candidate)) {
                    for (let i = 0; i < candidate.length; i++) {
                        if (nestedPredicate(candidate[i] as ArrayPathItem<T, P>)) return true;
                    }
                    return false;
                }
                if (candidate && typeof candidate === "object") {
                    return nestedPredicate(candidate as ArrayPathItem<T, P>);
                }
                return false;
            });
        });
    }

    public custom(predicate: Predicate<T>): FilterEngine<T> {
        return this.andPredicate(predicate);
    }
}

export type FilterEngineClassType = typeof FilterEngine;
