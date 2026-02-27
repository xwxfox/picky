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
        if (cached !== undefined || cache.has(value)) return cached ?? null;

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

    return {
        maxDateCache: options.maxDateCache,
        maxPathCache: options.maxPathCache,
        pathSegmentsCache,
        dateCache,
        parseIsoDate,
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

// note: option 3 (stricter compile-time shape) example:
// type Resolvable<T> = T extends object ? { [K in keyof T]: Resolvable<T[K]> } : T;
// pros: catches non-resolvable shapes early; cons: heavy on TS for deep/recursive types.
export class FilterEngine<T extends Record<string, unknown>> {
    private constructor(
        private readonly data: readonly T[],
        private readonly predicates: readonly Predicate<T>[],
        private readonly cache: CacheState
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
        return new FilterEngine(this.data, [...this.predicates, predicate], this.cache);
    }

    private andPredicate(condition: Predicate<T>): FilterEngine<T> {
        return this.withPredicate(condition);
    }

    public result(): T[] {
        const predicate = this.compile();
        return this.data.filter(predicate);
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
        ], this.cache);
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
                    return (candidate as ArrayPathItem<T, P>[]).some(nestedPredicate);
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
