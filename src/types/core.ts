export type Predicate<T> = (item: T) => boolean;

export type ScalarValue =
    | string
    | number
    | boolean
    | bigint
    | symbol
    | null
    | undefined
    | Date;

export type Comparable =
    | number
    | string
    | bigint
    | Date;

export type SortKey = number | string | bigint;

export type OrderDirection = "asc" | "desc";
export type NullOrder = "first" | "last";

export type GroupableValue =
    | string
    | number
    | boolean
    | bigint
    | symbol
    | Date;

export type GroupKeyValue =
    | string
    | number
    | boolean
    | bigint
    | symbol;

export type ResolveValue =
    | ScalarValue
    | { [key: string]: ResolveValue }
    | ResolveValue[];

export type ResolveObject = Record<string, ResolveValue>;

export type OrderOptions = {
    direction?: OrderDirection;
    nulls?: NullOrder;
};

export type PaginationTotalMode = "none" | "lazy" | "full";

export type PaginationOptions = {
    page?: number;
    pageSize: number;
    total?: PaginationTotalMode;
};

export type PaginationCursor<T> = {
    data: Array<T>;
    next: () => PaginationCursor<T>;
    page: number;
    previous: () => PaginationCursor<T>;
    total?: number;
};

export type CacheOptions = {
    maxDateCache: number;
    maxPathCache: number;
};

export type ResolvePredicate = (value: ResolveValue) => boolean;

export type ResolveSortKey = (value: ResolveValue) => SortKey | null;

export type OrderSpec = {
    direction: 1 | -1;
    nullsFirst: boolean;
    resolve: ResolveSortKey;
    segments: Array<string>;
};

export type GroupSpec = {
    convert: (value: ResolveValue) => GroupKeyValue | null;
    segments: Array<string>;
};
