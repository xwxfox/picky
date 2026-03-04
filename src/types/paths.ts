import type { ResolveObject, Comparable, GroupableValue } from "./core";

type Prev = [never, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

export type PathValue<T, P extends string, SeenArray extends boolean = false, Depth extends number = 5> =
    Depth extends 0
    ? never
    : P extends `${infer K}.${infer Rest}`
    ? K extends keyof T
    ? T[K] extends ReadonlyArray<infer U>
    ? SeenArray extends true
    ? never
    : PathValue<NonNullable<U>, Rest, true, Prev[Depth]>
    : PathValue<NonNullable<T[K]>, Rest, SeenArray, Prev[Depth]>
    : never
    : P extends keyof T
    ? T[P] extends ReadonlyArray<infer U>
    ? SeenArray extends true
    ? never
    : U
    : T[P]
    : never;

export type Path<T, P extends string> = PathValue<T, P> extends never ? never : P;

export type ArrayPathValue<T, P extends string, SeenArray extends boolean = false> =
    P extends `${infer K}.${infer Rest}`
    ? K extends keyof T
    ? T[K] extends ReadonlyArray<infer U>
    ? SeenArray extends true
    ? never
    : ArrayPathValue<NonNullable<U>, Rest, true>
    : ArrayPathValue<NonNullable<T[K]>, Rest, SeenArray>
    : never
    : P extends keyof T
    ? T[P] extends ReadonlyArray<infer U>
    ? SeenArray extends true
    ? never
    : U
    : never
    : never;

export type ArrayPath<T, P extends string> = ArrayPathValue<T, P> extends never ? never : P;

export type ArrayPathItem<T, P extends string> = Extract<ArrayPathValue<T, P>, ResolveObject>;

export type ArrayItem<T> = T extends ReadonlyArray<infer U> ? U : never;

export type Paths<T, SeenArray extends boolean = false, Depth extends number = 5> = Depth extends 0
    ? never
    : {
        [K in keyof T & string]:
        T[K] extends ReadonlyArray<infer U>
        ? SeenArray extends true
        ? never
        : K | `${K}.${Paths<NonNullable<U>, true, Prev[Depth]>}`
        : NonNullable<T[K]> extends ResolveObject
        ? K | `${K}.${Paths<NonNullable<T[K]>, SeenArray, Prev[Depth]>}`
        : K
    }[keyof T & string];

export type NonDatePaths<T> = {
    [P in Paths<T>]: Extract<PathValue<T, P>, Date> extends never ? P : never
}[Paths<T>];

export type DatePaths<T> = {
    [P in Paths<T>]: Extract<PathValue<T, P>, Date | string | number> extends never ? never : P
}[Paths<T>];

export type SortablePaths<T> = {
    [P in Paths<T>]: Extract<PathValue<T, P>, Comparable> extends never ? never : P
}[Paths<T>];

export type GroupablePaths<T> = {
    [P in Paths<T>]: Extract<PathValue<T, P>, GroupableValue> extends never ? never : P
}[Paths<T>];

export type NonNullablePathValue<T, P extends string> = Exclude<PathValue<T, P>, null | undefined>;

export type GroupKey<T, P extends string> = NonNullablePathValue<T, P> extends infer U
    ? U extends Date
    ? number
    : U
    : never;

export type ArrayPaths<T, SeenArray extends boolean = false, Depth extends number = 5> = Depth extends 0
    ? never
    : {
        [K in keyof T & string]:
        T[K] extends ReadonlyArray<infer U>
        ? SeenArray extends true
        ? never
        : K | `${K}.${Paths<NonNullable<U>, true, Prev[Depth]>}`
        : NonNullable<T[K]> extends ResolveObject
        ? `${K}.${ArrayPaths<NonNullable<T[K]>, SeenArray, Prev[Depth]>}`
        : never
    }[keyof T & string];
