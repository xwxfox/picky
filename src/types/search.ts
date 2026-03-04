import type { Paths, PathValue } from "./paths";

export type SearchCapabilityState<Tags extends string = string> = {
    fuzzy: boolean;
    tagger: boolean;
    tags: Tags;
};

export type AnySearchCapabilityState = SearchCapabilityState<string>;

export type DefaultSearchCapabilityState = {
    fuzzy: false;
    tagger: false;
    tags: never;
};

export type WithFuzzy<C extends SearchCapabilityState> = Omit<C, "fuzzy"> & { fuzzy: true };

export type WithTagger<C extends SearchCapabilityState, Tags extends string> = Omit<C, "tagger" | "tags"> & {
    tagger: true;
    tags: Tags;
};

export type MergeSearchCapability<A extends SearchCapabilityState, B extends SearchCapabilityState> = {
    fuzzy: A["fuzzy"] extends true ? true : B["fuzzy"] extends true ? true : false;
    tagger: A["tagger"] extends true ? true : B["tagger"] extends true ? true : false;
    tags: A["tags"] | B["tags"];
};

export type AvailableTags<C extends SearchCapabilityState> = C["tagger"] extends true ? C["tags"] : never;

export type FuzzyPaths<T> = {
    [P in Paths<T>]: Extract<PathValue<T, P>, string> extends never ? never : P
}[Paths<T>];

export type FuzzyField<T> = {
    path: FuzzyPaths<T>;
    weight?: number;
};

export type FuzzyOrder = "score" | "existing" | "scoreThenOrder";

export type FuzzyQueryInput = string | { minScore?: number; query: string; };

export type FuzzyConfig<T> = {
    fields: ReadonlyArray<FuzzyField<T>>;
    minScore?: number;
    normalize?: (value: string) => string;
    order?: FuzzyOrder;
    requireAll?: boolean;
    strict?: boolean;
};

export type TagFilter<Tags extends string> = {
    has?: ReadonlyArray<Tags>;
    hasAny?: ReadonlyArray<Tags>;
    not?: ReadonlyArray<Tags>;
    notAny?: ReadonlyArray<Tags>;
};

export type TaggerEqualsRule<T, Tags extends string> = {
    [P in Paths<T>]: { equals: PathValue<T, P>; field: P; tag: Tags; }
}[Paths<T>];

export type TaggerInRule<T, Tags extends string> = {
    [P in Paths<T>]: { field: P; in: Array<PathValue<T, P>>; tag: Tags; }
}[Paths<T>];

export type TaggerContainsRule<T, Tags extends string> = {
    [P in FuzzyPaths<T>]: { contains: string; field: P; tag: Tags; }
}[FuzzyPaths<T>];

export type TaggerStartsWithRule<T, Tags extends string> = {
    [P in FuzzyPaths<T>]: { field: P; startsWith: string; tag: Tags; }
}[FuzzyPaths<T>];

export type TaggerEndsWithRule<T, Tags extends string> = {
    [P in FuzzyPaths<T>]: { endsWith: string; field: P; tag: Tags; }
}[FuzzyPaths<T>];

export type TaggerMatchesRule<T, Tags extends string> = {
    [P in FuzzyPaths<T>]: { field: P; matches: RegExp; tag: Tags; }
}[FuzzyPaths<T>];

export type TaggerRule<T, Tags extends string> =
    | TaggerEqualsRule<T, Tags>
    | TaggerInRule<T, Tags>
    | TaggerContainsRule<T, Tags>
    | TaggerStartsWithRule<T, Tags>
    | TaggerEndsWithRule<T, Tags>
    | TaggerMatchesRule<T, Tags>;

export type TaggerConfig<T, Tags extends string> = {
    normalize?: (value: string) => string;
    rules: ReadonlyArray<TaggerRule<T, Tags>>;
    strict?: boolean;
    tags: ReadonlyArray<Tags>;
};

export type SearchQuery<Tags extends string> = {
    fuzzy?: FuzzyQueryInput;
    tags?: TagFilter<Tags>;
};

export type SearchFilterState = {
    fuzzy?: FuzzyQueryInput;
    tags?: TagFilter<string>;
};

type SearchOptions<C extends SearchCapabilityState> =
    (C["fuzzy"] extends true ? { fuzzy?: FuzzyQueryInput } : {})
    & (C["tagger"] extends true ? { tags?: TagFilter<AvailableTags<C>> } : {});

type HasSearch<C extends SearchCapabilityState> =
    C["fuzzy"] extends true ? true : C["tagger"] extends true ? true : false;

export type SearchInput<C extends SearchCapabilityState> =
    HasSearch<C> extends true
    ? | (C["fuzzy"] extends true ? string : never)
    | (C["tagger"] extends true ? Array<AvailableTags<C>> : never)
    | SearchOptions<C>
    : never;
