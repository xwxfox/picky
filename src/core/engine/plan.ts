import type { Predicate } from "@/types";
import type { CacheState } from "@/core/shared/cache";
import type { CompiledFuzzyConfig, CompiledTaggerConfig } from "@/core/search/runtime";
import type { SearchFilterState } from "@/types/search";

export type QueryPlan<T> = {
    cache: CacheState;
    fuzzyConfig: CompiledFuzzyConfig<T> | null;
    id: string;
    predicates: Array<Predicate<T>>;
    searchFilters: Array<SearchFilterState>;
    strictSearch: boolean;
    taggerConfig: CompiledTaggerConfig<T, string> | null;
};
