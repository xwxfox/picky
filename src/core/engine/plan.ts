import type { Predicate, ResolveValue } from "@/types";
import type { PushdownOrder, PushdownPredicate } from "@/io/ingress/adapters/pushdown";
import type { CacheState } from "@/core/shared/cache";
import type { PathAccessors } from "@/core/shared/path";
import type { CompiledFuzzyConfig, CompiledTaggerConfig } from "@/core/search/runtime";
import type { SearchFilterState } from "@/types/search";

export type PredicateKind = "builtin" | "custom" | "group" | "nested" | "const";

export type PredicateOp =
    | "eq"
    | "ne"
    | "gt"
    | "gte"
    | "lt"
    | "lte"
    | "between"
    | "in"
    | "notIn"
    | "contains"
    | "startsWith"
    | "endsWith"
    | "matches"
    | "isNull"
    | "notNull"
    | "pathExists"
    | "pathExistsNullable"
    | "arraySome"
    | "arrayEvery"
    | "arrayNone"
    | "dateEquals"
    | "dateAfter"
    | "dateAfterOrEqual"
    | "dateBefore"
    | "dateBeforeOrEqual"
    | "dateBetween"
    | "groupAnd"
    | "groupOr"
    | "groupNot"
    | "nested"
    | "custom"
    | "const";

export type PredicateSpec<T> = {
    accessors?: PathAccessors;
    constantValue?: boolean;
    cost?: number;
    field?: string;
    groupInfo?: {
        estimatedCost?: number;
        estimatedSelectivity?: number;
        planId?: string;
        predicateCount?: number;
        reorderable?: boolean;
    };
    id: string;
    ignoreCase?: boolean;
    kind: PredicateKind;
    notes?: Array<string>;
    op: PredicateOp;
    origin?: "builder" | "chain" | "group" | "nested";
    predicate: Predicate<T>;
    pushdown?: PushdownPredicate;
    reorderable: boolean;
    selectivity?: number;
    value?: unknown;
    valueSet?: Set<ResolveValue>;
};

export type PredicateDiagnostic = {
    cost: number;
    field?: string;
    id: string;
    ignoreCase?: boolean;
    op: PredicateOp;
    pushdown: boolean;
    reorderable: boolean;
    selectivity: number;
    value?: unknown;
};

export type PlannerDiagnostics = {
    final: {
        alwaysFalse: boolean;
        predicates: Array<PredicateDiagnostic>;
        pushdownPredicates: Array<PredicateDiagnostic>;
        residualPredicates: Array<PredicateDiagnostic>;
    };
    input: Array<PredicateDiagnostic>;
    merges: Array<{ reason: string; removed: Array<string>; result: Array<string>; }>;
    order: Array<{ after: Array<string>; before: Array<string>; reason: string; }>;
    pushdown: {
        applied: Array<string>;
        candidates: Array<string>;
        full: boolean;
        residual: Array<string>;
    };
};

export type PlanningMeta = {
    alwaysFalse: boolean;
    estimatedCost: number;
    estimatedSelectivity: number;
    predicateCount: number;
    pushdownCount: number;
    reorderable: boolean;
    residualCount: number;
};

export type QueryPlan<T> = {
    alwaysFalse?: boolean;
    cache: CacheState;
    diagnostics?: PlannerDiagnostics;
    fuzzyConfig: CompiledFuzzyConfig<T> | null;
    id: string;
    planning?: PlanningMeta;
    predicates: Array<Predicate<T>>;
    predicateSpecs?: Array<PredicateSpec<T>>;
    predicateFn: (item: T) => boolean;
    pushdownOrders: Array<PushdownOrder>;
    pushdownPredicates: Array<PushdownPredicate>;
    pushdownSafe: boolean;
    residualPredicateFn?: (item: T) => boolean;
    residualPredicates?: Array<Predicate<T>>;
    searchFilters: Array<SearchFilterState>;
    strictSearch: boolean;
    taggerConfig: CompiledTaggerConfig<T, string> | null;
};
