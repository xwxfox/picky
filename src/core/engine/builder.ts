import type {
    Predicate,
    ResolveObject,
    ResolveValue,
    Comparable,
    PathValue,
    Paths,
    NonDatePaths,
    DatePaths,
    ArrayPathItem,
    ArrayPaths,
    OrderOptions,
    SortablePaths,
} from "@/types";
import type { QueryChain } from "@/core/engine/chains/chain";
import { hashPlanId } from "./hash";
import { getPathAccessors } from "@/core/shared/cache";
import type { PathAccessors } from "@/core/shared/path";
import { createComparePredicate } from "@/core/engine/predicates/compare";
import { createBetweenPredicate } from "@/core/engine/predicates/range";
import { createDateEqualsPredicate, createDateComparePredicate, createDateBetweenPredicate } from "@/core/engine/predicates/dates";
import type { CacheState } from "@/core/shared/cache";
import type { QueryPlan, PredicateSpec } from "./plan";
import { optimizePlan } from "./planner";
import { EgressEngine } from "@/io/egress";
import { IngressEngine, AsyncIngressEngine } from "@/io/ingress";
import { Schema } from "@/io/schema";
import type {
    SearchCapabilityState,
    DefaultSearchCapabilityState,
    WithFuzzy,
    WithTagger,
    SearchInput,
    TagFilter,
    FuzzyConfig,
    TaggerConfig,
    AvailableTags,
    SearchFilterState,
} from "@/types/search";
import {
    compileFuzzyConfig,
    compileTaggerConfig,
    resolveSearchQuery,
} from "@/core/search/runtime";
import type {
    CompiledFuzzyConfig,
    CompiledTaggerConfig,
} from "@/core/search/runtime";
import type { PushdownOrder, PushdownPredicate } from "@/io/ingress/adapters/pushdown";

export type QueryBuilderState<T> = {
    cache: CacheState;
    fuzzyConfig: CompiledFuzzyConfig<T> | null;
    predicates: Array<Predicate<T>>;
    predicateSpecs: Array<PredicateSpec<T>>;
    pushdownOrders: Array<PushdownOrder>;
    pushdownPredicates: Array<PushdownPredicate>;
    pushdownSafe: boolean;
    searchFilters: Array<SearchFilterState>;
    strictSearch: boolean;
    taggerConfig: CompiledTaggerConfig<T, string> | null;
};

export class QueryBuilder<
    T extends Record<string, unknown>,
    C extends SearchCapabilityState = DefaultSearchCapabilityState,
    M extends "sync" | "async" = "sync"
> {
    private static readonly alwaysFalse = () => false;
    private static readonly alwaysTrue = () => true;
    private constructor(
        private readonly ingress: M extends "async" ? AsyncIngressEngine<T> : IngressEngine<T>,
        private readonly state: QueryBuilderState<T>
    ) { }

    static from<T extends Record<string, unknown>>(ingress: IngressEngine<T>): QueryBuilder<T, DefaultSearchCapabilityState, "sync"> {
        return new QueryBuilder<T, DefaultSearchCapabilityState, "sync">(ingress, {
            cache: ingress.cache,
            fuzzyConfig: null,
            predicates: [],
            predicateSpecs: [],
            pushdownOrders: [],
            pushdownPredicates: [],
            pushdownSafe: true,
            searchFilters: [],
            strictSearch: true,
            taggerConfig: null,
        });
    }

    static fromAsync<T extends Record<string, unknown>>(
        ingress: AsyncIngressEngine<T>
    ): QueryBuilder<T, DefaultSearchCapabilityState, "async"> {
        return new QueryBuilder<T, DefaultSearchCapabilityState, "async">(ingress, {
            cache: ingress.cache,
            fuzzyConfig: null,
            predicates: [],
            predicateSpecs: [],
            pushdownOrders: [],
            pushdownPredicates: [],
            pushdownSafe: true,
            searchFilters: [],
            strictSearch: true,
            taggerConfig: null,
        });
    }

    private addPredicate(predicate: Predicate<T>, spec?: PredicateSpec<T>): QueryBuilder<T, C, M> {
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicate],
            predicateSpecs: spec ? [...this.state.predicateSpecs, spec] : this.state.predicateSpecs,
        });
    }

    private addPushdownPredicate(predicate: PushdownPredicate): QueryBuilder<T, C, M> {
        return new QueryBuilder(this.ingress, {
            ...this.state,
            pushdownPredicates: [...this.state.pushdownPredicates, predicate],
            predicateSpecs: [...this.state.predicateSpecs],
        });
    }

    private getAccessors(path: string): PathAccessors {
        return getPathAccessors(this.state.cache, path);
    }

    private addPushdownOrder(order: PushdownOrder): QueryBuilder<T, C, M> {
        return new QueryBuilder(this.ingress, {
            ...this.state,
            pushdownOrders: [...this.state.pushdownOrders, order],
            predicateSpecs: [...this.state.predicateSpecs],
        });
    }

    private createPredicateSpec(
        params: Omit<PredicateSpec<T>, "id">
    ): PredicateSpec<T> {
        const index = this.state.predicateSpecs.length + 1;
        return {
            ...params,
            id: `p${index}`,
        };
    }

    use(chain: QueryChain<T>): QueryBuilder<T, C, M> {
        const plan = chain.getPlan();
        const predicate = (item: T) => {
            for (let i = 0; i < plan.predicates.length; i++) {
                if (!plan.predicates[i]!(item)) {return false;}
            }
            return true;
        };
        const spec = this.createPredicateSpec({
            kind: "custom",
            op: "custom",
            predicate,
            reorderable: false,
            origin: "chain",
            notes: [plan.id],
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicate],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    compilePlan(): QueryPlan<T> {
        const searchKey = `${this.state.fuzzyConfig ? "f" : "_"}${this.state.taggerConfig ? "t" : "_"}${this.state.searchFilters.length}`;
        const id = hashPlanId(this.state.predicates, searchKey);
        return optimizePlan({
            cache: this.state.cache,
            fuzzyConfig: this.state.fuzzyConfig,
            hints: this.ingress.hints,
            id,
            predicates: this.state.predicates,
            predicateSpecs: this.state.predicateSpecs,
            pushdownOrders: this.state.pushdownOrders,
            pushdownPredicates: this.state.pushdownPredicates,
            pushdownSafe: this.state.pushdownSafe,
            searchFilters: this.state.searchFilters,
            strictSearch: this.state.strictSearch,
            taggerConfig: this.state.taggerConfig,
        });
    }

    out(): EgressEngine<T, C, M> {
        const plan = this.compilePlan();
        const ingress = this.ingress;
        if (ingress instanceof Object && "mode" in ingress && ingress.mode === "async") {
            return EgressEngine.fromAsync<T, C>(ingress as AsyncIngressEngine<T>, plan) as EgressEngine<T, C, M>;
        }
        return EgressEngine.from<T, C>(ingress as IngressEngine<T>, plan) as EgressEngine<T, C, M>;
    }

    configureFuzzy(config: FuzzyConfig<T>): QueryBuilder<T, WithFuzzy<C>, M> {
        const compiled = compileFuzzyConfig(this.state.cache, config);
        return new QueryBuilder<T, WithFuzzy<C>, M>(this.ingress, {
            ...this.state,
            fuzzyConfig: compiled,
            predicateSpecs: [...this.state.predicateSpecs],
            pushdownSafe: false,
        });
    }

    configureTagger<Tags extends string>(config: TaggerConfig<T, Tags>): QueryBuilder<T, WithTagger<C, Tags>, M> {
        const compiled = compileTaggerConfig(this.state.cache, config);
        return new QueryBuilder<T, WithTagger<C, Tags>, M>(this.ingress, {
            ...this.state,
            pushdownSafe: false,
            predicateSpecs: [...this.state.predicateSpecs],
            taggerConfig: compiled,
        });
    }

    search(input: SearchInput<C>): QueryBuilder<T, C, M> {
        const emptyTagFilter: TagFilter<AvailableTags<C>> = { hasAny: [] };
        if (!this.state.fuzzyConfig && !this.state.taggerConfig) {
            if (this.state.strictSearch) { throw new Error("search() used without configureFuzzy/configureTagger."); }
            return new QueryBuilder(this.ingress, {
                ...this.state,
                pushdownSafe: false,
                predicateSpecs: [...this.state.predicateSpecs],
                searchFilters: [...this.state.searchFilters, { tags: emptyTagFilter }],
            });
        }
        const filter = resolveSearchQuery(input);
        return new QueryBuilder(this.ingress, {
            ...this.state,
            pushdownSafe: false,
            predicateSpecs: [...this.state.predicateSpecs],
            searchFilters: [...this.state.searchFilters, filter],
        });
    }

    tags(filter: TagFilter<AvailableTags<C>>): QueryBuilder<T, C, M> {
        const emptyTagFilter: TagFilter<AvailableTags<C>> = { hasAny: [] };
        if (!this.state.taggerConfig) {
            if (this.state.strictSearch) { throw new Error("tags() used without configureTagger."); }
            return new QueryBuilder(this.ingress, {
                ...this.state,
                pushdownSafe: false,
                predicateSpecs: [...this.state.predicateSpecs],
                searchFilters: [...this.state.searchFilters, { tags: emptyTagFilter }],
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            pushdownSafe: false,
            predicateSpecs: [...this.state.predicateSpecs],
            searchFilters: [...this.state.searchFilters, { tags: filter }],
        });
    }

    // Filter operators
    equals<P extends NonDatePaths<T>>(field: P, value: PathValue<T, P>): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const target = value as ResolveValue;
        const pushdown = { field: fieldKey, op: "eq", value: target } as const;
        const predicate = (item: T) => accessors.some(item as ResolveObject, (c) => c === target);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "eq",
            predicate,
            pushdown,
            reorderable: true,
            value: target,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
    }

    notEquals<P extends NonDatePaths<T>>(field: P, value: PathValue<T, P>): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const target = value as ResolveValue;
        const pushdown = { field: fieldKey, op: "ne", value: target } as const;
        const predicate = (item: T) => !accessors.some(item as ResolveObject, (c) => c === target);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "ne",
            predicate,
            pushdown,
            reorderable: true,
            value: target,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
    }

    greaterThan<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = createComparePredicate(value as ResolveValue, "gt");
        const pushdown = { field: fieldKey, op: "gt", value: value as ResolveValue } as const;
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, predicate);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "gt",
            predicate: predicateFn,
            pushdown,
            reorderable: true,
            value: value as ResolveValue,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicateFn, spec);
    }

    greaterThanOrEqual<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = createComparePredicate(value as ResolveValue, "gte");
        const pushdown = { field: fieldKey, op: "gte", value: value as ResolveValue } as const;
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, predicate);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "gte",
            predicate: predicateFn,
            pushdown,
            reorderable: true,
            value: value as ResolveValue,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicateFn, spec);
    }

    lessThan<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = createComparePredicate(value as ResolveValue, "lt");
        const pushdown = { field: fieldKey, op: "lt", value: value as ResolveValue } as const;
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, predicate);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "lt",
            predicate: predicateFn,
            pushdown,
            reorderable: true,
            value: value as ResolveValue,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicateFn, spec);
    }

    lessThanOrEqual<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = createComparePredicate(value as ResolveValue, "lte");
        const pushdown = { field: fieldKey, op: "lte", value: value as ResolveValue } as const;
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, predicate);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "lte",
            predicate: predicateFn,
            pushdown,
            reorderable: true,
            value: value as ResolveValue,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicateFn, spec);
    }

    between<P extends NonDatePaths<T>>(field: P, min: Extract<PathValue<T, P>, Comparable>, max: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = createBetweenPredicate(min as ResolveValue, max as ResolveValue);
        const rangeValue = { max: max as ResolveValue, min: min as ResolveValue };
        const pushdown = { field: fieldKey, op: "between", value: rangeValue } as const;
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, predicate);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "between",
            predicate: predicateFn,
            pushdown,
            reorderable: true,
            value: rangeValue,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicateFn, spec);
    }

    in<P extends NonDatePaths<T>>(field: P, values: Array<PathValue<T, P>>): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        if (values.length === 0) {
            const pushdown = { field: fieldKey, op: "in", value: [] } as const;
            const predicate = QueryBuilder.alwaysFalse;
            const spec = this.createPredicateSpec({
                accessors,
                constantValue: false,
                field: fieldKey,
                kind: "const",
                op: "const",
                predicate,
                pushdown,
                reorderable: true,
                value: [],
            });
            return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
        }
        const valueSet = new Set(values as Array<ResolveValue>);
        const pushdown = { field: fieldKey, op: "in", value: values as Array<ResolveValue> } as const;
        const predicate = (item: T) => accessors.some(item as ResolveObject, (c) => valueSet.has(c));
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "in",
            predicate,
            pushdown,
            reorderable: true,
            value: values as Array<ResolveValue>,
            valueSet,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
    }

    notIn<P extends NonDatePaths<T>>(field: P, values: Array<PathValue<T, P>>): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        if (values.length === 0) {
            const pushdown = { field: fieldKey, op: "notIn", value: [] } as const;
            const predicate = QueryBuilder.alwaysTrue;
            const spec = this.createPredicateSpec({
                accessors,
                constantValue: true,
                field: fieldKey,
                kind: "const",
                op: "const",
                predicate,
                pushdown,
                reorderable: true,
                value: [],
            });
            return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
        }
        const valueSet = new Set(values as Array<ResolveValue>);
        const pushdown = { field: fieldKey, op: "notIn", value: values as Array<ResolveValue> } as const;
        const predicate = (item: T) => !accessors.some(item as ResolveObject, (c) => valueSet.has(c));
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "notIn",
            predicate,
            pushdown,
            reorderable: true,
            value: values as Array<ResolveValue>,
            valueSet,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
    }

    dateEquals<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = createDateEqualsPredicate(this.state.cache.parseIsoDate, value);
        if (!predicate) {
            const predicateFn = QueryBuilder.alwaysFalse;
            const spec = this.createPredicateSpec({
                accessors,
                constantValue: false,
                field: fieldKey,
                kind: "const",
                op: "const",
                predicate: predicateFn,
                reorderable: true,
                value,
            });
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, predicateFn],
                predicateSpecs: [...this.state.predicateSpecs, spec],
                pushdownSafe: false,
            });
        }
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, predicate);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "dateEquals",
            predicate: predicateFn,
            reorderable: true,
            value,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    dateAfter<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "gt");
        if (!predicate) {
            const predicateFn = QueryBuilder.alwaysFalse;
            const spec = this.createPredicateSpec({
                accessors,
                constantValue: false,
                field: fieldKey,
                kind: "const",
                op: "const",
                predicate: predicateFn,
                reorderable: true,
                value,
            });
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, predicateFn],
                predicateSpecs: [...this.state.predicateSpecs, spec],
                pushdownSafe: false,
            });
        }
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, predicate);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "dateAfter",
            predicate: predicateFn,
            reorderable: true,
            value,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    dateAfterOrEqual<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "gte");
        if (!predicate) {
            const predicateFn = QueryBuilder.alwaysFalse;
            const spec = this.createPredicateSpec({
                accessors,
                constantValue: false,
                field: fieldKey,
                kind: "const",
                op: "const",
                predicate: predicateFn,
                reorderable: true,
                value,
            });
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, predicateFn],
                predicateSpecs: [...this.state.predicateSpecs, spec],
                pushdownSafe: false,
            });
        }
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, predicate);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "dateAfterOrEqual",
            predicate: predicateFn,
            reorderable: true,
            value,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    dateBefore<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "lt");
        if (!predicate) {
            const predicateFn = QueryBuilder.alwaysFalse;
            const spec = this.createPredicateSpec({
                accessors,
                constantValue: false,
                field: fieldKey,
                kind: "const",
                op: "const",
                predicate: predicateFn,
                reorderable: true,
                value,
            });
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, predicateFn],
                predicateSpecs: [...this.state.predicateSpecs, spec],
                pushdownSafe: false,
            });
        }
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, predicate);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "dateBefore",
            predicate: predicateFn,
            reorderable: true,
            value,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    dateBeforeOrEqual<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "lte");
        if (!predicate) {
            const predicateFn = QueryBuilder.alwaysFalse;
            const spec = this.createPredicateSpec({
                accessors,
                constantValue: false,
                field: fieldKey,
                kind: "const",
                op: "const",
                predicate: predicateFn,
                reorderable: true,
                value,
            });
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, predicateFn],
                predicateSpecs: [...this.state.predicateSpecs, spec],
                pushdownSafe: false,
            });
        }
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, predicate);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "dateBeforeOrEqual",
            predicate: predicateFn,
            reorderable: true,
            value,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    dateBetween<P extends DatePaths<T>>(field: P, min: Date | string | number, max: Date | string | number): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = createDateBetweenPredicate(this.state.cache.parseIsoDate, min, max);
        if (!predicate) {
            const predicateFn = QueryBuilder.alwaysFalse;
            const spec = this.createPredicateSpec({
                accessors,
                constantValue: false,
                field: fieldKey,
                kind: "const",
                op: "const",
                predicate: predicateFn,
                reorderable: true,
                value: { max, min },
            });
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, predicateFn],
                predicateSpecs: [...this.state.predicateSpecs, spec],
                pushdownSafe: false,
            });
        }
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, predicate);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "dateBetween",
            predicate: predicateFn,
            reorderable: true,
            value: { max, min },
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    contains<P extends Paths<T>>(field: P, substring: string, ignoreCase = false): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        if (ignoreCase) {
            const target = substring.toLowerCase();
            const pushdown = { field: fieldKey, ignoreCase, op: "contains", value: substring } as const;
            const predicate = (item: T) => accessors.some(item as ResolveObject, (c) =>
                typeof c === "string" && c.toLowerCase().includes(target)
            );
            const spec = this.createPredicateSpec({
                accessors,
                field: fieldKey,
                ignoreCase,
                kind: "builtin",
                op: "contains",
                predicate,
                pushdown,
                reorderable: true,
                value: substring,
            });
            return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
        }
        const pushdown = { field: fieldKey, ignoreCase, op: "contains", value: substring } as const;
        const predicate = (item: T) => accessors.some(item as ResolveObject, (c) =>
            typeof c === "string" && c.includes(substring)
        );
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            ignoreCase,
            kind: "builtin",
            op: "contains",
            predicate,
            pushdown,
            reorderable: true,
            value: substring,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
    }

    startsWith<P extends Paths<T>>(field: P, prefix: string, ignoreCase = false): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        if (ignoreCase) {
            const target = prefix.toLowerCase();
            const pushdown = { field: fieldKey, ignoreCase, op: "startsWith", value: prefix } as const;
            const predicate = (item: T) => accessors.some(item as ResolveObject, (c) =>
                typeof c === "string" && c.toLowerCase().startsWith(target)
            );
            const spec = this.createPredicateSpec({
                accessors,
                field: fieldKey,
                ignoreCase,
                kind: "builtin",
                op: "startsWith",
                predicate,
                pushdown,
                reorderable: true,
                value: prefix,
            });
            return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
        }
        const pushdown = { field: fieldKey, ignoreCase, op: "startsWith", value: prefix } as const;
        const predicate = (item: T) => accessors.some(item as ResolveObject, (c) =>
            typeof c === "string" && c.startsWith(prefix)
        );
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            ignoreCase,
            kind: "builtin",
            op: "startsWith",
            predicate,
            pushdown,
            reorderable: true,
            value: prefix,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
    }

    endsWith<P extends Paths<T>>(field: P, suffix: string, ignoreCase = false): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        if (ignoreCase) {
            const target = suffix.toLowerCase();
            const pushdown = { field: fieldKey, ignoreCase, op: "endsWith", value: suffix } as const;
            const predicate = (item: T) => accessors.some(item as ResolveObject, (c) =>
                typeof c === "string" && c.toLowerCase().endsWith(target)
            );
            const spec = this.createPredicateSpec({
                accessors,
                field: fieldKey,
                ignoreCase,
                kind: "builtin",
                op: "endsWith",
                predicate,
                pushdown,
                reorderable: true,
                value: suffix,
            });
            return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
        }
        const pushdown = { field: fieldKey, ignoreCase, op: "endsWith", value: suffix } as const;
        const predicate = (item: T) => accessors.some(item as ResolveObject, (c) =>
            typeof c === "string" && c.endsWith(suffix)
        );
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            ignoreCase,
            kind: "builtin",
            op: "endsWith",
            predicate,
            pushdown,
            reorderable: true,
            value: suffix,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
    }

    matches<P extends Paths<T>>(field: P, regex: RegExp): QueryBuilder<T, C, M> {
        const safeRegex = new RegExp(regex.source, regex.flags.replaceAll(/[gy]/g, ""));
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const pushdown = { field: fieldKey, op: "matches", value: safeRegex } as const;
        const predicate = (item: T) => accessors.some(item as ResolveObject, (c) =>
            typeof c === "string" && safeRegex.test(c)
        );
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "matches",
            predicate,
            pushdown,
            reorderable: true,
            value: safeRegex,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
    }

    isNull<P extends Paths<T>>(field: P): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const pushdown = { field: fieldKey, op: "isNull" } as const;
        const predicate = (item: T) => accessors.some(item as ResolveObject, (c) => c === null);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "isNull",
            predicate,
            pushdown,
            reorderable: true,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
    }

    valueNotNull<P extends Paths<T>>(field: P): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const pushdown = { field: fieldKey, op: "notNull" } as const;
        const predicate = (item: T) => accessors.some(item as ResolveObject, (c) => c != null);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "notNull",
            predicate,
            pushdown,
            reorderable: true,
        });
        return this.addPushdownPredicate(pushdown).addPredicate(predicate, spec);
    }

    pathExists<P extends Paths<T>>(field: P): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = (item: T) => accessors.exists(item as ResolveObject);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "pathExists",
            predicate,
            reorderable: true,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicate],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    pathExistsNullable<P extends Paths<T>>(field: P): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicate = (item: T) => accessors.some(item as ResolveObject, (c) => c !== undefined);
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "builtin",
            op: "pathExistsNullable",
            predicate,
            reorderable: true,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicate],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    arraySome<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicateFn = (item: T) => accessors.some(item as ResolveObject, (c) =>
            predicate(c as PathValue<T, P>)
        );
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "custom",
            op: "arraySome",
            predicate: predicateFn,
            reorderable: false,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    arrayEvery<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicateFn = (item: T) => accessors.every(item as ResolveObject, (c) =>
            predicate(c as PathValue<T, P>)
        );
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "custom",
            op: "arrayEvery",
            predicate: predicateFn,
            reorderable: false,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    arrayNone<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const accessors = this.getAccessors(fieldKey);
        const predicateFn = (item: T) => !accessors.some(item as ResolveObject, (c) =>
            predicate(c as PathValue<T, P>)
        );
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            kind: "custom",
            op: "arrayNone",
            predicate: predicateFn,
            reorderable: false,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    nested<P extends ArrayPaths<T>>(
        field: P,
        builder: (q: QueryBuilder<ArrayPathItem<T, P>>) => QueryBuilder<ArrayPathItem<T, P>>
    ): QueryBuilder<T, C, M> {
        const fieldKey = String(field);
        const nestedIngress = IngressEngine.fromSchema<ArrayPathItem<T, P>>(
            Schema.inline<ArrayPathItem<T, P>>()
        );
        const nestedPlan = builder(QueryBuilder.from(nestedIngress)).compilePlan();
        const nestedPredicate = nestedPlan.predicates;
        const accessors = this.getAccessors(fieldKey);
        const predicateFn = (item: T) => {
            return accessors.some(item as ResolveObject, (c) => {
                if (Array.isArray(c)) {
                    for (let i = 0; i < c.length; i++) {
                        for (let p = 0; p < nestedPredicate.length; p++) {
                            if (!nestedPredicate[p]!(c[i] as ArrayPathItem<T, P>)) {return false;}
                        }
                        return true;
                    }
                    return false;
                }
                if (c && typeof c === "object") {
                    for (let p = 0; p < nestedPredicate.length; p++) {
                        if (!nestedPredicate[p]!(c as ArrayPathItem<T, P>)) {return false;}
                    }
                    return true;
                }
                return false;
            });
        };
        const spec = this.createPredicateSpec({
            accessors,
            field: fieldKey,
            groupInfo: {
                estimatedCost: nestedPlan.planning?.estimatedCost,
                estimatedSelectivity: nestedPlan.planning?.estimatedSelectivity,
                planId: nestedPlan.id,
                predicateCount: nestedPredicate.length,
                reorderable: false,
            },
            kind: "nested",
            op: "nested",
            predicate: predicateFn,
            reorderable: false,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    and(builder: (q: QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">) => QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">): QueryBuilder<T, C, M> {
        const groupPlan = builder(this.builderForGroup()).compilePlan();
        const group = groupPlan.predicates;
        const predicateFn = (item: T) => group.every((p: Predicate<T>) => p(item));
        const spec = this.createPredicateSpec({
            groupInfo: {
                estimatedCost: groupPlan.planning?.estimatedCost,
                estimatedSelectivity: groupPlan.planning?.estimatedSelectivity,
                planId: groupPlan.id,
                predicateCount: group.length,
                reorderable: false,
            },
            kind: "group",
            op: "groupAnd",
            predicate: predicateFn,
            reorderable: false,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    or(builder: (q: QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">) => QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">): QueryBuilder<T, C, M> {
        const groupPlan = builder(this.builderForGroup()).compilePlan();
        const group = groupPlan.predicates;
        const left = this.state.predicates;
        if (group.length === 0) {return this;}
        if (left.length === 0) {
            const predicateFn = (item: T) => group.every((p: Predicate<T>) => p(item));
            const spec = this.createPredicateSpec({
                groupInfo: {
                    estimatedCost: groupPlan.planning?.estimatedCost,
                    estimatedSelectivity: groupPlan.planning?.estimatedSelectivity,
                    planId: groupPlan.id,
                    predicateCount: group.length,
                    reorderable: false,
                },
                kind: "group",
                op: "groupAnd",
                predicate: predicateFn,
                reorderable: false,
            });
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [predicateFn],
                predicateSpecs: [spec],
                pushdownSafe: false,
            });
        }
        const predicateFn = (item: T) => left.every((p: Predicate<T>) => p(item)) || group.every((p: Predicate<T>) => p(item));
        const spec = this.createPredicateSpec({
            groupInfo: {
                estimatedCost: groupPlan.planning?.estimatedCost,
                estimatedSelectivity: groupPlan.planning?.estimatedSelectivity,
                planId: groupPlan.id,
                predicateCount: group.length,
                reorderable: false,
            },
            kind: "group",
            op: "groupOr",
            predicate: predicateFn,
            reorderable: false,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [
                predicateFn,
            ],
            predicateSpecs: [spec],
            pushdownSafe: false,
        });
    }

    not(builder: (q: QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">) => QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">): QueryBuilder<T, C, M> {
        const groupPlan = builder(this.builderForGroup()).compilePlan();
        const group = groupPlan.predicates;
        const predicateFn = (item: T) => !group.every((p: Predicate<T>) => p(item));
        const spec = this.createPredicateSpec({
            groupInfo: {
                estimatedCost: groupPlan.planning?.estimatedCost,
                estimatedSelectivity: groupPlan.planning?.estimatedSelectivity,
                planId: groupPlan.id,
                predicateCount: group.length,
                reorderable: false,
            },
            kind: "group",
            op: "groupNot",
            predicate: predicateFn,
            reorderable: false,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicateFn],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    private builderForGroup(): QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async"> {
        const isAsync = this.ingress instanceof Object && "mode" in this.ingress && this.ingress.mode === "async";
        if (isAsync) {
            return QueryBuilder.fromAsync(this.ingress as AsyncIngressEngine<T>);
        }
        return QueryBuilder.from(this.ingress as IngressEngine<T>);
    }

    custom(predicate: Predicate<T>): QueryBuilder<T, C, M> {
        const spec = this.createPredicateSpec({
            kind: "custom",
            op: "custom",
            predicate,
            reorderable: false,
        });
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicate],
            predicateSpecs: [...this.state.predicateSpecs, spec],
            pushdownSafe: false,
        });
    }

    orderBy<P extends SortablePaths<T>>(field: P, options?: OrderOptions): QueryBuilder<T, C, M> {
        const order: PushdownOrder = {
            direction: options?.direction === "desc" ? "desc" : "asc",
            field: String(field),
            nulls: options?.nulls,
        };
        return this.addPushdownOrder(order);
    }

    orderByDate<P extends DatePaths<T>>(field: P, options?: OrderOptions): QueryBuilder<T, C, M> {
        const order: PushdownOrder = {
            direction: options?.direction === "desc" ? "desc" : "asc",
            field: String(field),
            nulls: options?.nulls,
        };
        return this.addPushdownOrder(order);
    }

    thenBy<P extends SortablePaths<T>>(field: P, options?: OrderOptions): QueryBuilder<T, C, M> {
        return this.orderBy(field, options);
    }

    thenByDate<P extends DatePaths<T>>(field: P, options?: OrderOptions): QueryBuilder<T, C, M> {
        return this.orderByDate(field, options);
    }
}
