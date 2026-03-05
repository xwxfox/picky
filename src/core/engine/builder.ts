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
import type { QueryPlan } from "./plan";
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
    private constructor(
        private readonly ingress: M extends "async" ? AsyncIngressEngine<T> : IngressEngine<T>,
        private readonly state: QueryBuilderState<T>
    ) {}

    static from<T extends Record<string, unknown>>(ingress: IngressEngine<T>): QueryBuilder<T, DefaultSearchCapabilityState, "sync"> {
        return new QueryBuilder<T, DefaultSearchCapabilityState, "sync">(ingress, {
            cache: ingress.cache,
            fuzzyConfig: null,
            predicates: [],
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
            pushdownOrders: [],
            pushdownPredicates: [],
            pushdownSafe: true,
            searchFilters: [],
            strictSearch: true,
            taggerConfig: null,
        });
    }

    private addPredicate(predicate: Predicate<T>): QueryBuilder<T, C, M> {
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicate],
        });
    }

    private addPushdownPredicate(predicate: PushdownPredicate): QueryBuilder<T, C, M> {
        return new QueryBuilder(this.ingress, {
            ...this.state,
            pushdownPredicates: [...this.state.pushdownPredicates, predicate],
        });
    }

    private getAccessors(path: string): PathAccessors {
        return getPathAccessors(this.state.cache, path);
    }

    private addPushdownOrder(order: PushdownOrder): QueryBuilder<T, C, M> {
        return new QueryBuilder(this.ingress, {
            ...this.state,
            pushdownOrders: [...this.state.pushdownOrders, order],
        });
    }

    use(chain: QueryChain<T>): QueryBuilder<T, C, M> {
        const plan = chain.getPlan();
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, ...plan.predicates],
            pushdownSafe: false,
        });
    }

    compilePlan(): QueryPlan<T> {
        const searchKey = `${this.state.fuzzyConfig ? "f" : "_"}${this.state.taggerConfig ? "t" : "_"}${this.state.searchFilters.length}`;
        const id = hashPlanId(this.state.predicates, searchKey);
        const predicates = this.state.predicates;
        const predicateFn = predicates.length === 0
            ? () => true
            : (item: T) => {
                for (let i = 0; i < predicates.length; i++) {
                    if (!predicates[i]!(item)) {return false;}
                }
                return true;
            };
        return {
            cache: this.state.cache,
            fuzzyConfig: this.state.fuzzyConfig,
            id,
            predicateFn,
            predicates,
            pushdownOrders: this.state.pushdownOrders,
            pushdownPredicates: this.state.pushdownPredicates,
            pushdownSafe: this.state.pushdownSafe,
            searchFilters: this.state.searchFilters,
            strictSearch: this.state.strictSearch,
            taggerConfig: this.state.taggerConfig,
        };
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
            pushdownSafe: false,
        });
    }

    configureTagger<Tags extends string>(config: TaggerConfig<T, Tags>): QueryBuilder<T, WithTagger<C, Tags>, M> {
        const compiled = compileTaggerConfig(this.state.cache, config);
        return new QueryBuilder<T, WithTagger<C, Tags>, M>(this.ingress, {
            ...this.state,
            pushdownSafe: false,
            taggerConfig: compiled,
        });
    }

    search(input: SearchInput<C>): QueryBuilder<T, C, M> {
        const emptyTagFilter: TagFilter<AvailableTags<C>> = { hasAny: [] };
        if (!this.state.fuzzyConfig && !this.state.taggerConfig) {
            if (this.state.strictSearch) {throw new Error("search() used without configureFuzzy/configureTagger.");}
            return new QueryBuilder(this.ingress, {
                ...this.state,
                pushdownSafe: false,
                searchFilters: [...this.state.searchFilters, { tags: emptyTagFilter }],
            });
        }
        const filter = resolveSearchQuery(input);
        return new QueryBuilder(this.ingress, {
            ...this.state,
            pushdownSafe: false,
            searchFilters: [...this.state.searchFilters, filter],
        });
    }

    tags(filter: TagFilter<AvailableTags<C>>): QueryBuilder<T, C, M> {
        const emptyTagFilter: TagFilter<AvailableTags<C>> = { hasAny: [] };
        if (!this.state.taggerConfig) {
            if (this.state.strictSearch) {throw new Error("tags() used without configureTagger.");}
            return new QueryBuilder(this.ingress, {
                ...this.state,
                pushdownSafe: false,
                searchFilters: [...this.state.searchFilters, { tags: emptyTagFilter }],
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            pushdownSafe: false,
            searchFilters: [...this.state.searchFilters, { tags: filter }],
        });
    }

    // Filter operators
    equals<P extends NonDatePaths<T>>(field: P, value: PathValue<T, P>): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const target = value as ResolveValue;
        return this.addPushdownPredicate({ field: String(field), op: "eq", value: target }).addPredicate(item =>
            accessors.some(item as ResolveObject, (c) => c === target)
        );
    }

    notEquals<P extends NonDatePaths<T>>(field: P, value: PathValue<T, P>): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const target = value as ResolveValue;
        return this.addPushdownPredicate({ field: String(field), op: "ne", value: target }).addPredicate(item =>
            !accessors.some(item as ResolveObject, (c) => c === target)
        );
    }

    greaterThan<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const predicate = createComparePredicate(value as ResolveValue, "gt");
        return this.addPushdownPredicate({ field: String(field), op: "gt", value: value as ResolveValue }).addPredicate(item =>
            accessors.some(item as ResolveObject, predicate)
        );
    }

    greaterThanOrEqual<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const predicate = createComparePredicate(value as ResolveValue, "gte");
        return this.addPushdownPredicate({ field: String(field), op: "gte", value: value as ResolveValue }).addPredicate(item =>
            accessors.some(item as ResolveObject, predicate)
        );
    }

    lessThan<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const predicate = createComparePredicate(value as ResolveValue, "lt");
        return this.addPushdownPredicate({ field: String(field), op: "lt", value: value as ResolveValue }).addPredicate(item =>
            accessors.some(item as ResolveObject, predicate)
        );
    }

    lessThanOrEqual<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const predicate = createComparePredicate(value as ResolveValue, "lte");
        return this.addPushdownPredicate({ field: String(field), op: "lte", value: value as ResolveValue }).addPredicate(item =>
            accessors.some(item as ResolveObject, predicate)
        );
    }

    between<P extends NonDatePaths<T>>(field: P, min: Extract<PathValue<T, P>, Comparable>, max: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const predicate = createBetweenPredicate(min as ResolveValue, max as ResolveValue);
        return this.addPushdownPredicate({ field: String(field), op: "between", value: { max: max as ResolveValue, min: min as ResolveValue } }).addPredicate(item =>
            accessors.some(item as ResolveObject, predicate)
        );
    }

    in<P extends NonDatePaths<T>>(field: P, values: Array<PathValue<T, P>>): QueryBuilder<T, C, M> {
        if (values.length === 0) {return this.addPushdownPredicate({ field: String(field), op: "in", value: [] }).addPredicate(() => false);}
        const valueSet = new Set(values as Array<ResolveValue>);
        const accessors = this.getAccessors(String(field));
        return this.addPushdownPredicate({ field: String(field), op: "in", value: values as Array<ResolveValue> }).addPredicate(item =>
            accessors.some(item as ResolveObject, (c) => valueSet.has(c))
        );
    }

    notIn<P extends NonDatePaths<T>>(field: P, values: Array<PathValue<T, P>>): QueryBuilder<T, C, M> {
        if (values.length === 0) {return this.addPushdownPredicate({ field: String(field), op: "notIn", value: [] }).addPredicate(() => true);}
        const valueSet = new Set(values as Array<ResolveValue>);
        const accessors = this.getAccessors(String(field));
        return this.addPushdownPredicate({ field: String(field), op: "notIn", value: values as Array<ResolveValue> }).addPredicate(item =>
            !accessors.some(item as ResolveObject, (c) => valueSet.has(c))
        );
    }

    dateEquals<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateEqualsPredicate(this.state.cache.parseIsoDate, value);
        if (!predicate) {
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, () => false],
                pushdownSafe: false,
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => accessors.some(item as ResolveObject, predicate)],
            pushdownSafe: false,
        });
    }

    dateAfter<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "gt");
        if (!predicate) {
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, () => false],
                pushdownSafe: false,
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => accessors.some(item as ResolveObject, predicate)],
            pushdownSafe: false,
        });
    }

    dateAfterOrEqual<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "gte");
        if (!predicate) {
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, () => false],
                pushdownSafe: false,
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => accessors.some(item as ResolveObject, predicate)],
            pushdownSafe: false,
        });
    }

    dateBefore<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "lt");
        if (!predicate) {
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, () => false],
                pushdownSafe: false,
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => accessors.some(item as ResolveObject, predicate)],
            pushdownSafe: false,
        });
    }

    dateBeforeOrEqual<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "lte");
        if (!predicate) {
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, () => false],
                pushdownSafe: false,
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => accessors.some(item as ResolveObject, predicate)],
            pushdownSafe: false,
        });
    }

    dateBetween<P extends DatePaths<T>>(field: P, min: Date | string | number, max: Date | string | number): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateBetweenPredicate(this.state.cache.parseIsoDate, min, max);
        if (!predicate) {
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...this.state.predicates, () => false],
                pushdownSafe: false,
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => accessors.some(item as ResolveObject, predicate)],
            pushdownSafe: false,
        });
    }

    contains<P extends Paths<T>>(field: P, substring: string, ignoreCase = false): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        if (ignoreCase) {
            const target = substring.toLowerCase();
            return this.addPushdownPredicate({ field: String(field), ignoreCase, op: "contains", value: substring }).addPredicate(item =>
                accessors.some(item as ResolveObject, (c) =>
                    typeof c === "string" && c.toLowerCase().includes(target)
                )
            );
        }
        return this.addPushdownPredicate({ field: String(field), ignoreCase, op: "contains", value: substring }).addPredicate(item =>
            accessors.some(item as ResolveObject, (c) =>
                typeof c === "string" && c.includes(substring)
            )
        );
    }

    startsWith<P extends Paths<T>>(field: P, prefix: string, ignoreCase = false): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        if (ignoreCase) {
            const target = prefix.toLowerCase();
            return this.addPushdownPredicate({ field: String(field), ignoreCase, op: "startsWith", value: prefix }).addPredicate(item =>
                accessors.some(item as ResolveObject, (c) =>
                    typeof c === "string" && c.toLowerCase().startsWith(target)
                )
            );
        }
        return this.addPushdownPredicate({ field: String(field), ignoreCase, op: "startsWith", value: prefix }).addPredicate(item =>
            accessors.some(item as ResolveObject, (c) =>
                typeof c === "string" && c.startsWith(prefix)
            )
        );
    }

    endsWith<P extends Paths<T>>(field: P, suffix: string, ignoreCase = false): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        if (ignoreCase) {
            const target = suffix.toLowerCase();
            return this.addPushdownPredicate({ field: String(field), ignoreCase, op: "endsWith", value: suffix }).addPredicate(item =>
                accessors.some(item as ResolveObject, (c) =>
                    typeof c === "string" && c.toLowerCase().endsWith(target)
                )
            );
        }
        return this.addPushdownPredicate({ field: String(field), ignoreCase, op: "endsWith", value: suffix }).addPredicate(item =>
            accessors.some(item as ResolveObject, (c) =>
                typeof c === "string" && c.endsWith(suffix)
            )
        );
    }

    matches<P extends Paths<T>>(field: P, regex: RegExp): QueryBuilder<T, C, M> {
        const safeRegex = new RegExp(regex.source, regex.flags.replaceAll(/[gy]/g, ""));
        const accessors = this.getAccessors(String(field));
        return this.addPushdownPredicate({ field: String(field), op: "matches", value: safeRegex }).addPredicate(item =>
            accessors.some(item as ResolveObject, (c) =>
                typeof c === "string" && safeRegex.test(c)
            )
        );
    }

    isNull<P extends Paths<T>>(field: P): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        return this.addPushdownPredicate({ field: String(field), op: "isNull" }).addPredicate(item =>
            accessors.some(item as ResolveObject, (c) => c === null)
        );
    }

    valueNotNull<P extends Paths<T>>(field: P): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        return this.addPushdownPredicate({ field: String(field), op: "notNull" }).addPredicate(item =>
            accessors.some(item as ResolveObject, (c) => c != null)
        );
    }

    pathExists<P extends Paths<T>>(field: P): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => accessors.exists(item as ResolveObject)],
            pushdownSafe: false,
        });
    }

    pathExistsNullable<P extends Paths<T>>(field: P): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => accessors.some(item as ResolveObject, (c) => c !== undefined)],
            pushdownSafe: false,
        });
    }

    arraySome<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => accessors.some(item as ResolveObject, (c) =>
                predicate(c as PathValue<T, P>)
            )],
            pushdownSafe: false,
        });
    }

    arrayEvery<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => accessors.every(item as ResolveObject, (c) =>
                predicate(c as PathValue<T, P>)
            )],
            pushdownSafe: false,
        });
    }

    arrayNone<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T, C, M> {
        const accessors = this.getAccessors(String(field));
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => !accessors.some(item as ResolveObject, (c) =>
                predicate(c as PathValue<T, P>)
            )],
            pushdownSafe: false,
        });
    }

    nested<P extends ArrayPaths<T>>(
        field: P,
        builder: (q: QueryBuilder<ArrayPathItem<T, P>>) => QueryBuilder<ArrayPathItem<T, P>>
    ): QueryBuilder<T, C, M> {
        const nestedIngress = IngressEngine.fromSchema<ArrayPathItem<T, P>>(
            Schema.inline<ArrayPathItem<T, P>>()
        );
        const nestedPredicate = builder(QueryBuilder.from(nestedIngress)).compilePlan().predicates;
        const accessors = this.getAccessors(String(field));
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => {
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
            }],
            pushdownSafe: false,
        });
    }

    and(builder: (q: QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">) => QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">): QueryBuilder<T, C, M> {
        const group = builder(this.builderForGroup()).compilePlan().predicates;
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => group.every((p: Predicate<T>) => p(item))],
            pushdownSafe: false,
        });
    }

    or(builder: (q: QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">) => QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">): QueryBuilder<T, C, M> {
        const group = builder(this.builderForGroup()).compilePlan().predicates;
        const left = this.state.predicates;
        if (group.length === 0) {return this;}
        if (left.length === 0) {
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...group],
                pushdownSafe: false,
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [
                (item) => left.every((p: Predicate<T>) => p(item)) || group.every((p: Predicate<T>) => p(item)),
            ],
            pushdownSafe: false,
        });
    }

    not(builder: (q: QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">) => QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async">): QueryBuilder<T, C, M> {
        const group = builder(this.builderForGroup()).compilePlan().predicates;
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, (item) => !group.every((p: Predicate<T>) => p(item))],
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
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicate],
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
