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
import type { IngressEngine } from "@/io/ingress";
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

export type QueryBuilderState<T> = {
    cache: CacheState;
    fuzzyConfig: CompiledFuzzyConfig<T> | null;
    predicates: Array<Predicate<T>>;
    searchFilters: Array<SearchFilterState>;
    strictSearch: boolean;
    taggerConfig: CompiledTaggerConfig<T, string> | null;
};

export class QueryBuilder<
    T extends Record<string, unknown>,
    C extends SearchCapabilityState = DefaultSearchCapabilityState
> {
    private constructor(
        private readonly ingress: IngressEngine<T>,
        private readonly state: QueryBuilderState<T>
    ) {}

    static from<T extends Record<string, unknown>>(ingress: IngressEngine<T>): QueryBuilder<T> {
        return new QueryBuilder(ingress, {
            cache: ingress.cache,
            fuzzyConfig: null,
            predicates: [],
            searchFilters: [],
            strictSearch: true,
            taggerConfig: null,
        });
    }

    private addPredicate(predicate: Predicate<T>): QueryBuilder<T, C> {
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicate],
        });
    }

    private getAccessors(path: string): PathAccessors {
        return getPathAccessors(this.state.cache, path);
    }

    use(chain: QueryChain<T>): QueryBuilder<T, C> {
        const plan = chain.getPlan();
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, ...plan.predicates],
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
            predicates,
            predicateFn,
            searchFilters: this.state.searchFilters,
            strictSearch: this.state.strictSearch,
            taggerConfig: this.state.taggerConfig,
        };
    }

    out(): EgressEngine<T, C> {
        const plan = this.compilePlan();
        return EgressEngine.from<T, C>(this.ingress, plan);
    }

    configureFuzzy(config: FuzzyConfig<T>): QueryBuilder<T, WithFuzzy<C>> {
        const compiled = compileFuzzyConfig(this.state.cache, config);
        return new QueryBuilder<T, WithFuzzy<C>>(this.ingress, {
            ...this.state,
            fuzzyConfig: compiled,
        });
    }

    configureTagger<Tags extends string>(config: TaggerConfig<T, Tags>): QueryBuilder<T, WithTagger<C, Tags>> {
        const compiled = compileTaggerConfig(this.state.cache, config);
        return new QueryBuilder<T, WithTagger<C, Tags>>(this.ingress, {
            ...this.state,
            taggerConfig: compiled,
        });
    }

    search(input: SearchInput<C>): QueryBuilder<T, C> {
        const emptyTagFilter: TagFilter<AvailableTags<C>> = { hasAny: [] };
        if (!this.state.fuzzyConfig && !this.state.taggerConfig) {
            if (this.state.strictSearch) {throw new Error("search() used without configureFuzzy/configureTagger.");}
            return new QueryBuilder(this.ingress, {
                ...this.state,
                searchFilters: [...this.state.searchFilters, { tags: emptyTagFilter }],
            });
        }
        const filter = resolveSearchQuery(input);
        return new QueryBuilder(this.ingress, {
            ...this.state,
            searchFilters: [...this.state.searchFilters, filter],
        });
    }

    tags(filter: TagFilter<AvailableTags<C>>): QueryBuilder<T, C> {
        const emptyTagFilter: TagFilter<AvailableTags<C>> = { hasAny: [] };
        if (!this.state.taggerConfig) {
            if (this.state.strictSearch) {throw new Error("tags() used without configureTagger.");}
            return new QueryBuilder(this.ingress, {
                ...this.state,
                searchFilters: [...this.state.searchFilters, { tags: emptyTagFilter }],
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            searchFilters: [...this.state.searchFilters, { tags: filter }],
        });
    }

    // Filter operators
    equals<P extends NonDatePaths<T>>(field: P, value: PathValue<T, P>): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const target = value as ResolveValue;
        return this.addPredicate(item =>
            accessors.some(item as ResolveObject, (c) => c === target)
        );
    }

    notEquals<P extends NonDatePaths<T>>(field: P, value: PathValue<T, P>): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const target = value as ResolveValue;
        return this.addPredicate(item =>
            !accessors.some(item as ResolveObject, (c) => c === target)
        );
    }

    greaterThan<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const predicate = createComparePredicate(value as ResolveValue, "gt");
        return this.addPredicate(item => accessors.some(item as ResolveObject, predicate));
    }

    greaterThanOrEqual<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const predicate = createComparePredicate(value as ResolveValue, "gte");
        return this.addPredicate(item => accessors.some(item as ResolveObject, predicate));
    }

    lessThan<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const predicate = createComparePredicate(value as ResolveValue, "lt");
        return this.addPredicate(item => accessors.some(item as ResolveObject, predicate));
    }

    lessThanOrEqual<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const predicate = createComparePredicate(value as ResolveValue, "lte");
        return this.addPredicate(item => accessors.some(item as ResolveObject, predicate));
    }

    between<P extends NonDatePaths<T>>(field: P, min: Extract<PathValue<T, P>, Comparable>, max: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const predicate = createBetweenPredicate(min as ResolveValue, max as ResolveValue);
        return this.addPredicate(item => accessors.some(item as ResolveObject, predicate));
    }

    in<P extends NonDatePaths<T>>(field: P, values: Array<PathValue<T, P>>): QueryBuilder<T, C> {
        if (values.length === 0) {return this.addPredicate(() => false);}
        const valueSet = new Set(values as Array<ResolveValue>);
        const accessors = this.getAccessors(String(field));
        return this.addPredicate(item =>
            accessors.some(item as ResolveObject, (c) => valueSet.has(c))
        );
    }

    notIn<P extends NonDatePaths<T>>(field: P, values: Array<PathValue<T, P>>): QueryBuilder<T, C> {
        if (values.length === 0) {return this.addPredicate(() => true);}
        const valueSet = new Set(values as Array<ResolveValue>);
        const accessors = this.getAccessors(String(field));
        return this.addPredicate(item =>
            !accessors.some(item as ResolveObject, (c) => valueSet.has(c))
        );
    }

    dateEquals<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateEqualsPredicate(this.state.cache.parseIsoDate, value);
        if (!predicate) {return this.addPredicate(() => false);}
        return this.addPredicate(item => accessors.some(item as ResolveObject, predicate));
    }

    dateAfter<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "gt");
        if (!predicate) {return this.addPredicate(() => false);}
        return this.addPredicate(item => accessors.some(item as ResolveObject, predicate));
    }

    dateAfterOrEqual<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "gte");
        if (!predicate) {return this.addPredicate(() => false);}
        return this.addPredicate(item => accessors.some(item as ResolveObject, predicate));
    }

    dateBefore<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "lt");
        if (!predicate) {return this.addPredicate(() => false);}
        return this.addPredicate(item => accessors.some(item as ResolveObject, predicate));
    }

    dateBeforeOrEqual<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "lte");
        if (!predicate) {return this.addPredicate(() => false);}
        return this.addPredicate(item => accessors.some(item as ResolveObject, predicate));
    }

    dateBetween<P extends DatePaths<T>>(field: P, min: Date | string | number, max: Date | string | number): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        const predicate = createDateBetweenPredicate(this.state.cache.parseIsoDate, min, max);
        if (!predicate) {return this.addPredicate(() => false);}
        return this.addPredicate(item => accessors.some(item as ResolveObject, predicate));
    }

    contains<P extends Paths<T>>(field: P, substring: string, ignoreCase = false): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        if (ignoreCase) {
            const target = substring.toLowerCase();
            return this.addPredicate(item =>
                accessors.some(item as ResolveObject, (c) =>
                    typeof c === "string" && c.toLowerCase().includes(target)
                )
            );
        }
        return this.addPredicate(item =>
            accessors.some(item as ResolveObject, (c) =>
                typeof c === "string" && c.includes(substring)
            )
        );
    }

    startsWith<P extends Paths<T>>(field: P, prefix: string, ignoreCase = false): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        if (ignoreCase) {
            const target = prefix.toLowerCase();
            return this.addPredicate(item =>
                accessors.some(item as ResolveObject, (c) =>
                    typeof c === "string" && c.toLowerCase().startsWith(target)
                )
            );
        }
        return this.addPredicate(item =>
            accessors.some(item as ResolveObject, (c) =>
                typeof c === "string" && c.startsWith(prefix)
            )
        );
    }

    endsWith<P extends Paths<T>>(field: P, suffix: string, ignoreCase = false): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        if (ignoreCase) {
            const target = suffix.toLowerCase();
            return this.addPredicate(item =>
                accessors.some(item as ResolveObject, (c) =>
                    typeof c === "string" && c.toLowerCase().endsWith(target)
                )
            );
        }
        return this.addPredicate(item =>
            accessors.some(item as ResolveObject, (c) =>
                typeof c === "string" && c.endsWith(suffix)
            )
        );
    }

    matches<P extends Paths<T>>(field: P, regex: RegExp): QueryBuilder<T, C> {
        const safeRegex = new RegExp(regex.source, regex.flags.replaceAll(/[gy]/g, ""));
        const accessors = this.getAccessors(String(field));
        return this.addPredicate(item =>
            accessors.some(item as ResolveObject, (c) =>
                typeof c === "string" && safeRegex.test(c)
            )
        );
    }

    isNull<P extends Paths<T>>(field: P): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        return this.addPredicate(item =>
            accessors.some(item as ResolveObject, (c) => c === null)
        );
    }

    valueNotNull<P extends Paths<T>>(field: P): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        return this.addPredicate(item =>
            accessors.some(item as ResolveObject, (c) => c != null)
        );
    }

    pathExists<P extends Paths<T>>(field: P): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        return this.addPredicate(item =>
            accessors.exists(item as ResolveObject)
        );
    }

    pathExistsNullable<P extends Paths<T>>(field: P): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        return this.addPredicate(item =>
            accessors.some(item as ResolveObject, (c) => c !== undefined)
        );
    }

    arraySome<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        return this.addPredicate(item =>
            accessors.some(item as ResolveObject, (c) =>
                predicate(c as PathValue<T, P>)
            )
        );
    }

    arrayEvery<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        return this.addPredicate(item =>
            accessors.every(item as ResolveObject, (c) =>
                predicate(c as PathValue<T, P>)
            )
        );
    }

    arrayNone<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T, C> {
        const accessors = this.getAccessors(String(field));
        return this.addPredicate(item =>
            !accessors.some(item as ResolveObject, (c) =>
                predicate(c as PathValue<T, P>)
            )
        );
    }

    nested<P extends ArrayPaths<T>>(
        field: P,
        builder: (q: QueryBuilder<ArrayPathItem<T, P>>) => QueryBuilder<ArrayPathItem<T, P>>
    ): QueryBuilder<T, C> {
        const nestedPredicate = builder(
            QueryBuilder.from(this.ingress as IngressEngine<ArrayPathItem<T, P>>)
        ).compilePlan().predicates;
        const accessors = this.getAccessors(String(field));
        return this.addPredicate(item => {
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
        });
    }

    and(builder: (q: QueryBuilder<T>) => QueryBuilder<T>): QueryBuilder<T, C> {
        const group = builder(QueryBuilder.from(this.ingress)).compilePlan().predicates;
        return this.addPredicate((item) => group.every((p: Predicate<T>) => p(item)));
    }

    or(builder: (q: QueryBuilder<T>) => QueryBuilder<T>): QueryBuilder<T, C> {
        const group = builder(QueryBuilder.from(this.ingress)).compilePlan().predicates;
        const left = this.state.predicates;
        if (group.length === 0) {return this;}
        if (left.length === 0) {
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...group],
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [
                (item) => left.every((p: Predicate<T>) => p(item)) || group.every((p: Predicate<T>) => p(item)),
            ],
        });
    }

    not(builder: (q: QueryBuilder<T>) => QueryBuilder<T>): QueryBuilder<T, C> {
        const group = builder(QueryBuilder.from(this.ingress)).compilePlan().predicates;
        return this.addPredicate((item) => !group.every((p: Predicate<T>) => p(item)));
    }

    custom(predicate: Predicate<T>): QueryBuilder<T, C> {
        return this.addPredicate(predicate);
    }
}
