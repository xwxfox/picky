import type { QueryPlan } from "@/core/engine/plan";
import type { IngressEngine } from "@/io/ingress";
import type { SortKey, OrderOptions, PaginationOptions, PaginationCursor, OrderSpec, DatePaths, SortablePaths, GroupablePaths, GroupKey, ResolveObject } from "@/types";
import type {
    SearchCapabilityState,
    SearchInput,
    TagFilter,
    AvailableTags,
    SearchFilterState,
} from "@/types/search";
import { resolveSearchQuery } from "@/core/search/runtime";
import type { CompiledTaggerConfig } from "@/core/search/runtime";
import type { GroupKeyValue } from "@/types/core";
import { ExecutionEngine } from "@/core/engine/executor";
import { executeSearchPipeline } from "@/core/search/runtime";
import { getSegments } from "@/core/shared/cache";
import { forEachResolvedWithSegments, resolveOrderValueWithSegments } from "@/core/shared/path";
import { createComparator } from "@/core/engine/compare";
import type { Orderable1, Orderable2, Orderable3, OrderableN, Orderable } from "@/core/engine/compare";

type EgressState<T extends Record<string, unknown>> = {
    ingress: IngressEngine<T>;
    limitCount: number | null;
    offsetCount: number;
    orders: Array<OrderSpec>;
    plan: QueryPlan<T>;
    searchFilters: Array<SearchFilterState>;
};

export class EgressEngine<
    T extends Record<string, unknown>,
    C extends SearchCapabilityState = SearchCapabilityState
> {
    private constructor(private readonly state: EgressState<T>) {}

    static from<T extends Record<string, unknown>, C extends SearchCapabilityState = SearchCapabilityState>(
        ingress: IngressEngine<T>,
        plan: QueryPlan<T>
    ): EgressEngine<T, C> {
        return new EgressEngine<T, C>({
            ingress,
            limitCount: null,
            offsetCount: 0,
            orders: [],
            plan,
            searchFilters: [],
        });
    }

    private clone(next: Partial<EgressState<T>>): EgressEngine<T, C> {
        return new EgressEngine<T, C>({ ...this.state, ...next });
    }

    search(input: SearchInput<C>): EgressEngine<T, C> {
        if (!this.state.plan.fuzzyConfig && !this.state.plan.taggerConfig) {
            if (this.state.plan.strictSearch) {throw new Error("search() used without configureFuzzy/configureTagger.");}
            return this.clone({
                searchFilters: [...this.state.searchFilters, { tags: { hasAny: [] } }],
            });
        }
        const filter = resolveSearchQuery(input);
        return this.clone({
            searchFilters: [...this.state.searchFilters, filter],
        });
    }

    tags(filter: TagFilter<AvailableTags<C>>): EgressEngine<T, C> {
        if (!this.state.plan.taggerConfig) {
            if (this.state.plan.strictSearch) {throw new Error("tags() used without configureTagger.");}
            return this.clone({
                searchFilters: [...this.state.searchFilters, { tags: { hasAny: [] } }],
            });
        }
        return this.clone({
            searchFilters: [...this.state.searchFilters, { tags: filter }],
        });
    }

    orderBy<P extends SortablePaths<T>>(field: P, options?: OrderOptions): EgressEngine<T, C> {
        const segments = getSegments(this.state.plan.cache, String(field));
        const direction = options?.direction === "desc" ? -1 : 1;
        const nullsFirst = options?.nulls === "first";
        const order: OrderSpec = {
            direction,
            nullsFirst,
            resolve: this.state.plan.cache.orderResolver,
            segments,
        };
        return this.clone({ orders: [...this.state.orders, order] });
    }

    orderByDate<P extends DatePaths<T>>(field: P, options?: OrderOptions): EgressEngine<T, C> {
        const segments = getSegments(this.state.plan.cache, String(field));
        const direction = options?.direction === "desc" ? -1 : 1;
        const nullsFirst = options?.nulls === "first";
        const order: OrderSpec = {
            direction,
            nullsFirst,
            resolve: this.state.plan.cache.orderResolverDate,
            segments,
        };
        return this.clone({ orders: [...this.state.orders, order] });
    }

    thenBy<P extends SortablePaths<T>>(field: P, options?: OrderOptions): EgressEngine<T, C> {
        return this.orderBy(field, options);
    }

    thenByDate<P extends DatePaths<T>>(field: P, options?: OrderOptions): EgressEngine<T, C> {
        return this.orderByDate(field, options);
    }

    limit(count: number): EgressEngine<T, C> {
        const safeCount = Number.isFinite(count) ? Math.max(0, Math.floor(count)) : 0;
        return this.clone({ limitCount: safeCount });
    }

    offset(count: number): EgressEngine<T, C> {
        const safeCount = Number.isFinite(count) ? Math.max(0, Math.floor(count)) : 0;
        return this.clone({ offsetCount: safeCount });
    }

    page(page: number, pageSize: number): EgressEngine<T, C> {
        const safePage = Number.isFinite(page) && page > 0 ? Math.floor(page) : 1;
        const safeSize = Number.isFinite(pageSize) && pageSize > 0 ? Math.floor(pageSize) : 1;
        return this.clone({ limitCount: safeSize, offsetCount: (safePage - 1) * safeSize });
    }

    result(): Array<T> {
        const executor = new ExecutionEngine<T>();
        const mergedPlan = this.state.searchFilters.length === 0
            ? this.state.plan
            : {
                ...this.state.plan,
                searchFilters: [...this.state.plan.searchFilters, ...this.state.searchFilters],
            };
        const filtered = executor.execute(this.state.ingress, mergedPlan);
        if (this.state.orders.length === 0) {
            if (this.state.limitCount === null && this.state.offsetCount === 0) {return filtered;}
            const start = this.state.offsetCount > 0 ? this.state.offsetCount : 0;
            const end = this.state.limitCount === null ? filtered.length : start + this.state.limitCount;
            return filtered.slice(start, end);
        }

        const orderCount = this.state.orders.length;
        const compare = createComparator<T>(this.state.orders);
        const entries: Array<Orderable<T>> = [];

        for (let i = 0; i < filtered.length; i++) {
            const item = filtered[i]!;
            if (orderCount === 1) {
                const order0 = this.state.orders[0]!;
                entries.push({
                    index: entries.length,
                    item,
                    k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                } as Orderable1<T>);
            } else if (orderCount === 2) {
                const order0 = this.state.orders[0]!;
                const order1 = this.state.orders[1]!;
                entries.push({
                    index: entries.length,
                    item,
                    k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                    k1: resolveOrderValueWithSegments(item as ResolveObject, order1.segments, order1.resolve),
                } as Orderable2<T>);
            } else if (orderCount === 3) {
                const order0 = this.state.orders[0]!;
                const order1 = this.state.orders[1]!;
                const order2 = this.state.orders[2]!;
                entries.push({
                    index: entries.length,
                    item,
                    k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                    k1: resolveOrderValueWithSegments(item as ResolveObject, order1.segments, order1.resolve),
                    k2: resolveOrderValueWithSegments(item as ResolveObject, order2.segments, order2.resolve),
                } as Orderable3<T>);
            } else {
                const keys = new Array<SortKey | null>(orderCount);
                for (let j = 0; j < orderCount; j++) {
                    const order = this.state.orders[j]!;
                    keys[j] = resolveOrderValueWithSegments(item as ResolveObject, order.segments, order.resolve);
                }
                entries.push({ index: entries.length, item, keys } as OrderableN<T>);
            }
        }

        entries.sort(compare);
        const start = this.state.offsetCount > 0 ? this.state.offsetCount : 0;
        const end = this.state.limitCount === null ? entries.length : start + this.state.limitCount;
        const result: Array<T> = [];
        const last = end < entries.length ? end : entries.length;
        for (let i = start; i < last; i++) {result.push(entries[i]!.item);}
        return result;
    }

    paginate(options: PaginationOptions): PaginationCursor<T> {
        const safeSize = Number.isFinite(options.pageSize) && options.pageSize > 0
            ? Math.floor(options.pageSize)
            : 1;
        const safePage = Number.isFinite(options.page) && options.page! > 0
            ? Math.floor(options.page!)
            : 1;
        const totalMode = options.total ?? "none";

        const data = this.result();
        const total = data.length;

        const cursor: PaginationCursor<T> = {
            data: [],
            next: () => {
                cursor.page++;
                fillPage(cursor.data, cursor.page);
                return cursor;
            },
            page: safePage,
            previous: () => {
                if (cursor.page <= 1) {return cursor;}
                cursor.page--;
                fillPage(cursor.data, cursor.page);
                return cursor;
            },
            total: totalMode === "none" ? undefined : total,
        };

        const fillPage = (buffer: Array<T>, page: number) => {
            const start = (page - 1) * safeSize;
            buffer.length = 0;
            if (start >= data.length) {return;}
            const end = start + safeSize;
            const last = end < data.length ? end : data.length;
            for (let i = start; i < last; i++) {buffer.push(data[i]!);}
        };

        fillPage(cursor.data, cursor.page);
        return cursor;
    }

    groupBy<P extends GroupablePaths<T>>(
        field: P,
        options?: { date?: boolean }
    ): Map<GroupKey<T, P>, Array<T>> {
        const items = this.result();
        const segments = getSegments(this.state.plan.cache, String(field));
        const convert = options?.date ? this.state.plan.cache.groupKeyConverterDate : this.state.plan.cache.groupKeyConverter;
        const result = new Map<GroupKeyValue, Array<T>>();
        for (let i = 0; i < items.length; i++) {
            const item = items[i]!;
            forEachResolvedWithSegments(item as ResolveObject, segments, (value) => {
                const key = convert(value);
                if (key === null || key === undefined) {return;}
                const bucket = result.get(key);
                if (bucket) {bucket.push(item);}
                else {result.set(key, [item]);}
            });
        }
        return result as Map<GroupKey<T, P>, Array<T>>;
    }

    resultWithMetadata(): Array<{ item: T; score?: number; tagMask?: number }> {
        const mergedPlan = this.state.searchFilters.length === 0
            ? this.state.plan
            : {
                ...this.state.plan,
                searchFilters: [...this.state.plan.searchFilters, ...this.state.searchFilters],
            };
        if (mergedPlan.searchFilters.length === 0) {
            return this.result().map(item => ({ item }));
        }
        const result = executeSearchPipeline<T, SearchCapabilityState>(
            this.state.ingress.data,
            mergedPlan.predicates,
            mergedPlan.cache,
            mergedPlan.fuzzyConfig,
            mergedPlan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
            mergedPlan.searchFilters,
            true
        );
        const output: Array<{ item: T; score?: number; tagMask?: number }> = [];
        for (let i = 0; i < result.items.length; i++) {
            output.push({
                item: result.items[i]!,
                score: result.scores ? result.scores[i] : undefined,
                tagMask: result.tagMasks ? result.tagMasks[i] : undefined,
            });
        }
        return output;
    }
}
