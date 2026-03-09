import type { QueryPlan } from "@/core/engine/plan";
import type { IngressEngine, AsyncIngressEngine } from "@/io/ingress";
import type { SortKey, OrderOptions, PaginationOptions, PaginationCursor, OrderSpec, DatePaths, SortablePaths, GroupablePaths, GroupKey, ResolveObject } from "@/types";
import type { PushdownOrder, PushdownQuery } from "@/io/ingress/adapters/pushdown";
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
import { AsyncExecutionEngine } from "@/core/engine/executor-async";
import { executeSearchPipeline, executeSearchPipelineAsync } from "@/core/search/runtime";
import { getPathAccessors } from "@/core/shared/cache";
import { resolveOrderValueWithSegments } from "@/core/shared/path";
import { createComparator } from "@/core/engine/compare";
import type { Orderable1, Orderable2, Orderable3, OrderableN, Orderable } from "@/core/engine/compare";
import { logPlanner } from "@/core/engine/planner-logger";
import type { TimingToken } from "@/core/engine/telemetry";
import { emitMarker, emitMetrics, endTiming, startTiming } from "@/core/engine/telemetry";
import { createPrefilterContext, emitPrefilterMetrics } from "@/core/engine/prefilter-plan";

export type EgressMode = "sync" | "async";

type EgressState<T extends Record<string, unknown>, M extends EgressMode> = {
    ingress: M extends "async" ? AsyncIngressEngine<T> : IngressEngine<T>;
    limitCount: number | null;
    offsetCount: number;
    orders: Array<OrderSpec>;
    plan: QueryPlan<T>;
    pushdownOrders: Array<PushdownOrder>;
    searchFilters: Array<SearchFilterState>;
};

export class EgressEngine<
    T extends Record<string, unknown>,
    C extends SearchCapabilityState = SearchCapabilityState,
    M extends EgressMode = "sync"
> {
    private constructor(private readonly state: EgressState<T, M>) { }

    static from<T extends Record<string, unknown>, C extends SearchCapabilityState = SearchCapabilityState>(
        ingress: IngressEngine<T>,
        plan: QueryPlan<T>
    ): EgressEngine<T, C, "sync"> {
        return new EgressEngine<T, C, "sync">({
            ingress,
            limitCount: null,
            offsetCount: 0,
            orders: [],
            plan,
            pushdownOrders: plan.pushdownOrders,
            searchFilters: [],
        });
    }

    static fromAsync<T extends Record<string, unknown>, C extends SearchCapabilityState = SearchCapabilityState>(
        ingress: AsyncIngressEngine<T>,
        plan: QueryPlan<T>
    ): EgressEngine<T, C, "async"> {
        return new EgressEngine<T, C, "async">({
            ingress,
            limitCount: null,
            offsetCount: 0,
            orders: [],
            plan,
            pushdownOrders: plan.pushdownOrders,
            searchFilters: [],
        });
    }

    private clone(next: Partial<EgressState<T, M>>): EgressEngine<T, C, M> {
        return new EgressEngine<T, C, M>({ ...this.state, ...next });
    }

    search(input: SearchInput<C>): EgressEngine<T, C, M> {
        if (!this.state.plan.fuzzyConfig && !this.state.plan.taggerConfig) {
            if (this.state.plan.strictSearch) { throw new Error("search() used without configureFuzzy/configureTagger."); }
            return this.clone({
                searchFilters: [...this.state.searchFilters, { tags: { hasAny: [] } }],
            });
        }
        const filter = resolveSearchQuery(input);
        return this.clone({
            searchFilters: [...this.state.searchFilters, filter],
        });
    }

    tags(filter: TagFilter<AvailableTags<C>>): EgressEngine<T, C, M> {
        if (!this.state.plan.taggerConfig) {
            if (this.state.plan.strictSearch) { throw new Error("tags() used without configureTagger."); }
            return this.clone({
                searchFilters: [...this.state.searchFilters, { tags: { hasAny: [] } }],
            });
        }
        return this.clone({
            searchFilters: [...this.state.searchFilters, { tags: filter }],
        });
    }

    orderBy<P extends SortablePaths<T>>(field: P, options?: OrderOptions): EgressEngine<T, C, M> {
        const accessors = getPathAccessors(this.state.plan.cache, String(field));
        const direction = options?.direction === "desc" ? -1 : 1;
        const nullsFirst = options?.nulls === "first";
        const order: OrderSpec = {
            direction,
            nullsFirst,
            resolve: this.state.plan.cache.orderResolver,
            segments: accessors.segments,
        };
        const pushdownOrder: PushdownOrder = {
            direction: options?.direction === "desc" ? "desc" : "asc",
            field: String(field),
            nulls: options?.nulls,
        };
        return this.clone({
            orders: [...this.state.orders, order],
            pushdownOrders: [...this.state.pushdownOrders, pushdownOrder],
        });
    }

    orderByDate<P extends DatePaths<T>>(field: P, options?: OrderOptions): EgressEngine<T, C, M> {
        const accessors = getPathAccessors(this.state.plan.cache, String(field));
        const direction = options?.direction === "desc" ? -1 : 1;
        const nullsFirst = options?.nulls === "first";
        const order: OrderSpec = {
            direction,
            nullsFirst,
            resolve: this.state.plan.cache.orderResolverDate,
            segments: accessors.segments,
        };
        const pushdownOrder: PushdownOrder = {
            direction: options?.direction === "desc" ? "desc" : "asc",
            field: String(field),
            nulls: options?.nulls,
        };
        return this.clone({
            orders: [...this.state.orders, order],
            pushdownOrders: [...this.state.pushdownOrders, pushdownOrder],
        });
    }

    thenBy<P extends SortablePaths<T>>(field: P, options?: OrderOptions): EgressEngine<T, C, M> {
        return this.orderBy(field, options);
    }

    thenByDate<P extends DatePaths<T>>(field: P, options?: OrderOptions): EgressEngine<T, C, M> {
        return this.orderByDate(field, options);
    }

    limit(count: number): EgressEngine<T, C, M> {
        const safeCount = Number.isFinite(count) ? Math.max(0, Math.floor(count)) : 0;
        return this.clone({ limitCount: safeCount });
    }

    offset(count: number): EgressEngine<T, C, M> {
        const safeCount = Number.isFinite(count) ? Math.max(0, Math.floor(count)) : 0;
        return this.clone({ offsetCount: safeCount });
    }

    page(page: number, pageSize: number): EgressEngine<T, C, M> {
        const safePage = Number.isFinite(page) && page > 0 ? Math.floor(page) : 1;
        const safeSize = Number.isFinite(pageSize) && pageSize > 0 ? Math.floor(pageSize) : 1;
        return this.clone({ limitCount: safeSize, offsetCount: (safePage - 1) * safeSize });
    }

    result(): M extends "async" ? Promise<Array<T>> : Array<T> {
        return this.executeResult() as M extends "async" ? Promise<Array<T>> : Array<T>;
    }

    first(): M extends "async" ? Promise<T | undefined> : T | undefined {
        return this.executeFirst() as M extends "async" ? Promise<T | undefined> : T | undefined;
    }

    firstOrThrow(): M extends "async" ? Promise<T> : T {
        const result = this.first();
        if (result instanceof Promise) {
            return result.then((item) => {
                if (item === undefined) {throw new Error("firstOrThrow() found no matching items.");}
                return item;
            }) as M extends "async" ? Promise<T> : T;
        }
        if (result === undefined) {throw new Error("firstOrThrow() found no matching items.");}
        return result as M extends "async" ? Promise<T> : T;
    }

    private executeResult(): Array<T> | Promise<Array<T>> {
        const mergedPlan = this.state.searchFilters.length === 0
            ? this.state.plan
            : {
                ...this.state.plan,
                searchFilters: [...this.state.plan.searchFilters, ...this.state.searchFilters],
            };
        const runTiming = startTiming("egress", "run.execute", mergedPlan.id);
        const timing = startTiming("egress", "egress.executeResult", mergedPlan.id, runTiming);
        emitMarker("egress", "egress.execute.start", mergedPlan.id, timing);
        emitMarker("egress", "egress.mergeFilters", mergedPlan.id, timing);
        logPlanner({
            source: "egress",
            type: "input",
            planId: mergedPlan.id,
            data: {
                hasSearch: mergedPlan.searchFilters.length > 0,
                orders: this.state.orders.length,
                limit: this.state.limitCount,
                offset: this.state.offsetCount,
            },
        });
        if (this.state.ingress instanceof Object && "mode" in this.state.ingress && this.state.ingress.mode === "async") {
            endTiming(timing, { skipData: true });
            return this.executeAsync(mergedPlan, runTiming);
        }
        const executor = new ExecutionEngine<T>();
        const filtered = executor.execute(this.state.ingress as IngressEngine<T>, mergedPlan, { timingParent: timing });
        logPlanner({
            source: "egress",
            type: "final",
            planId: mergedPlan.id,
            data: {
                mode: "sync",
                hasSearch: mergedPlan.searchFilters.length > 0,
                residualPredicates: mergedPlan.residualPredicates?.length ?? 0,
                resultCount: filtered.length,
            },
        });
        emitMetrics({
            source: "egress",
            planId: mergedPlan.id,
            metrics: {
                extras: {
                    mode: "sync",
                    hasSearch: mergedPlan.searchFilters.length > 0,
                    orders: this.state.orders.length,
                    limit: this.state.limitCount,
                    offset: this.state.offsetCount,
                },
            },
        });
        const finalizeTiming = startTiming("egress", "egress.finalizeOrder", mergedPlan.id, timing);
        const finalized = finalizeOrderAndWindow(filtered, this.state.orders, this.state.limitCount, this.state.offsetCount);
        endTiming(finalizeTiming, { skipData: true });
        emitMarker("egress", "egress.result.final", mergedPlan.id, timing);
        endTiming(timing, { skipData: true });
        endTiming(runTiming, { skipData: true });
        return finalized;
    }

    private executeFirst(): T | undefined | Promise<T | undefined> {
        const mergedPlan = this.state.searchFilters.length === 0
            ? this.state.plan
            : {
                ...this.state.plan,
                searchFilters: [...this.state.plan.searchFilters, ...this.state.searchFilters],
            };
        if (this.state.ingress instanceof Object && "mode" in this.state.ingress && this.state.ingress.mode === "async") {
            return this.executeFirstAsync(mergedPlan);
        }
        return this.executeFirstSync(mergedPlan);
    }

    private executeFirstSync(plan: QueryPlan<T>): T | undefined {
        if (plan.alwaysFalse) {return undefined;}
        if (this.state.limitCount === 0) {return undefined;}
        const offset = this.state.offsetCount;
        const hasOrders = this.state.orders.length > 0;
        const hasSearch = plan.searchFilters.length > 0;

        if (hasOrders) {
            const items = this.result() as Array<T>;
            return items[0];
        }

        const data = (this.state.ingress as IngressEngine<T>).data;

        if (hasSearch) {
            const windowLimit = offset + 1;
            const result = executeSearchPipeline<T, SearchCapabilityState>(
                data,
                plan.predicates,
                plan.predicateFn,
                plan.cache,
                plan.fuzzyConfig,
                plan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
                plan.searchFilters,
                false,
                plan.id,
                undefined,
                windowLimit
            );
            return result.items[offset];
        }

        if (plan.predicates.length === 0) {
            return offset < data.length ? data[offset] : undefined;
        }

        const predicateFn = plan.predicateFn;
        const residualPredicateFn = plan.residualPredicateFn;
        let seen = 0;

        if (residualPredicateFn && residualPredicateFn !== predicateFn) {
            for (let i = 0; i < data.length; i++) {
                const item = data[i]!;
                if (!predicateFn(item)) {continue;}
                if (!residualPredicateFn(item)) {continue;}
                if (seen++ < offset) {continue;}
                return item;
            }
            return undefined;
        }

        for (let i = 0; i < data.length; i++) {
            const item = data[i]!;
            if (!predicateFn(item)) {continue;}
            if (seen++ < offset) {continue;}
            return item;
        }
        return undefined;
    }

    private async executeFirstAsync(plan: QueryPlan<T>): Promise<T | undefined> {
        if (plan.alwaysFalse) {return undefined;}
        if (this.state.limitCount === 0) {return undefined;}
        const offset = this.state.offsetCount;
        const hasOrders = this.state.orders.length > 0;
        const hasSearch = plan.searchFilters.length > 0;
        const ingress = this.state.ingress as AsyncIngressEngine<T>;
        const hasResidual = !!(plan.residualPredicates && plan.residualPredicates.length > 0);

        if (!hasSearch && ingress.source.pushdown && !hasResidual && this.state.searchFilters.length === 0) {
            const query: PushdownQuery = {
                limit: 1,
                offset: offset > 0 ? offset : undefined,
                orders: this.state.pushdownOrders.length > 0 ? this.state.pushdownOrders : undefined,
                predicates: plan.pushdownPredicates.length > 0 ? plan.pushdownPredicates : undefined,
            };
            const pushed = ingress.source.pushdown(query);
            if (pushed) {
                for await (const item of pushed) {return item;}
                return undefined;
            }
        }

        if (hasOrders) {
            const items = await (this.result() as Promise<Array<T>>);
            return items[0];
        }

        if (hasSearch) {
            const prefilterContext = createPrefilterContext(plan);
            const result = await executeSearchPipelineAsync<T, SearchCapabilityState>(
                ingress.stream(prefilterContext?.streamOptions),
                plan.predicates,
                plan.predicateFn,
                plan.cache,
                plan.fuzzyConfig,
                plan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
                plan.searchFilters,
                false,
                offset + 1,
                plan.id
            );
            emitPrefilterMetrics(plan.id, prefilterContext, { phase: "executeFirstAsync.search" });
            return result.items[offset];
        }

        const predicateFn = plan.predicateFn;
        const residualPredicateFn = plan.residualPredicateFn;
        let seen = 0;

        if (residualPredicateFn && residualPredicateFn !== predicateFn) {
            const prefilterContext = createPrefilterContext(plan);
            for await (const item of ingress.stream(prefilterContext?.streamOptions)) {
                if (!predicateFn(item)) {continue;}
                if (!residualPredicateFn(item)) {continue;}
                if (seen++ < offset) {continue;}
                emitPrefilterMetrics(plan.id, prefilterContext, { phase: "executeFirstAsync" });
                return item;
            }
            emitPrefilterMetrics(plan.id, prefilterContext, { phase: "executeFirstAsync" });
            return undefined;
        }

        const prefilterContext = createPrefilterContext(plan);
        for await (const item of ingress.stream(prefilterContext?.streamOptions)) {
            if (!predicateFn(item)) {continue;}
            if (seen++ < offset) {continue;}
            emitPrefilterMetrics(plan.id, prefilterContext, { phase: "executeFirstAsync" });
            return item;
        }
        emitPrefilterMetrics(plan.id, prefilterContext, { phase: "executeFirstAsync" });
        return undefined;
    }

    private async executeAsync(plan: QueryPlan<T>, runTiming?: TimingToken | null): Promise<Array<T>> {
        const rootTiming = runTiming ?? startTiming("egress", "run.execute", plan.id);
        const timing = startTiming("egress", "egress.executeAsync", plan.id, rootTiming);
        emitMarker("egress", "egress.execute.start", plan.id, timing);
        const limit = this.state.limitCount;
        const offset = this.state.offsetCount;
        const hasOrders = this.state.orders.length > 0;
        const windowLimit = limit !== null ? limit + offset : undefined;
        const ingress = this.state.ingress as AsyncIngressEngine<T>;
        const hasSearch = plan.searchFilters.length > 0;
        const hasResidual = !!(plan.residualPredicates && plan.residualPredicates.length > 0);

        if (
            ingress.source.pushdown
            && !hasResidual
            && !hasSearch
            && this.state.searchFilters.length === 0
            && !(offset > 0 && limit === null)
        ) {
            const pushdownTiming = startTiming("egress", "egress.pushdownAttempt", plan.id, timing);
            const query: PushdownQuery = {
                limit: limit === null ? undefined : limit,
                offset: offset > 0 ? offset : undefined,
                orders: this.state.pushdownOrders.length > 0 ? this.state.pushdownOrders : undefined,
                predicates: plan.pushdownPredicates.length > 0 ? plan.pushdownPredicates : undefined,
            };
            logPlanner({
                source: "egress",
                type: "pushdown",
                planId: plan.id,
                data: {
                    applied: false,
                    eligible: true,
                    query,
                },
            });
            const pushed = ingress.source.pushdown(query);
            if (pushed) {
                const output: Array<T> = [];
                for await (const item of pushed) {output.push(item);}
                endTiming(pushdownTiming, { skipData: true });
                logPlanner({
                    source: "egress",
                    type: "final",
                    planId: plan.id,
                    data: {
                        mode: "async",
                        path: "pushdown",
                        resultCount: output.length,
                    },
                });
                emitMetrics({
                    source: "egress",
                    planId: plan.id,
                    metrics: {
                        extras: {
                            path: "pushdown",
                            hasOrders,
                            hasSearch,
                            limit,
                            offset,
                        },
                    },
                });
                emitMarker("egress", "egress.result.final", plan.id, timing);
                endTiming(timing, { skipData: true });
                endTiming(rootTiming, { skipData: true });
                return output;
            }
            endTiming(pushdownTiming, { skipData: true });
        }

        if (
            !ingress.source.pushdown
            || hasResidual
            || hasSearch
            || this.state.searchFilters.length > 0
            || (offset > 0 && limit === null)
        ) {
            logPlanner({
                source: "egress",
                type: "pushdown",
                planId: plan.id,
                data: {
                    applied: false,
                    eligible: false,
                    reason: {
                        hasResidual,
                        hasSearch,
                        hasSearchFilters: this.state.searchFilters.length > 0,
                        ingressSupportsPushdown: !!ingress.source.pushdown,
                        offsetWithoutLimit: offset > 0 && limit === null,
                    },
                },
            });
        }

        const executor = new AsyncExecutionEngine<T>();
        const filtered = await executor.execute(
            ingress,
            plan,
            {
                requiresGrouping: false,
                requiresOrdering: hasOrders,
                windowLimit,
                timingParent: timing,
            }
        );
        logPlanner({
            source: "egress",
            type: "final",
            planId: plan.id,
            data: {
                mode: "async",
                path: "local",
                hasOrders,
                hasSearch,
                residualPredicates: plan.residualPredicates?.length ?? 0,
                resultCount: filtered.length,
            },
        });
        emitMetrics({
            source: "egress",
            planId: plan.id,
            metrics: {
                extras: {
                    path: "local",
                    hasOrders,
                    hasSearch,
                    limit,
                    offset,
                    windowLimit,
                },
            },
        });
        const finalizeTiming = startTiming("egress", "egress.finalizeOrder", plan.id, timing);
        const finalized = finalizeOrderAndWindow(filtered, this.state.orders, this.state.limitCount, this.state.offsetCount);
        endTiming(finalizeTiming, { skipData: true });
        emitMarker("egress", "egress.result.final", plan.id, timing);
        endTiming(timing, { skipData: true });
        endTiming(rootTiming, { skipData: true });
        return finalized;
    }



    paginate(options: PaginationOptions): M extends "async" ? Promise<PaginationCursor<T>> : PaginationCursor<T> {
        return this.executePaginate(options) as M extends "async" ? Promise<PaginationCursor<T>> : PaginationCursor<T>;
    }

    private executePaginate(options: PaginationOptions): PaginationCursor<T> | Promise<PaginationCursor<T>> {
        const safeSize = Number.isFinite(options.pageSize) && options.pageSize > 0
            ? Math.floor(options.pageSize)
            : 1;
        const safePage = Number.isFinite(options.page) && options.page! > 0
            ? Math.floor(options.page!)
            : 1;
        const totalMode = options.total ?? "none";

        if (this.state.ingress instanceof Object && "mode" in this.state.ingress && this.state.ingress.mode === "async") {
            return this.paginateAsync(safePage, safeSize, totalMode);
        }
        const data = this.result() as Array<T>;
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
            for (let i = start; i < last; i++) { buffer.push(data[i]!); }
        };

        fillPage(cursor.data, cursor.page);
        return cursor;
    }

    private async paginateAsync(
        safePage: number,
        safeSize: number,
        totalMode: PaginationOptions["total"]
    ): Promise<PaginationCursor<T>> {
        const data = await (this.result() as Promise<Array<T>>);
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
            for (let i = start; i < last; i++) { buffer.push(data[i]!); }
        };

        fillPage(cursor.data, cursor.page);
        return cursor;
    }

    groupBy<P extends GroupablePaths<T>>(
        field: P,
        options?: { date?: boolean }
    ): M extends "async" ? Promise<Map<GroupKey<T, P>, Array<T>>> : Map<GroupKey<T, P>, Array<T>> {
        return this.executeGroupBy(field, options) as M extends "async"
            ? Promise<Map<GroupKey<T, P>, Array<T>>>
            : Map<GroupKey<T, P>, Array<T>>;
    }

    private executeGroupBy<P extends GroupablePaths<T>>(
        field: P,
        options?: { date?: boolean }
    ): Map<GroupKey<T, P>, Array<T>> | Promise<Map<GroupKey<T, P>, Array<T>>> {
        if (this.state.ingress instanceof Object && "mode" in this.state.ingress && this.state.ingress.mode === "async") {
            return this.groupByAsync(field, options);
        }
        const items = this.result() as Array<T>;
        const accessors = getPathAccessors(this.state.plan.cache, String(field));
        const convert = options?.date ? this.state.plan.cache.groupKeyConverterDate : this.state.plan.cache.groupKeyConverter;
        const result = new Map<GroupKeyValue, Array<T>>();
        for (let i = 0; i < items.length; i++) {
            const item = items[i]!;
            accessors.forEach(item as ResolveObject, (value) => {
                const key = convert(value);
                if (key === null || key === undefined) {return;}
                const bucket = result.get(key);
                if (bucket) { bucket.push(item); }
                else { result.set(key, [item]); }
            });
        }
        return result as Map<GroupKey<T, P>, Array<T>>;
    }

    private async groupByAsync<P extends GroupablePaths<T>>(
        field: P,
        options?: { date?: boolean }
    ): Promise<Map<GroupKey<T, P>, Array<T>>> {
        const executor = new AsyncExecutionEngine<T>();
        const filtered = await executor.execute(
            this.state.ingress as AsyncIngressEngine<T>,
            this.state.searchFilters.length === 0
                ? this.state.plan
                : {
                    ...this.state.plan,
                    searchFilters: [...this.state.plan.searchFilters, ...this.state.searchFilters],
                },
            { requiresGrouping: true, requiresOrdering: false }
        );
        const accessors = getPathAccessors(this.state.plan.cache, String(field));
        const convert = options?.date ? this.state.plan.cache.groupKeyConverterDate : this.state.plan.cache.groupKeyConverter;
        const result = new Map<GroupKeyValue, Array<T>>();
        for (let i = 0; i < filtered.length; i++) {
            const item = filtered[i]!;
            accessors.forEach(item as ResolveObject, (value) => {
                const key = convert(value);
                if (key === null || key === undefined) {return;}
                const bucket = result.get(key);
                if (bucket) { bucket.push(item); }
                else { result.set(key, [item]); }
            });
        }
        return result as Map<GroupKey<T, P>, Array<T>>;
    }

    resultWithMetadata(): M extends "async"
        ? Promise<Array<{ item: T; score?: number; tagMask?: number }>>
        : Array<{ item: T; score?: number; tagMask?: number }> {
        return this.executeResultWithMetadata() as M extends "async"
            ? Promise<Array<{ item: T; score?: number; tagMask?: number }>>
            : Array<{ item: T; score?: number; tagMask?: number }>;
    }

    private executeResultWithMetadata():
        | Array<{ item: T; score?: number; tagMask?: number }>
        | Promise<Array<{ item: T; score?: number; tagMask?: number }>> {
        const mergedPlan = this.state.searchFilters.length === 0
            ? this.state.plan
            : {
                ...this.state.plan,
                searchFilters: [...this.state.plan.searchFilters, ...this.state.searchFilters],
            };
        if (this.state.ingress instanceof Object && "mode" in this.state.ingress && this.state.ingress.mode === "async") {
            return this.resultWithMetadataAsync(mergedPlan);
        }
        if (mergedPlan.searchFilters.length === 0) {
            return (this.result() as Array<T>).map(item => ({ item }));
        }
        const result = executeSearchPipeline<T, SearchCapabilityState>(
            (this.state.ingress as IngressEngine<T>).data,
            mergedPlan.predicates,
            mergedPlan.predicateFn,
            mergedPlan.cache,
            mergedPlan.fuzzyConfig,
            mergedPlan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
            mergedPlan.searchFilters,
            true,
            mergedPlan.id
        );
        return mapSearchMetadata(result);
    }

    private async resultWithMetadataAsync(
        mergedPlan: QueryPlan<T>
    ): Promise<Array<{ item: T; score?: number; tagMask?: number }>> {
        if (mergedPlan.searchFilters.length === 0) {
            const items = await (this.result() as Promise<Array<T>>);
            return items.map(item => ({ item }));
        }
        const data = await (this.state.ingress as AsyncIngressEngine<T>).materialize();
        const result = executeSearchPipeline<T, SearchCapabilityState>(
            data,
            mergedPlan.predicates,
            mergedPlan.predicateFn,
            mergedPlan.cache,
            mergedPlan.fuzzyConfig,
            mergedPlan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
            mergedPlan.searchFilters,
            true,
            mergedPlan.id
        );
        return mapSearchMetadata(result);
    }
}

function finalizeOrderAndWindow<T extends Record<string, unknown>>(
    filtered: Array<T>,
    orders?: Array<OrderSpec>,
    limitCount?: number | null,
    offsetCount?: number
): Array<T> {
    const effectiveOrders = orders ?? [];
    const effectiveLimit = limitCount ?? null;
    const effectiveOffset = offsetCount ?? 0;
    if (effectiveOrders.length === 0) {
        if (effectiveLimit === null && effectiveOffset === 0) {return filtered;}
        const start = effectiveOffset > 0 ? effectiveOffset : 0;
        const end = effectiveLimit === null ? filtered.length : start + effectiveLimit;
        return filtered.slice(start, end);
    }

    const orderCount = effectiveOrders.length;
    const resolveOrders = new Array<(obj: ResolveObject) => SortKey | null>(orderCount);
    for (let i = 0; i < orderCount; i++) {
        const order = effectiveOrders[i]!;
        resolveOrders[i] = (obj: ResolveObject) => resolveOrderValueWithSegments(obj, order.segments, order.resolve);
    }
    const compare = createComparator<T>(effectiveOrders);
    const entries: Array<Orderable<T>> = [];
    const pushEntry = (item: T, index: number) => {
        if (orderCount === 1) {
            entries.push({
                index,
                item,
                k0: resolveOrders[0]!(item as ResolveObject),
            } as Orderable1<T>);
            return;
        }
        if (orderCount === 2) {
            entries.push({
                index,
                item,
                k0: resolveOrders[0]!(item as ResolveObject),
                k1: resolveOrders[1]!(item as ResolveObject),
            } as Orderable2<T>);
            return;
        }
        if (orderCount === 3) {
            entries.push({
                index,
                item,
                k0: resolveOrders[0]!(item as ResolveObject),
                k1: resolveOrders[1]!(item as ResolveObject),
                k2: resolveOrders[2]!(item as ResolveObject),
            } as Orderable3<T>);
            return;
        }
        const keys = new Array<SortKey | null>(orderCount);
        for (let j = 0; j < orderCount; j++) {
            keys[j] = resolveOrders[j]!(item as ResolveObject);
        }
        entries.push({ index, item, keys } as OrderableN<T>);
    };

    const start = effectiveOffset > 0 ? effectiveOffset : 0;
    const needsHeap = effectiveLimit !== null && effectiveLimit > 0 && filtered.length > effectiveLimit;
    if (needsHeap) {
        const heapLimit = start + effectiveLimit;
        if (heapLimit <= 0) {return [] as Array<T>;}
        const heap: Array<Orderable<T>> = [];
        const insertCandidate = (candidate: Orderable<T>) => {
            if (heap.length < heapLimit) {
                heap.push(candidate);
                let idx = heap.length - 1;
                while (idx > 0) {
                    const parent = (idx - 1) >> 1;
                    if (compare(heap[idx]!, heap[parent]!) <= 0) {break;}
                    const temp = heap[parent]!;
                    heap[parent] = heap[idx]!;
                    heap[idx] = temp;
                    idx = parent;
                }
                return;
            }
            if (compare(candidate, heap[0]!) >= 0) {return;}
            heap[0] = candidate;
            let idx = 0;
            const length = heap.length;
            while (true) {
                const left = (idx << 1) + 1;
                if (left >= length) {break;}
                const right = left + 1;
                let nextIdx = left;
                if (right < length && compare(heap[right]!, heap[left]!) > 0) { nextIdx = right; }
                if (compare(heap[nextIdx]!, heap[idx]!) <= 0) {break;}
                const temp = heap[idx]!;
                heap[idx] = heap[nextIdx]!;
                heap[nextIdx] = temp;
                idx = nextIdx;
            }
        };

        for (let i = 0; i < filtered.length; i++) {
            const item = filtered[i]!;
            const index = i;
            if (orderCount === 1) {
                insertCandidate({
                    index,
                    item,
                    k0: resolveOrders[0]!(item as ResolveObject),
                } as Orderable1<T>);
                continue;
            }
            if (orderCount === 2) {
                insertCandidate({
                    index,
                    item,
                    k0: resolveOrders[0]!(item as ResolveObject),
                    k1: resolveOrders[1]!(item as ResolveObject),
                } as Orderable2<T>);
                continue;
            }
            if (orderCount === 3) {
                insertCandidate({
                    index,
                    item,
                    k0: resolveOrders[0]!(item as ResolveObject),
                    k1: resolveOrders[1]!(item as ResolveObject),
                    k2: resolveOrders[2]!(item as ResolveObject),
                } as Orderable3<T>);
                continue;
            }
            const keys = new Array<SortKey | null>(orderCount);
            for (let j = 0; j < orderCount; j++) {
                keys[j] = resolveOrders[j]!(item as ResolveObject);
            }
            insertCandidate({ index, item, keys } as OrderableN<T>);
        }

        heap.sort(compare);
        for (let i = 0; i < heap.length; i++) { entries.push(heap[i]!); }
    } else {
        for (let i = 0; i < filtered.length; i++) {
            const item = filtered[i]!;
            pushEntry(item, i);
        }
        entries.sort(compare);
    }

    const end = effectiveLimit === null ? entries.length : start + effectiveLimit;
    const result: Array<T> = [];
    const last = end < entries.length ? end : entries.length;
    for (let i = start; i < last; i++) { result.push(entries[i]!.item); }
    return result;
}

function mapSearchMetadata<T>(
    result: {
        items: Array<T>;
        scores?: Array<number>;
        tagMasks?: Array<number>;
    }
): Array<{ item: T; score?: number; tagMask?: number }> {
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
