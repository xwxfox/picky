import type { QueryPlan } from "@/core/engine/plan";
import type { AsyncIngressEngine } from "@/io/ingress";
import { executeSearchPipeline, executeSearchPipelineAsync } from "@/core/search/runtime";
import type { SearchCapabilityState, AvailableTags } from "@/types/search";
import type { CompiledTaggerConfig } from "@/core/search/runtime";
import { planIngress } from "@/core/engine/ingress-plan";
import { emitMarker, emitMetrics, endTiming, startTiming } from "@/core/engine/telemetry";
import type { TimingToken } from "@/core/engine/telemetry";
import { createPredicateExecutionMetrics, shouldCollectDeepMetrics } from "@/core/engine/metrics";
import { nowMs } from "@/core/engine/telemetry-time";
import { createPrefilterContext, emitPrefilterMetrics } from "@/core/engine/prefilter-plan";

export class AsyncExecutionEngine<T extends Record<string, unknown>> {
    async execute(
        ingress: AsyncIngressEngine<T>,
        plan: QueryPlan<T>,
        options?: { requiresGrouping?: boolean; requiresOrdering?: boolean; windowLimit?: number; timingParent?: TimingToken | null; }
    ): Promise<Array<T>> {
        const hasSearch = plan.searchFilters.length > 0;
        const predicates = plan.predicates;
        const predicateFn = plan.predicateFn;
        const residualPredicateFn = plan.residualPredicateFn;
        const execTiming = startTiming("execution", "execution.executeAsync", plan.id, options?.timingParent ?? null);
        emitMarker("execution", "execution.execute.start", plan.id, execTiming);
        const collectMetrics = shouldCollectDeepMetrics();
        const predicateMetrics = collectMetrics ? createPredicateExecutionMetrics() : null;
        if (plan.alwaysFalse) {
            emitMarker("execution", "execution.execute.end", plan.id, execTiming);
            endTiming(execTiming, { skipData: true });
            return [];
        }
        const requiresGrouping = options?.requiresGrouping ?? false;
        const requiresOrdering = options?.requiresOrdering ?? false;
        const ingressPlan = planIngress(
            ingress.hints,
            ingress.capabilities,
            requiresOrdering,
            requiresGrouping,
            hasSearch,
            execTiming
        );
        const shouldStream = ingressPlan.strategy === "stream";

        const windowLimit = options?.windowLimit;

        if (!shouldStream) {
            const prefilterContext = createPrefilterContext(plan, execTiming);
            const loadTiming = startTiming("execution", "execution.load.materialize", plan.id, execTiming);
            if (prefilterContext) {
                prefilterContext.streamOptions.timingParent = loadTiming?.spanId ?? null;
            }
            const data = await ingress.materialize(prefilterContext?.streamOptions);
            endTiming(loadTiming, { skipData: true });
            if (predicates.length === 0 && !hasSearch) {
                emitMarker("execution", "execution.execute.end", plan.id, execTiming);
                endTiming(execTiming, { skipData: true });
                emitPrefilterMetrics(plan.id, prefilterContext, { phase: "executeAsync.materialize" });
                return [...data];
            }
            if (hasSearch) {
                const searchTiming = startTiming("execution", "execution.searchPipeline", plan.id, execTiming);
                const result = executeSearchPipeline<T, SearchCapabilityState>(
                    data,
                    predicates,
                    predicateFn,
                    plan.cache,
                    plan.fuzzyConfig,
                    plan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
                    plan.searchFilters,
                    false,
                    plan.id,
                    execTiming
                );
                endTiming(searchTiming, { skipData: true });
                emitMarker("execution", "execution.execute.end", plan.id, execTiming);
                endTiming(execTiming, { skipData: true });
                emitPrefilterMetrics(plan.id, prefilterContext, { phase: "executeAsync.materialize.search" });
                return result.items;
            }
            const output: Array<T> = [];
            const useMetrics = predicateMetrics !== null && plan.predicateSpecs && plan.predicateSpecs.length > 0;
            if (residualPredicateFn && residualPredicateFn !== predicateFn) {
                for (let i = 0; i < data.length; i++) {
                    const item = data[i]!;
                    let ok = true;
                    if (useMetrics) {
                        for (let p = 0; p < plan.predicateSpecs!.length; p++) {
                            const spec = plan.predicateSpecs![p]!;
                            const start = nowMs();
                            const passed = spec.predicate(item);
                            const elapsed = nowMs() - start;
                            predicateMetrics!.counts[spec.op] = (predicateMetrics!.counts[spec.op] ?? 0) + 1;
                            predicateMetrics!.durationsMs[spec.op] = (predicateMetrics!.durationsMs[spec.op] ?? 0) + elapsed;
                            if (!passed) {ok = false; break;}
                        }
                        if (!ok) {continue;}
                    } else if (!predicateFn(item)) {
                        continue;
                    }
                    if (!residualPredicateFn(item)) {continue;}
                    output.push(item);
                }
                if (useMetrics && predicateMetrics) {
                    emitMetrics({
                        source: "execution",
                        planId: plan.id,
                        metrics: {
                            counts: predicateMetrics.counts,
                            durationsMs: predicateMetrics.durationsMs,
                            extras: { phase: "executeAsync" },
                        },
                    });
                }
                emitPrefilterMetrics(plan.id, prefilterContext, { phase: "executeAsync.materialize" });
                emitMarker("execution", "execution.execute.end", plan.id, execTiming);
                endTiming(execTiming, { skipData: true });
                return output;
            }
            for (let i = 0; i < data.length; i++) {
                const item = data[i]!;
                let ok = true;
                if (useMetrics) {
                    for (let p = 0; p < plan.predicateSpecs!.length; p++) {
                        const spec = plan.predicateSpecs![p]!;
                        const start = nowMs();
                        const passed = spec.predicate(item);
                        const elapsed = nowMs() - start;
                        predicateMetrics!.counts[spec.op] = (predicateMetrics!.counts[spec.op] ?? 0) + 1;
                        predicateMetrics!.durationsMs[spec.op] = (predicateMetrics!.durationsMs[spec.op] ?? 0) + elapsed;
                        if (!passed) {ok = false; break;}
                    }
                    if (!ok) {continue;}
                } else if (!predicateFn(item)) {
                    continue;
                }
                output.push(item);
            }
            if (useMetrics && predicateMetrics) {
                emitMetrics({
                    source: "execution",
                    planId: plan.id,
                    metrics: {
                        counts: predicateMetrics.counts,
                        durationsMs: predicateMetrics.durationsMs,
                        extras: { phase: "executeAsync" },
                    },
                });
            }
            emitPrefilterMetrics(plan.id, prefilterContext, { phase: "executeAsync.materialize" });
            emitMarker("execution", "execution.execute.end", plan.id, execTiming);
            endTiming(execTiming, { skipData: true });
            return output;
        }

        if (hasSearch) {
            if (shouldStream) {
                const searchTiming = startTiming("execution", "execution.searchPipelineAsync", plan.id, execTiming);
                const prefilterContext = createPrefilterContext(plan, execTiming);                const result = await executeSearchPipelineAsync<T, SearchCapabilityState>(
                    ingress.stream(prefilterContext?.streamOptions),
                    predicates,
                    predicateFn,
                    plan.cache,
                    plan.fuzzyConfig,
                    plan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
                    plan.searchFilters,
                    false,
                    windowLimit,
                    plan.id,
                    execTiming
                );
                endTiming(searchTiming, { skipData: true });
                emitPrefilterMetrics(plan.id, prefilterContext, { phase: "executeAsync.search" });
                emitMarker("execution", "execution.execute.end", plan.id, execTiming);
                endTiming(execTiming, { skipData: true });
                return result.items;
            }
            const data = await ingress.materialize();
            const searchTiming = startTiming("execution", "execution.searchPipeline", plan.id, execTiming);
            const result = executeSearchPipeline<T, SearchCapabilityState>(
                data,
                predicates,
                predicateFn,
                plan.cache,
                plan.fuzzyConfig,
                plan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
                plan.searchFilters,
                false,
                plan.id,
                execTiming
            );
            endTiming(searchTiming, { skipData: true });
            emitMarker("execution", "execution.execute.end", plan.id, execTiming);
            endTiming(execTiming, { skipData: true });
            return result.items;
        }

        const output: Array<T> = [];
        if (shouldStream) {
            const streamTiming = startTiming("execution", "execution.executeAsync.stream", plan.id, execTiming);
            emitMarker("execution", "execution.load.stream", plan.id, execTiming);
            const limit = windowLimit ?? 0;
            const useMetrics = predicateMetrics !== null && plan.predicateSpecs && plan.predicateSpecs.length > 0;
            const prefilterContext = createPrefilterContext(plan, execTiming);
            for await (const item of ingress.stream(prefilterContext?.streamOptions)) {
                let ok = true;
                if (useMetrics) {
                    for (let p = 0; p < plan.predicateSpecs!.length; p++) {
                        const spec = plan.predicateSpecs![p]!;
                        const start = nowMs();
                        const passed = spec.predicate(item);
                        const elapsed = nowMs() - start;
                        predicateMetrics!.counts[spec.op] = (predicateMetrics!.counts[spec.op] ?? 0) + 1;
                        predicateMetrics!.durationsMs[spec.op] = (predicateMetrics!.durationsMs[spec.op] ?? 0) + elapsed;
                        if (!passed) {ok = false; break;}
                    }
                    if (!ok) {continue;}
                } else if (!predicateFn(item)) {
                    continue;
                }
                if (residualPredicateFn && residualPredicateFn !== predicateFn && !residualPredicateFn(item)) {
                    continue;
                }
                output.push(item);
                if (limit > 0 && output.length >= limit) {break;}
            }
            endTiming(streamTiming, { skipData: true });
            if (useMetrics && predicateMetrics) {
                emitMetrics({
                    source: "execution",
                    planId: plan.id,
                    metrics: {
                        counts: predicateMetrics.counts,
                        durationsMs: predicateMetrics.durationsMs,
                        extras: { phase: "executeAsync.stream" },
                    },
                });
            }
            emitPrefilterMetrics(plan.id, prefilterContext, { phase: "executeAsync.stream" });
            emitMarker("execution", "execution.execute.end", plan.id, execTiming);
            endTiming(execTiming, { skipData: true });
            return output;
        }
        const loadTiming = startTiming("execution", "execution.load.materialize", plan.id, execTiming);
        const data = await ingress.materialize();
        endTiming(loadTiming, { skipData: true });
        const useMetrics = predicateMetrics !== null && plan.predicateSpecs && plan.predicateSpecs.length > 0;
        if (residualPredicateFn && residualPredicateFn !== predicateFn) {
            for (let i = 0; i < data.length; i++) {
                const item = data[i]!;
                let ok = true;
                if (useMetrics) {
                    for (let p = 0; p < plan.predicateSpecs!.length; p++) {
                        const spec = plan.predicateSpecs![p]!;
                        const start = nowMs();
                        const passed = spec.predicate(item);
                        const elapsed = nowMs() - start;
                        predicateMetrics!.counts[spec.op] = (predicateMetrics!.counts[spec.op] ?? 0) + 1;
                        predicateMetrics!.durationsMs[spec.op] = (predicateMetrics!.durationsMs[spec.op] ?? 0) + elapsed;
                        if (!passed) {ok = false; break;}
                    }
                    if (!ok) {continue;}
                } else if (!predicateFn(item)) {
                    continue;
                }
                if (!residualPredicateFn(item)) {continue;}
                output.push(item);
            }
            if (useMetrics && predicateMetrics) {
                emitMetrics({
                    source: "execution",
                    planId: plan.id,
                    metrics: {
                        counts: predicateMetrics.counts,
                        durationsMs: predicateMetrics.durationsMs,
                        extras: { phase: "executeAsync.materialize" },
                    },
                });
            }
            emitMarker("execution", "execution.execute.end", plan.id, execTiming);
            endTiming(execTiming, { skipData: true });
            return output;
        }
        for (let i = 0; i < data.length; i++) {
            const item = data[i]!;
            let ok = true;
            if (useMetrics) {
                for (let p = 0; p < plan.predicateSpecs!.length; p++) {
                    const spec = plan.predicateSpecs![p]!;
                    const start = nowMs();
                    const passed = spec.predicate(item);
                    const elapsed = nowMs() - start;
                    predicateMetrics!.counts[spec.op] = (predicateMetrics!.counts[spec.op] ?? 0) + 1;
                    predicateMetrics!.durationsMs[spec.op] = (predicateMetrics!.durationsMs[spec.op] ?? 0) + elapsed;
                    if (!passed) {ok = false; break;}
                }
                if (!ok) {continue;}
            } else if (!predicateFn(item)) {
                continue;
            }
            output.push(item);
        }
        if (useMetrics && predicateMetrics) {
            emitMetrics({
                source: "execution",
                planId: plan.id,
                metrics: {
                    counts: predicateMetrics.counts,
                    durationsMs: predicateMetrics.durationsMs,
                    extras: { phase: "executeAsync.materialize" },
                },
            });
        }
        emitMarker("execution", "execution.execute.end", plan.id, execTiming);
        endTiming(execTiming, { skipData: true });
        return output;
    }
}
