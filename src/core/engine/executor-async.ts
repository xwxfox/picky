import type { QueryPlan } from "@/core/engine/plan";
import type { AsyncIngressEngine } from "@/io/ingress";
import { executeSearchPipeline, executeSearchPipelineAsync } from "@/core/search/runtime";
import type { SearchCapabilityState, AvailableTags } from "@/types/search";
import type { CompiledTaggerConfig } from "@/core/search/runtime";
import { planIngress } from "@/core/engine/ingress-plan";

export class AsyncExecutionEngine<T extends Record<string, unknown>> {
    async execute(
        ingress: AsyncIngressEngine<T>,
        plan: QueryPlan<T>,
        options?: { requiresGrouping?: boolean; requiresOrdering?: boolean; windowLimit?: number; }
    ): Promise<Array<T>> {
        const hasSearch = plan.searchFilters.length > 0;
        const predicates = plan.predicates;
        const predicateFn = plan.predicateFn;
        const residualPredicateFn = plan.residualPredicateFn;
        if (plan.alwaysFalse) {
            return [];
        }
        const requiresGrouping = options?.requiresGrouping ?? false;
        const requiresOrdering = options?.requiresOrdering ?? false;
        const ingressPlan = planIngress(
            ingress.hints,
            ingress.capabilities,
            requiresOrdering,
            requiresGrouping,
            hasSearch
        );
        const shouldStream = ingressPlan.strategy === "stream";

        const windowLimit = options?.windowLimit;

        if (!shouldStream) {
            const data = await ingress.materialize();
            if (predicates.length === 0 && !hasSearch) {return [...data];}
            if (hasSearch) {
                const result = executeSearchPipeline<T, SearchCapabilityState>(
                    data,
                    predicates,
                    predicateFn,
                    plan.cache,
                    plan.fuzzyConfig,
                    plan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
                    plan.searchFilters,
                    false
                );
                return result.items;
            }
            const output: Array<T> = [];
            if (residualPredicateFn && residualPredicateFn !== predicateFn) {
                for (let i = 0; i < data.length; i++) {
                    const item = data[i]!;
                    if (!predicateFn(item)) {continue;}
                    if (!residualPredicateFn(item)) {continue;}
                    output.push(item);
                }
                return output;
            }
            for (let i = 0; i < data.length; i++) {
                const item = data[i]!;
                if (!predicateFn(item)) {continue;}
                output.push(item);
            }
            return output;
        }

        if (hasSearch) {
            if (shouldStream) {
                const result = await executeSearchPipelineAsync<T, SearchCapabilityState>(
                    ingress.stream(),
                    predicates,
                    predicateFn,
                    plan.cache,
                    plan.fuzzyConfig,
                    plan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
                    plan.searchFilters,
                    false,
                    windowLimit
                );
                return result.items;
            }
            const data = await ingress.materialize();
            const result = executeSearchPipeline<T, SearchCapabilityState>(
                data,
                predicates,
                predicateFn,
                plan.cache,
                plan.fuzzyConfig,
                plan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
                plan.searchFilters,
                false
            );
            return result.items;
        }

        const output: Array<T> = [];
        if (shouldStream) {
            const limit = windowLimit ?? 0;
            for await (const item of ingress.stream()) {
                if (!predicateFn(item)) {continue;}
                if (residualPredicateFn && residualPredicateFn !== predicateFn && !residualPredicateFn(item)) {
                    continue;
                }
                output.push(item);
                if (limit > 0 && output.length >= limit) {break;}
            }
            return output;
        }
        const data = await ingress.materialize();
        if (residualPredicateFn && residualPredicateFn !== predicateFn) {
            for (let i = 0; i < data.length; i++) {
                const item = data[i]!;
                if (!predicateFn(item)) {continue;}
                if (!residualPredicateFn(item)) {continue;}
                output.push(item);
            }
            return output;
        }
        for (let i = 0; i < data.length; i++) {
            const item = data[i]!;
            if (!predicateFn(item)) {continue;}
            output.push(item);
        }
        return output;
    }
}
