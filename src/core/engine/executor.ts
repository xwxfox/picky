import type { QueryPlan } from "./plan";
import type { IngressEngine } from "@/io/ingress";
import { executeSearchPipeline } from "@/core/search/runtime";
import type { SearchCapabilityState, AvailableTags } from "@/types/search";
import type { CompiledTaggerConfig } from "@/core/search/runtime";

export class ExecutionEngine<T extends Record<string, unknown>> {
    execute(ingress: IngressEngine<T>, plan: QueryPlan<T>): Array<T> {
        const data = ingress.data;
        const predicates = plan.predicates;
        const predicateFn = plan.predicateFn;
        const residualPredicateFn = plan.residualPredicateFn;
        const hasSearch = plan.searchFilters.length > 0;
        if (plan.alwaysFalse) {
            return [];
        }
        if (predicates.length === 0 && !hasSearch) {
            return [...data];
        }
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
        const result: Array<T> = [];
        if (residualPredicateFn && residualPredicateFn !== predicateFn) {
            for (let i = 0; i < data.length; i++) {
                const item = data[i]!;
                if (!predicateFn(item)) {continue;}
                if (!residualPredicateFn(item)) {continue;}
                result.push(item);
            }
            return result;
        }
        for (let i = 0; i < data.length; i++) {
            const item = data[i]!;
            if (!predicateFn(item)) {continue;}
            result.push(item);
        }
        return result;
    }
}
