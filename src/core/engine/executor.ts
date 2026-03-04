import type { QueryPlan } from "./plan";
import type { IngressEngine } from "@/io/ingress";
import { executeSearchPipeline } from "@/core/search/runtime";
import type { SearchCapabilityState, AvailableTags } from "@/types/search";
import type { CompiledTaggerConfig } from "@/core/search/runtime";

export class ExecutionEngine<T extends Record<string, unknown>> {
    execute(ingress: IngressEngine<T>, plan: QueryPlan<T>): Array<T> {
        const data = ingress.data;
        const predicates = plan.predicates;
        const hasSearch = plan.searchFilters.length > 0;
        if (predicates.length === 0 && !hasSearch) {
            return [...data];
        }
        if (hasSearch) {
            const result = executeSearchPipeline<T, SearchCapabilityState>(
                data,
                predicates,
                plan.cache,
                plan.fuzzyConfig,
                plan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
                plan.searchFilters,
                false
            );
            return result.items;
        }
        const result: Array<T> = [];
        outer: for (let i = 0; i < data.length; i++) {
            const item = data[i]!;
            for (let p = 0; p < predicates.length; p++) {
                if (!predicates[p]!(item)) {continue outer;}
            }
            result.push(item);
        }
        return result;
    }
}
