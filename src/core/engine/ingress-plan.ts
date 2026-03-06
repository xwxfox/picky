import type { IngressCapabilities, IngressHints } from "@/io/ingress/types";
import { logPlanner } from "./planner-logger";

export type IngressStrategy = "eager" | "stream";

export type IngressPlan = {
    strategy: IngressStrategy;
};

const defaultEagerThreshold = 50_000;
const defaultMaxMemoryBytes = 128 * 1024 * 1024;

export function planIngress<T extends Record<string, unknown>>(
    hints: IngressHints<T> | undefined,
    capabilities: IngressCapabilities,
    requiresOrdering: boolean,
    requiresGrouping: boolean,
    requiresSearch: boolean
): IngressPlan {
    const planId = "ingress";
    logPlanner({
        source: "ingress",
        type: "input",
        planId,
        data: {
            capabilities,
            hints,
            requiresGrouping,
            requiresOrdering,
            requiresSearch,
        },
    });
    const preferStreaming = hints?.preferStreaming ?? false;
    if (preferStreaming && !(requiresOrdering || requiresGrouping || requiresSearch)) {
        const plan = { strategy: "stream" } as const;
        logPlanner({
            source: "ingress",
            type: "final",
            planId,
            data: { plan, reason: "preferStreaming" },
        });
        return plan;
    }

    if (requiresOrdering || requiresGrouping) {
        const plan = (capabilities.order || capabilities.group)
            ? ({ strategy: "stream" } as const)
            : ({ strategy: "eager" } as const);
        logPlanner({
            source: "ingress",
            type: "final",
            planId,
            data: {
                plan,
                reason: "ordering/grouping",
                supported: { group: capabilities.group, order: capabilities.order },
            },
        });
        return plan;
    }

    if (requiresSearch && capabilities.search) {
        const plan = { strategy: "stream" } as const;
        logPlanner({
            source: "ingress",
            type: "final",
            planId,
            data: { plan, reason: "search" },
        });
        return plan;
    }

    const estimatedCount = hints?.estimatedCount;
    const avgRowBytes = hints?.avgRowBytes ?? 0;
    const eagerThreshold = hints?.eagerThreshold ?? defaultEagerThreshold;
    const maxMemoryBytes = hints?.maxMemoryBytes ?? defaultMaxMemoryBytes;
    if (estimatedCount && estimatedCount > eagerThreshold) {
        const plan = { strategy: "stream" } as const;
        logPlanner({
            source: "ingress",
            type: "final",
            planId,
            data: {
                plan,
                reason: "estimatedCount",
                estimatedCount,
                eagerThreshold,
            },
        });
        return plan;
    }
    if (estimatedCount && avgRowBytes > 0) {
        const estimateBytes = estimatedCount * avgRowBytes;
        if (estimateBytes > maxMemoryBytes) {
            const plan = { strategy: "stream" } as const;
            logPlanner({
                source: "ingress",
                type: "final",
                planId,
                data: {
                    avgRowBytes,
                    estimatedCount,
                    estimateBytes,
                    maxMemoryBytes,
                    plan,
                    reason: "memoryEstimate",
                },
            });
            return plan;
        }
    }
    const plan = { strategy: "eager" } as const;
    logPlanner({
        source: "ingress",
        type: "final",
        planId,
        data: {
            avgRowBytes,
            estimatedCount,
            eagerThreshold,
            maxMemoryBytes,
            plan,
            reason: "default",
        },
    });
    return plan;
}
