import type { IngressCapabilities, IngressHints } from "@/io/ingress/types";

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
    const preferStreaming = hints?.preferStreaming ?? false;
    if (preferStreaming && !(requiresOrdering || requiresGrouping || requiresSearch)) {
        return { strategy: "stream" };
    }

    if (requiresOrdering || requiresGrouping) {
        if (capabilities.order || capabilities.group) {return { strategy: "stream" };}
        return { strategy: "eager" };
    }

    if (requiresSearch && capabilities.search) {return { strategy: "stream" };}

    const estimatedCount = hints?.estimatedCount;
    const avgRowBytes = hints?.avgRowBytes ?? 0;
    const eagerThreshold = hints?.eagerThreshold ?? defaultEagerThreshold;
    const maxMemoryBytes = hints?.maxMemoryBytes ?? defaultMaxMemoryBytes;
    if (estimatedCount && estimatedCount > eagerThreshold) {
        return { strategy: "stream" };
    }
    if (estimatedCount && avgRowBytes > 0) {
        const estimateBytes = estimatedCount * avgRowBytes;
        if (estimateBytes > maxMemoryBytes) {return { strategy: "stream" };}
    }
    return { strategy: "eager" };
}
