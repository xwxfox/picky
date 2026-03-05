import type { Schema } from "@/io/schema";
import type { PushdownQuery } from "@/io/ingress/adapters/pushdown";
import type { Paths } from "@/types/paths";

export type IngressMode = "sync" | "async";

export type IngressCapabilities = {
    count: boolean;
    filter: boolean;
    group: boolean;
    order: boolean;
    paginate: boolean;
    search: boolean;
};

export type IngressEstimate = {
    avgRowBytes?: number;
    estimatedCount?: number;
};

export type IngressHints<T extends Record<string, unknown>> = {
    avgRowBytes?: number;
    batchSize?: number;
    eagerThreshold?: number;
    estimatedCount?: number;
    maxMemoryBytes?: number;
    preferStreaming?: boolean;
    sortedBy?: Paths<T>;
    uniqueKey?: Paths<T>;
};

export type SyncIngressSource<T extends Record<string, unknown>> = {
    capabilities?: Partial<IngressCapabilities>;
    data: ReadonlyArray<T>;
    hints?: IngressHints<T>;
    mode: "sync";
    schema?: Schema<T>;
};

export type AsyncIngressSource<T extends Record<string, unknown>> = {
    capabilities?: Partial<IngressCapabilities>;
    close?: () => void | Promise<void>;
    hints?: IngressHints<T>;
    materialize?: () => Promise<ReadonlyArray<T>>;
    mode: "async";
    schema?: Schema<T>;
    stream: () => AsyncIterable<T>;
    pushdown?: (query: PushdownQuery) => AsyncIterable<T> | null;
};

export type IngressSource<T extends Record<string, unknown>> =
    | SyncIngressSource<T>
    | AsyncIngressSource<T>;

export const defaultIngressCapabilities: IngressCapabilities = {
    count: false,
    filter: false,
    group: false,
    order: false,
    paginate: false,
    search: false,
};

export function normalizeIngressCapabilities(
    input?: Partial<IngressCapabilities>
): IngressCapabilities {
    if (!input) {return { ...defaultIngressCapabilities };}
    return {
        count: input.count ?? false,
        filter: input.filter ?? false,
        group: input.group ?? false,
        order: input.order ?? false,
        paginate: input.paginate ?? false,
        search: input.search ?? false,
    };
}
