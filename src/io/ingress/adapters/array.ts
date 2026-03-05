import type { Schema } from "@/io/schema";
import type { IngressCapabilities, IngressHints, SyncIngressSource } from "@/io/ingress/types";

export function arraySource<T extends Record<string, unknown>>(
    data: ReadonlyArray<T>,
    options?: {
        schema?: Schema<T>;
        hints?: IngressHints<T>;
        capabilities?: Partial<IngressCapabilities>;
    }
): SyncIngressSource<T> {
    return {
        capabilities: options?.capabilities,
        data,
        hints: options?.hints,
        mode: "sync",
        schema: options?.schema,
    };
}
