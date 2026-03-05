import type { IngressEngine, AsyncIngressEngine } from "@/io/ingress";
import type { DefaultSearchCapabilityState } from "@/types/search";
import { QueryBuilder } from "./builder";

export class Engine {
    static from<T extends Record<string, unknown>>(
        ingress: IngressEngine<T>
    ): QueryBuilder<T, DefaultSearchCapabilityState, "sync">;
    static from<T extends Record<string, unknown>>(
        ingress: AsyncIngressEngine<T>
    ): QueryBuilder<T, DefaultSearchCapabilityState, "async">;
    static from<T extends Record<string, unknown>>(
        ingress: IngressEngine<T> | AsyncIngressEngine<T>
    ): QueryBuilder<T, DefaultSearchCapabilityState, "sync" | "async"> {
        if (ingress instanceof Object && "mode" in ingress && ingress.mode === "async") {
            return QueryBuilder.fromAsync(ingress as AsyncIngressEngine<T>);
        }
        return QueryBuilder.from(ingress as IngressEngine<T>);
    }
}
