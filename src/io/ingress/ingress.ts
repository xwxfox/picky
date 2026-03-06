import type { CacheOptions } from "@/types";
import type { Schema } from "@/io/schema";
import type {
    AsyncIngressSource,
    IngressCapabilities,
    IngressHints,
    IngressMode,
    IngressSource,
    SyncIngressSource,
} from "@/io/ingress/types";
import { normalizeIngressCapabilities } from "@/io/ingress/types";
import {
    defaultCacheOptions,
    getUseSharedCache,
    setUseSharedCache,
    getSharedCacheState,
    setSharedCacheState,
    createCacheState,
} from "@/core/shared/cache";
import type { CacheState } from "@/core/shared/cache";

export type IngressConfig<T extends Record<string, unknown>> = {
    cache?: Partial<CacheOptions>;
    sharedCache?: boolean;
    hints?: IngressHints<T>;
    capabilities?: Partial<IngressCapabilities>;
};

function resolveCacheState<T extends Record<string, unknown>>(config?: IngressConfig<T>): CacheState {
    const cacheOptions = config?.cache ? { ...defaultCacheOptions, ...config.cache } : defaultCacheOptions;

    if (config?.sharedCache ?? getUseSharedCache()) {
        const shared = getSharedCacheState();
        if (shared) {return shared;}
        const created = createCacheState(cacheOptions);
        setSharedCacheState(created);
        return created;
    }

    return createCacheState(cacheOptions);
}

function mergeHints<T extends Record<string, unknown>>(
    base?: IngressHints<T>,
    incoming?: IngressHints<T>
): IngressHints<T> | undefined {
    if (!base && !incoming) {return undefined;}
    return { ...base, ...incoming } as IngressHints<T>;
}

export class IngressEngine<T extends Record<string, unknown>> {
    public readonly mode: IngressMode = "sync";
    public constructor(
        public readonly data: ReadonlyArray<T>,
        public readonly cache: CacheState,
        public readonly capabilities: IngressCapabilities = normalizeIngressCapabilities(),
        public readonly hints?: IngressHints<T>,
        public readonly schema?: Schema<T>
    ) { }

    static from<T extends Record<string, unknown>>(
        data: ReadonlyArray<T>,
        schema?: Schema<T>,
        config?: IngressConfig<T>
    ): IngressEngine<T> {
        const cache = resolveCacheState(config);
        const capabilities = normalizeIngressCapabilities(config?.capabilities);
        const hints = mergeHints(config?.hints, undefined);
        return new IngressEngine<T>(data, cache, capabilities, hints, schema);
    }

    static create<T extends Record<string, unknown>>(
        config?: IngressConfig<T>,
        schema?: Schema<T>
    ): IngressEngine<T> {
        const cache = resolveCacheState(config);
        const capabilities = normalizeIngressCapabilities(config?.capabilities);
        const empty: Array<T> = [];
        return new IngressEngine<T>(empty, cache, capabilities, config?.hints, schema);
    }

    static fromSchema<T extends Record<string, unknown>>(
        schema: Schema<T>,
        config?: IngressConfig<T>
    ): IngressEngine<T> {
        return IngressEngine.create<T>(config, schema);
    }

    static fromSource<T extends Record<string, unknown>>(
        source: SyncIngressSource<T>,
        config?: IngressConfig<T>
    ): IngressEngine<T>;
    static fromSource<T extends Record<string, unknown>>(
        source: AsyncIngressSource<T>,
        config?: IngressConfig<T>
    ): AsyncIngressEngine<T>;
    static fromSource<T extends Record<string, unknown>>(
        source: IngressSource<T>,
        config?: IngressConfig<T>
    ): IngressEngine<T> | AsyncIngressEngine<T> {
        const cache = resolveCacheState(config);
        const capabilities = normalizeIngressCapabilities({
            ...source.capabilities,
            ...config?.capabilities,
        });
        const hints = mergeHints(config?.hints, source.hints);
        if (source.mode === "sync") {
            return new IngressEngine<T>(source.data, cache, capabilities, hints, source.schema);
        }
        return new AsyncIngressEngine<T>(source, cache, capabilities, hints, source.schema);
    }

    static configure(options: {
        maxDateCache?: number;
        maxPathCache?: number;
        sharedCache?: boolean;
    }): void {
        if (typeof options.sharedCache === "boolean") { setUseSharedCache(options.sharedCache); }
        if (typeof options.maxDateCache === "number") { defaultCacheOptions.maxDateCache = options.maxDateCache; }
        if (typeof options.maxPathCache === "number") { defaultCacheOptions.maxPathCache = options.maxPathCache; }
        if (getUseSharedCache()) {
            setSharedCacheState(createCacheState(defaultCacheOptions));
        }
    }

    static clearCaches(): void {
        const state = getSharedCacheState();
        if (state) {
            state.pathSegmentsCache.clear();
            state.pathAccessorsCache.clear();
            state.dateCache.clear();
        }
    }

    load(data: ReadonlyArray<T>, hints?: IngressHints<T>): IngressEngine<T> {
        const mergedHints = mergeHints(this.hints, hints);
        return new IngressEngine<T>(data, this.cache, this.capabilities, mergedHints, this.schema);
    }

    loadFrom<U>(source: U, adapter: (input: U) => ReadonlyArray<T>, hints?: IngressHints<T>): IngressEngine<T> {
        const mergedHints = mergeHints(this.hints, hints);
        return new IngressEngine<T>(adapter(source), this.cache, this.capabilities, mergedHints, this.schema);
    }

    get length(): number {
        return this.data.length;
    }

    isEmpty(): boolean {
        return this.data.length === 0;
    }

    clear(): IngressEngine<T> {
        const empty: Array<T> = [];
        return new IngressEngine<T>(empty, this.cache, this.capabilities, this.hints, this.schema);
    }
}

export class AsyncIngressEngine<T extends Record<string, unknown>> {
    public readonly mode: IngressMode = "async";
    public constructor(
        public readonly source: AsyncIngressSource<T>,
        public readonly cache: CacheState,
        public readonly capabilities: IngressCapabilities = normalizeIngressCapabilities(),
        public readonly hints?: IngressHints<T>,
        public readonly schema?: Schema<T>
    ) { }

    async materialize(): Promise<ReadonlyArray<T>> {
        if (this.source.materialize) {
            return this.source.materialize();
        }
        const items: Array<T> = [];
        for await (const item of this.source.stream()) {
            items.push(item);
        }
        return items;
    }

    stream(): AsyncIterable<T> {
        return this.source.stream();
    }

    get length(): number {
        return this.source.hints?.estimatedCount ?? 0;
    }

    isEmpty(): boolean {
        const count = this.source.hints?.estimatedCount;
        if (count === undefined) {return false;}
        return count === 0;
    }

    async load(data: ReadonlyArray<T>, hints?: IngressHints<T>): Promise<IngressEngine<T>> {
        const mergedHints = mergeHints(this.hints, hints);
        return new IngressEngine<T>(data, this.cache, this.capabilities, mergedHints, this.schema);
    }

    async loadFrom<U>(source: U, adapter: (input: U) => ReadonlyArray<T>, hints?: IngressHints<T>): Promise<IngressEngine<T>> {
        const mergedHints = mergeHints(this.hints, hints);
        return new IngressEngine<T>(adapter(source), this.cache, this.capabilities, mergedHints, this.schema);
    }

    async close(): Promise<void> {
        if (!this.source.close) {return;}
        const result = this.source.close();
        if (result instanceof Promise) {
            await result;
        }
    }
}

export type AnyIngressEngine<T extends Record<string, unknown>> =
    | IngressEngine<T>
    | AsyncIngressEngine<T>;

export function isAsyncIngress<T extends Record<string, unknown>>(
    ingress: AnyIngressEngine<T>
): ingress is AsyncIngressEngine<T> {
    return ingress.mode === "async";
}
