import type { CacheOptions } from "@/types";
import type { Schema } from "@/io/schema";
import {
    defaultCacheOptions,
    getUseSharedCache,
    setUseSharedCache,
    getSharedCacheState,
    setSharedCacheState,
    createCacheState,
} from "@/core/shared/cache";
import type { CacheState } from "@/core/shared/cache";

export type IngressConfig = {
    cache?: Partial<CacheOptions>;
    sharedCache?: boolean;
}

export class IngressEngine<T extends Record<string, unknown>> {
    public constructor(
        public readonly data: ReadonlyArray<T>,
        public readonly cache: CacheState,
        public readonly schema?: Schema<T>
    ) {}

    static from<T extends Record<string, unknown>>(data: ReadonlyArray<T>, schema?: Schema<T>): IngressEngine<T> {
        return new IngressEngine<T>(data, createCacheState(defaultCacheOptions), schema);
    }

    static create<T extends Record<string, unknown>>(config?: IngressConfig, schema?: Schema<T>): IngressEngine<T> {
        const cacheOptions = config?.cache ? { ...defaultCacheOptions, ...config.cache } : defaultCacheOptions;
        
        let cache: CacheState;
        if (config?.sharedCache ?? getUseSharedCache()) {
            cache = getSharedCacheState() ?? (() => {
                const newState = createCacheState(cacheOptions);
                setSharedCacheState(newState);
                return newState;
            })();
        } else {
            cache = createCacheState(cacheOptions);
        }
        
        const empty: Array<T> = [];
        return new IngressEngine<T>(empty, cache, schema);
    }

    static fromSchema<T extends Record<string, unknown>>(schema: Schema<T>, config?: IngressConfig): IngressEngine<T> {
        return IngressEngine.create<T>(config, schema);
    }

    static configure(options: {
        maxDateCache?: number;
        maxPathCache?: number;
        sharedCache?: boolean;
    }): void {
        if (typeof options.sharedCache === "boolean") {setUseSharedCache(options.sharedCache);}
        if (typeof options.maxDateCache === "number") {defaultCacheOptions.maxDateCache = options.maxDateCache;}
        if (typeof options.maxPathCache === "number") {defaultCacheOptions.maxPathCache = options.maxPathCache;}
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

    load(data: ReadonlyArray<T>): IngressEngine<T> {
        return new IngressEngine<T>(data, this.cache, this.schema);
    }

    loadFrom<U>(source: U, adapter: (input: U) => ReadonlyArray<T>): IngressEngine<T> {
        return new IngressEngine<T>(adapter(source), this.cache, this.schema);
    }

    get length(): number {
        return this.data.length;
    }

    isEmpty(): boolean {
        return this.data.length === 0;
    }

    clear(): IngressEngine<T> {
        const empty: Array<T> = [];
        return new IngressEngine<T>(empty, this.cache, this.schema);
    }
}
