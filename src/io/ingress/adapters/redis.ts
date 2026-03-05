import type { RedisClient } from "bun";
import type { Schema } from "@/io/schema";
import type { PushdownQuery } from "@/io/ingress/adapters/pushdown";
import type { IngressCapabilities, IngressHints, AsyncIngressSource } from "@/io/ingress/types";
import { AsyncQueue, isRecord } from "@/io/ingress/utils";

export type RedisIngressOptions<T extends Record<string, unknown>> = {
    capabilities?: Partial<IngressCapabilities>;
    hints?: IngressHints<T>;
    keyPattern: string;
    limit?: number;
    parse?: (value: string) => T | null;
    redis: RedisClient;
    schema?: Schema<T>;
    scanCount?: number;
};

const defaultParse = <T extends Record<string, unknown>>(value: string): T | null => {
    const parsed = JSON.parse(value);
    if (!isRecord(parsed)) {return null;}
    return parsed as T;
};

export function redisSource<T extends Record<string, unknown>>(
    options: RedisIngressOptions<T>
): AsyncIngressSource<T> {
    const stream = () => streamRedis<T>(options);
    const materialize = async () => {
        const items: Array<T> = [];
        for await (const item of stream()) {items.push(item);}
        return items as ReadonlyArray<T>;
    };
    return {
        capabilities: options.capabilities,
        hints: options.hints,
        materialize,
        mode: "async",
        pushdown: (query) => pushdownRedis<T>(options, query),
        schema: options.schema,
        stream,
    };
}

async function* streamRedis<T extends Record<string, unknown>>(
    options: RedisIngressOptions<T>
): AsyncIterable<T> {
    const queue = new AsyncQueue<T>();
    const pending = (async () => {
        const parse = options.parse ?? defaultParse<T>;
        const count = options.scanCount ?? 200;
        const limit = options.limit ?? 0;
        let total = 0;
        let cursor: string | number = "0";
        while (true) {
            const scanResult: [string, Array<string>] = await options.redis.scan(
                cursor,
                "MATCH",
                options.keyPattern,
                "COUNT",
                count
            );
            const nextCursor: string = scanResult[0];
            const keys: Array<string> = scanResult[1];
            if (keys.length > 0) {
                const values = await options.redis.mget(...keys);
                for (let i = 0; i < values.length; i++) {
                    const value = values[i];
                    if (value == null) {continue;}
                    const parsed = parse(value);
                    if (parsed) {queue.push(parsed);}
                    if (limit > 0) {
                        total += 1;
                        if (total >= limit) {
                            queue.close();
                            return;
                        }
                    }
                }
            }
            if (nextCursor === "0") {break;}
            cursor = nextCursor;
        }
        queue.close();
    })();

    for await (const item of queue) {
        yield item;
    }
    await pending;
}

function pushdownRedis<T extends Record<string, unknown>>(
    options: RedisIngressOptions<T>,
    query: PushdownQuery
): AsyncIterable<T> | null {
    const limit = query.limit ?? options.limit ?? 0;
    if (query.offset && query.offset > 0) {return null;}
    if (query.orders && query.orders.length > 0) {return null;}
    if (query.predicates && query.predicates.length > 0) {return null;}
    return streamRedis({ ...options, limit });
}
