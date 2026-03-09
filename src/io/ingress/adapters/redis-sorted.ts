import type { RedisClient } from "bun";
import type { Schema } from "@/io/schema";
import type { PushdownQuery, PushdownPredicate } from "@/io/ingress/adapters/pushdown";
import type { IngressCapabilities, IngressHints, AsyncIngressSource } from "@/io/ingress/types";
import { AsyncQueue, isRecord } from "@/io/ingress/utils";

export type RedisSortedSetOptions<T extends Record<string, unknown>> = {
    capabilities?: Partial<IngressCapabilities>;
    hints?: IngressHints<T>;
    key: string;
    parse?: (value: string) => T | null;
    redis: RedisClient;
    schema?: Schema<T>;
    scoreField?: keyof T & string;
    direction?: "asc" | "desc";
};

const defaultParse = <T extends Record<string, unknown>>(value: string): T | null => {
    const parsed = JSON.parse(value);
    if (!isRecord(parsed)) {return null;}
    return parsed as T;
};

export function redisSortedSetSource<T extends Record<string, unknown>>(
    options: RedisSortedSetOptions<T>
): AsyncIngressSource<T> {
    const stream = (_options?: import("@/io/ingress/prefilter").PrefilterStreamOptions) => streamRedisSorted<T>(options, undefined);
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
        pushdown: (query) => pushdownRedisSorted<T>(options, query),
        schema: options.schema,
        stream,
    };
}

function pushdownRedisSorted<T extends Record<string, unknown>>(
    options: RedisSortedSetOptions<T>,
    query: PushdownQuery
): AsyncIterable<T> | null {
    const supported = supportsSortedPushdown(query, options.scoreField);
    if (!supported) {return null;}
    return streamRedisSorted(options, query);
}

function supportsSortedPushdown(query: PushdownQuery, scoreField?: string): boolean {
    if (query.offset && query.offset > 0) {return false;}
    const predicates = query.predicates ?? [];
    if (predicates.length > 1) {return false;}
    if (predicates.length === 1) {
        const predicate = predicates[0]!;
        if (!scoreField || predicate.field !== scoreField) {return false;}
        if (predicate.op !== "gt" && predicate.op !== "gte" && predicate.op !== "lt" && predicate.op !== "lte" && predicate.op !== "between") {
            return false;
        }
    }
    const orders = query.orders ?? [];
    if (orders.length > 1) {return false;}
    if (orders.length === 1 && scoreField) {
        if (orders[0]!.nulls) {return false;}
        if (orders[0]!.field !== scoreField) {return false;}
    }
    return true;
}

async function* streamRedisSorted<T extends Record<string, unknown>>(
    options: RedisSortedSetOptions<T>,
    query?: PushdownQuery
): AsyncIterable<T> {
    const queue = new AsyncQueue<T>();
    const pending = (async () => {
        const parse = options.parse ?? defaultParse<T>;
        const limit = query?.limit ?? 0;
        const direction = query?.orders?.[0]?.direction ?? options.direction ?? "asc";
        const scorePredicate = query?.predicates?.[0];

        const items = await fetchSortedMembers(options, direction, scorePredicate, limit);
        for (let i = 0; i < items.length; i++) {
            const parsed = parse(items[i]!);
            if (parsed) {queue.push(parsed);}
        }
        queue.close();
    })();

    for await (const item of queue) {
        yield item;
    }
    await pending;
}

async function fetchSortedMembers<T extends Record<string, unknown>>(
    options: RedisSortedSetOptions<T>,
    direction: "asc" | "desc",
    predicate: PushdownPredicate | undefined,
    limit: number
): Promise<Array<string>> {
    const key = options.key;
    if (!predicate) {
        if (direction === "desc") {
            return limit > 0
                ? options.redis.zrevrange(key, 0, limit - 1)
                : options.redis.zrevrange(key, 0, -1);
        }
        return limit > 0
            ? options.redis.zrange(key, 0, limit - 1)
            : options.redis.zrange(key, 0, -1);
    }

    const [min, max] = scoreBounds(predicate);
    if (direction === "desc") {
        return limit > 0
            ? options.redis.zrevrangebyscore(key, max, min, "LIMIT", 0, limit)
            : options.redis.zrevrangebyscore(key, max, min);
    }
    return limit > 0
        ? options.redis.zrangebyscore(key, min, max, "LIMIT", 0, limit)
        : options.redis.zrangebyscore(key, min, max);
}

function scoreBounds(predicate: { op: string; value?: unknown }): [string | number, string | number] {
    if (predicate.op === "between") {
        const range = predicate.value as { max: number; min: number };
        return [range.min, range.max];
    }
    if (predicate.op === "gt") {return [`(${String(predicate.value)}`, "+inf"];
    }
    if (predicate.op === "gte") {return [String(predicate.value), "+inf"];
    }
    if (predicate.op === "lt") {return ["-inf", `(${String(predicate.value)}`];
    }
    return ["-inf", String(predicate.value)];
}
