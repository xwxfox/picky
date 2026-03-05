import { RedisClient } from "bun";
import { Engine, IngressEngine, Schema } from "../../../src";
import type { BenchSchema } from "../schema";
import type { LargeItem } from "../random_data";
import { redisSortedSetSource } from "../../../src";

export const schema: BenchSchema = {
    datasets: [],
    name: "async-pushdown-redis",
};

const makeJson = (item: LargeItem) => JSON.stringify({
    id: item.id,
    name: item.name,
    score: item.score,
});

export const run = async () => {
    const items: Array<LargeItem> = [];
    for (let i = 0; i < 50_000; i++) {
        items.push({
            active: i % 2 === 0,
            created: "2026-01-01T00:00:00.000Z",
            flags: [],
            id: i,
            Logs: [],
            meta: { owner: { name: i % 2 === 0 ? "Ada" : "Bea", nickname: null } },
            name: i % 3 === 0 ? "alpha" : "beta",
            score: i % 100,
        });
    }

    const redis = new RedisClient();
    try {
        await redis.connect();
    } catch {
        console.log("async-pushdown-redis: skipped (redis unavailable)");
        redis.close();
        return;
    }
    const key = "bench:sorted";
    const zaddBatch: Array<string | number> = [];
    for (let i = 0; i < items.length; i++) {
        const item = items[i]!;
        zaddBatch.push(item.score, makeJson(item));
    }

    await redis.del(key);
    await redis.zadd(key, ...zaddBatch);

    const source = redisSortedSetSource<{ id: number; name: string | null; score: number }>({
        key,
        redis,
        schema: Schema.inline<{ id: number; name: string | null; score: number }>(),
        scoreField: "score",
    });

    const start = performance.now();
    const ingress = IngressEngine.fromSource(source);
    const result = await Engine.from(ingress)
        .greaterThanOrEqual("score", 90)
        .out()
        .orderBy("score", { direction: "desc" })
        .limit(50)
        .result();
    const end = performance.now();

    console.log(`async-pushdown-redis: size=${items.length} res=${result.length} time=${(end - start).toFixed(2)}ms`);
    await redis.del(key);
    redis.close();
};

if (import.meta.main) {
    await run();
}
