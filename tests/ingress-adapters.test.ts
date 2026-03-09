import { afterEach, describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { arraySource } from "@/io/ingress/adapters/array";
import { fileSource } from "@/io/ingress/adapters/file";
import { httpSource } from "@/io/ingress/adapters/http";
import { openApiBehavior } from "@/io/ingress/adapters/openapi";
import { RedisClient } from "bun";
import { redisSource } from "@/io/ingress/adapters/redis";
import { redisSortedSetSource } from "@/io/ingress/adapters/redis-sorted";
import { sqliteTableSource } from "@/io/ingress/adapters/sqlite";
import type { PushdownQuery } from "@/io/ingress/adapters/pushdown";
import type { ApiBehavior } from "@/io/ingress/adapters/http";
import type { OpenApiDocument } from "@/io/ingress/adapters/openapi";
import { Schema } from "@/io/schema";
import { Engine, IngressEngine } from "../src";

type BasicItem = { id: number; name: string; score: number };

const sampleItems: Array<BasicItem> = [
    { id: 1, name: "alpha", score: 1 },
    { id: 2, name: "beta", score: 2 },
    { id: 3, name: "gamma", score: 3 },
];

describe("Ingress adapters", () => {
    it("arraySource exposes sync source data", () => {
        const source = arraySource(sampleItems, { schema: Schema.inline<BasicItem>() });
        expect(source.mode).toEqual("sync");
        expect(source.data).toEqual(sampleItems);
        expect(source.schema?.source).toEqual("inline");
    });

    it("fileSource reads JSON array and NDJSON", async () => {
        const jsonPath = `./tests/temp-json-${Date.now()}.json`;
        const ndjsonPath = `./tests/temp-ndjson-${Date.now()}.ndjson`;
        await Bun.write(jsonPath, JSON.stringify(sampleItems));
        await Bun.write(ndjsonPath, sampleItems.map(item => JSON.stringify(item)).join("\n"));

        try {
            const jsonSource = fileSource<BasicItem>(jsonPath, { format: "json" });
            const jsonItems = await jsonSource.materialize!();
            expect(jsonItems.map(item => item.id)).toEqual([1, 2, 3]);

            const ndjsonSource = fileSource<BasicItem>(ndjsonPath, { format: "ndjson" });
            const ndjsonItems: Array<BasicItem> = [];
            for await (const item of ndjsonSource.stream()) {ndjsonItems.push(item);}
            expect(ndjsonItems.map(item => item.id)).toEqual([1, 2, 3]);
        } finally {
            await Bun.file(jsonPath).delete();
            await Bun.file(ndjsonPath).delete();
        }
    });

    it("fileSource prefilters JSON array", async () => {
        const jsonPath = `./tests/temp-json-${Date.now()}.json`;
        const items = [
            { id: 1, name: "alpha", score: 1, active: true },
            { id: 2, name: "beta", score: 2, active: false },
            { id: 3, name: "alpha", score: 3, active: true },
        ];
        await Bun.write(jsonPath, JSON.stringify(items));

        try {
            const ingress = IngressEngine.fromSource(fileSource<typeof items[number]>(jsonPath, { format: "json" }));
            const result = await Engine.from(ingress)
                .equals("name", "alpha")
                .greaterThan("score", 1)
                .out()
                .result();
            expect(result.map(item => item.id)).toEqual([3]);
        } finally {
            await Bun.file(jsonPath).delete();
        }
    });

    it("fileSource prefilters NDJSON", async () => {
        const ndjsonPath = `./tests/temp-ndjson-${Date.now()}.ndjson`;
        const items = [
            { id: 1, name: "alpha", score: 1, active: true },
            { id: 2, name: "beta", score: 2, active: false },
            { id: 3, name: "alpha", score: 3, active: true },
        ];
        await Bun.write(ndjsonPath, items.map(item => JSON.stringify(item)).join("\n"));

        try {
            const ingress = IngressEngine.fromSource(fileSource<typeof items[number]>(ndjsonPath, { format: "ndjson" }));
            const result = await Engine.from(ingress)
                .equals("name", "alpha")
                .greaterThan("score", 1)
                .out()
                .result();
            expect(result.map(item => item.id)).toEqual([3]);
        } finally {
            await Bun.file(ndjsonPath).delete();
        }
    });

    it("prefilter never drops values for complex predicates", async () => {
        const jsonPath = `./tests/temp-json-${Date.now()}.json`;
        const items = [
            { id: 1, name: "alpha", score: 1, active: true },
            { id: 2, name: "beta", score: 2, active: false },
            { id: 3, name: "alpha", score: 3, active: true },
            { id: 4, name: "alpha", score: 4, active: false },
        ];
        await Bun.write(jsonPath, JSON.stringify(items));

        try {
            const ingress = IngressEngine.fromSource(fileSource<typeof items[number]>(jsonPath, { format: "json" }));
            const result = await Engine.from(ingress)
                .equals("name", "alpha")
                .custom(item => item.active === false)
                .out()
                .result();
            expect(result.map(item => item.id)).toEqual([4]);
        } finally {
            await Bun.file(jsonPath).delete();
        }
    });

    it("prefilter handles escaped strings", async () => {
        const jsonPath = `./tests/temp-json-${Date.now()}.json`;
        const items = [
            { id: 1, name: "a\"b", score: 1, active: true },
            { id: 2, name: "plain", score: 2, active: false },
        ];
        await Bun.write(jsonPath, JSON.stringify(items));

        try {
            const ingress = IngressEngine.fromSource(fileSource<typeof items[number]>(jsonPath, { format: "json" }));
            const result = await Engine.from(ingress)
                .equals("name", "a\"b")
                .out()
                .result();
            expect(result.map(item => item.id)).toEqual([1]);
        } finally {
            await Bun.file(jsonPath).delete();
        }
    });

    it("prefilter handles boolean eq", async () => {
        const jsonPath = `./tests/temp-json-${Date.now()}.json`;
        const items = [
            { id: 1, name: "alpha", score: 1, active: true },
            { id: 2, name: "beta", score: 2, active: false },
        ];
        await Bun.write(jsonPath, JSON.stringify(items));

        try {
            const ingress = IngressEngine.fromSource(fileSource<typeof items[number]>(jsonPath, { format: "json" }));
            const result = await Engine.from(ingress)
                .equals("active", true)
                .out()
                .result();
            expect(result.map(item => item.id)).toEqual([1]);
        } finally {
            await Bun.file(jsonPath).delete();
        }
    });

    it("prefilter respects notEquals for missing field", async () => {
        const jsonPath = `./tests/temp-json-${Date.now()}.json`;
        const items = [
            { id: 1, name: "alpha", score: 1 } as { id: number; name: string; score: number; active?: boolean },
            { id: 2, name: "beta", score: 2 } as { id: number; name: string; score: number; active?: boolean },
        ];
        await Bun.write(jsonPath, JSON.stringify(items));

        try {
            const ingress = IngressEngine.fromSource(fileSource<typeof items[number]>(jsonPath, { format: "json" }));
            const result = await Engine.from(ingress)
                .notEquals("active", true)
                .out()
                .result();
            expect(result.map(item => item.id)).toEqual([1, 2]);
        } finally {
            await Bun.file(jsonPath).delete();
        }
    });

    it("prefilter supports null checks", async () => {
        const jsonPath = `./tests/temp-json-${Date.now()}.json`;
        const items = [
            { id: 1, name: null as string | null, score: 1 },
            { id: 2, name: "beta", score: 2 },
        ];
        await Bun.write(jsonPath, JSON.stringify(items));

        try {
            const ingress = IngressEngine.fromSource(fileSource<typeof items[number]>(jsonPath, { format: "json" }));
            const isNullResult = await Engine.from(ingress)
                .isNull("name")
                .out()
                .result();
            expect(isNullResult.map(item => item.id)).toEqual([1]);

            const notNullResult = await Engine.from(ingress)
                .valueNotNull("name")
                .out()
                .result();
            expect(notNullResult.map(item => item.id)).toEqual([2]);
        } finally {
            await Bun.file(jsonPath).delete();
        }
    });

    it("httpSource paginates over offset and cursor", async () => {
        const calls: Array<{ body?: string; method?: string; url: string }> = [];
        const originalFetch = globalThis.fetch;
        const items = [
            { id: 1, name: "alpha", score: 1 },
            { id: 2, name: "beta", score: 2 },
            { id: 3, name: "gamma", score: 3 },
        ];
        const cursorPayloads: Array<{ data: Array<BasicItem>; next?: string }> = [
            { data: items.slice(0, 2), next: "next" },
            { data: items.slice(2), next: "" },
        ];
        let cursorIndex = 0;
        globalThis.fetch = (async (input, init) => {
            calls.push({
                body: init?.body as string | undefined,
                method: init?.method as string | undefined,
                url: String(input),
            });
            const url = new URL(String(input));
            if (url.searchParams.has("offset")) {
                const offset = Number(url.searchParams.get("offset"));
                const limit = Number(url.searchParams.get("limit"));
                const slice = items.slice(offset, offset + limit);
                return new Response(JSON.stringify({ data: slice }), { status: 200 });
            }
            const payload = cursorPayloads[cursorIndex] ?? { data: [], next: "" };
            cursorIndex += 1;
            return new Response(JSON.stringify(payload), { status: 200 });
        }) as typeof fetch;

        try {
            const offsetBehavior: ApiBehavior<BasicItem> = {
                baseUrl: "https://example.com",
                dataPath: "data",
                method: "GET",
                pagination: { limitParam: "limit", mode: "offset", offsetParam: "offset", pageSize: 2 },
                path: "/items",
            };
            const offsetSource = httpSource<BasicItem>({ behavior: offsetBehavior });
            const offsetItems: Array<BasicItem> = [];
            for await (const item of offsetSource.stream()) {offsetItems.push(item);}
            expect(offsetItems.map(item => item.id)).toEqual([1, 2, 3]);

            const cursorBehavior: ApiBehavior<BasicItem> = {
                baseUrl: "https://example.com",
                dataPath: "data",
                method: "POST",
                pagination: {
                    cursorParam: "cursor",
                    limitParam: "limit",
                    mode: "cursor",
                    nextCursorPath: "next",
                    pageSize: 2,
                },
                path: "/items",
            };
            const cursorSource = httpSource<BasicItem>({ behavior: cursorBehavior });
            const cursorItems: Array<BasicItem> = [];
            for await (const item of cursorSource.stream()) {cursorItems.push(item);}
            expect(cursorItems.map(item => item.id)).toEqual([1, 2, 3]);
        } finally {
            globalThis.fetch = originalFetch;
        }

        expect(calls.length).toEqual(4);
        expect(calls[0]?.url).toContain("offset=0");
        expect(calls[1]?.url).toContain("offset=2");
        const body0 = JSON.parse(calls[2]?.body ?? "{}");
        const body1 = JSON.parse(calls[3]?.body ?? "{}");
        expect(body0.limit).toEqual(2);
        expect(body1.cursor).toEqual("next");
    });

    it("openApiBehavior resolves schema and builds behavior", () => {
        const doc: OpenApiDocument = {
            components: {
                schemas: {
                    Item: {
                        properties: {
                            id: { type: "integer" },
                            name: { type: "string" },
                            score: { type: "number" },
                        },
                        required: ["id", "name"],
                        type: "object",
                    },
                },
            },
            openapi: "3.0.0",
            paths: {
                "/items": {
                    get: {
                        responses: {
                            "200": {
                                content: {
                                    "application/json": {
                                        schema: {
                                            properties: {
                                                data: {
                                                    items: {
                                                        $ref: "#/components/schemas/Item",
                                                    },
                                                    type: "array",
                                                },
                                            },
                                            type: "object",
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        };
        const result = openApiBehavior<BasicItem>(doc, "/items", { dataPath: "data" });
        expect(result.schema.source).toEqual("inferred");
        expect(result.behavior.path).toEqual("/items");
    });

    it("openApiBehavior merges allOf and warns once for 3.1", () => {
        const doc: OpenApiDocument = {
            components: {
                schemas: {
                    Base: {
                        properties: { id: { type: "integer" } },
                        type: "object",
                    },
                    Extra: {
                        properties: { name: { type: "string" } },
                        type: "object",
                    },
                },
            },
            openapi: "3.1.0",
            paths: {
                "/items": {
                    get: {
                        responses: {
                            "200": {
                                content: {
                                    "application/json": {
                                        schema: {
                                            allOf: [
                                                { $ref: "#/components/schemas/Base" },
                                                { $ref: "#/components/schemas/Extra" },
                                            ],
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        };

        const originalWarn = console.warn;
        const warnings: Array<string> = [];
        console.warn = (message) => {warnings.push(String(message));};
        try {
            const result = openApiBehavior<{ id: number; name: string }>(doc, "/items");
            openApiBehavior<{ id: number; name: string }>(doc, "/items");
            expect(result.schema.sample as { id: number; name: string } | undefined)
                .toEqual({ id: 0, name: "" });
        } finally {
            console.warn = originalWarn;
        }
        expect(warnings.length).toEqual(1);
    });

    it("openApiBehavior resolves oneOf and anyOf", () => {
        const docOneOf: OpenApiDocument = {
            openapi: "3.0.0",
            paths: {
                "/items": {
                    get: {
                        responses: {
                            "200": {
                                content: {
                                    "application/json": {
                                        schema: {
                                            oneOf: [
                                                { properties: { id: { type: "integer" } }, type: "object" },
                                                { properties: { name: { type: "string" } }, type: "object" },
                                            ],
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        };
        const oneOfResult = openApiBehavior<{ id: number }>(docOneOf, "/items");
        expect(oneOfResult.schema.sample).toEqual({ id: 0 });

        const docAnyOf: OpenApiDocument = {
            openapi: "3.0.0",
            paths: {
                "/items": {
                    get: {
                        responses: {
                            "200": {
                                content: {
                                    "application/json": {
                                        schema: {
                                            anyOf: [
                                                { properties: { id: { type: "integer" } }, type: "object" },
                                                { properties: { name: { type: "string" } }, type: "object" },
                                            ],
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        };
        const anyOfResult = openApiBehavior<{ id: number }>(docAnyOf, "/items");
        expect(anyOfResult.schema.sample).toEqual({ id: 0 });
    });
});

describe("Pushdown adapters", () => {
    afterEach(() => {
        // reset any globals
    });

    it("redisSource pushdown enforces limit only", async () => {
        const redis = new RedisClient();
        const mget: RedisClient["mget"] = async (..._keys) => {
            const values: Array<string | null> = [];
            return values;
        };
        async function scan(_cursor: string | number): Promise<[string, Array<string>]>;
        async function scan(
            _cursor: string | number,
            _match: "MATCH",
            _pattern: string
        ): Promise<[string, Array<string>]>;
        async function scan(
            _cursor: string | number,
            _count: "COUNT",
            _hint: number
        ): Promise<[string, Array<string>]>;
        async function scan(
            _cursor: string | number,
            _match: "MATCH",
            _pattern: string,
            _count: "COUNT",
            _hint: number
        ): Promise<[string, Array<string>]>;
        async function scan(
            _cursor: string | number,
            ..._options: Array<string | number>
        ): Promise<[string, Array<string>]> {
            const keys: Array<string> = [];
            return ["0", keys];
        }
        redis.mget = mget;
        redis.scan = scan;
        const source = redisSource<BasicItem>({ keyPattern: "*", redis });
        const pushed = source.pushdown?.({ limit: 1 });
        expect(pushed).not.toBeNull();

        const bad = source.pushdown?.({ offset: 1 });
        expect(bad).toBeNull();
    });

    it("redisSortedSetSource supports score pushdown", async () => {
        const called: Array<string> = [];
        const redis = new RedisClient();
        async function zrange(
            _key: RedisClient.KeyLike,
            _start: string | number,
            _stop: string | number
        ): Promise<Array<string>>;
        async function zrange(
            _key: RedisClient.KeyLike,
            _start: string | number,
            _stop: string | number,
            _withscores: "WITHSCORES"
        ): Promise<Array<[string, number]>>;
        async function zrange(
            _key: RedisClient.KeyLike,
            _start: string | number,
            _stop: string | number,
            _byscore: "BYSCORE"
        ): Promise<Array<string>>;
        async function zrange(
            _key: RedisClient.KeyLike,
            _start: string,
            _stop: string,
            _bylex: "BYLEX"
        ): Promise<Array<string>>;
        async function zrange(
            _key: RedisClient.KeyLike,
            _start: string | number,
            _stop: string | number,
            ...options: Array<string>
        ): Promise<Array<string> | Array<[string, number]>> {
            called.push("zrange");
            if (options.includes("WITHSCORES")) {
                return [[JSON.stringify(sampleItems[0]), 1], [JSON.stringify(sampleItems[1]), 2]];
            }
            return [JSON.stringify(sampleItems[0]), JSON.stringify(sampleItems[1])];
        }

        async function zrangebyscore(
            _key: RedisClient.KeyLike,
            _min: string | number,
            _max: string | number
        ): Promise<Array<string>>;
        async function zrangebyscore(
            _key: RedisClient.KeyLike,
            _min: string | number,
            _max: string | number,
            _withscores: "WITHSCORES"
        ): Promise<Array<[string, number]>>;
        async function zrangebyscore(
            _key: RedisClient.KeyLike,
            _min: string | number,
            _max: string | number,
            _limit: "LIMIT",
            _offset: number,
            _count: number
        ): Promise<Array<string>>;
        async function zrangebyscore(
            _key: RedisClient.KeyLike,
            _min: string | number,
            _max: string | number,
            _limit: "LIMIT",
            _offset: number,
            _count: number,
            _withscores: "WITHSCORES"
        ): Promise<Array<[string, number]>>;
        async function zrangebyscore(
            _key: RedisClient.KeyLike,
            _min: string | number,
            _max: string | number,
            ...options: Array<string | number>
        ): Promise<Array<string> | Array<[string, number]>> {
            called.push("zrangebyscore");
            if (options.includes("WITHSCORES")) {
                return [[JSON.stringify(sampleItems[2]), 2]];
            }
            return [JSON.stringify(sampleItems[2])];
        }

        async function zrevrange(
            _key: RedisClient.KeyLike,
            _start: number,
            _stop: number
        ): Promise<Array<string>>;
        async function zrevrange(
            _key: RedisClient.KeyLike,
            _start: number,
            _stop: number,
            _withscores: "WITHSCORES"
        ): Promise<Array<[string, number]>>;
        async function zrevrange(
            _key: RedisClient.KeyLike,
            _start: number,
            _stop: number,
            ..._options: Array<string>
        ): Promise<Array<string> | Array<[string, number]>> {
            called.push("zrevrange");
            return [JSON.stringify(sampleItems[2])];
        }

        async function zrevrangebyscore(
            _key: RedisClient.KeyLike,
            _max: string | number,
            _min: string | number
        ): Promise<Array<string>>;
        async function zrevrangebyscore(
            _key: RedisClient.KeyLike,
            _max: string | number,
            _min: string | number,
            _withscores: "WITHSCORES"
        ): Promise<Array<[string, number]>>;
        async function zrevrangebyscore(
            _key: RedisClient.KeyLike,
            _max: string | number,
            _min: string | number,
            _limit: "LIMIT",
            _offset: number,
            _count: number
        ): Promise<Array<string>>;
        async function zrevrangebyscore(
            _key: RedisClient.KeyLike,
            _max: string | number,
            _min: string | number,
            _limit: "LIMIT",
            _offset: number,
            _count: number,
            _withscores: "WITHSCORES"
        ): Promise<Array<[string, number]>>;
        async function zrevrangebyscore(
            _key: RedisClient.KeyLike,
            _max: string | number,
            _min: string | number,
            ...options: Array<string | number>
        ): Promise<Array<string> | Array<[string, number]>> {
            called.push("zrevrangebyscore");
            if (options.includes("WITHSCORES")) {
                return [[JSON.stringify(sampleItems[2]), 2]];
            }
            return [JSON.stringify(sampleItems[2])];
        }

        redis.zrange = zrange;
        redis.zrangebyscore = zrangebyscore;
        redis.zrevrange = zrevrange;
        redis.zrevrangebyscore = zrevrangebyscore;
        const source = redisSortedSetSource<BasicItem>({
            key: "items",
            redis,
            scoreField: "score",
        });
        const query: PushdownQuery = {
            limit: 1,
            orders: [{ direction: "desc", field: "score" }],
            predicates: [{ field: "score", op: "gte", value: 2 }],
        };
        const pushed = source.pushdown?.(query);
        expect(pushed).not.toBeNull();
        const items: Array<BasicItem> = [];
        if (pushed) {
            for await (const item of pushed) {items.push(item);}
        }
        expect(items.map(item => item.id)).toEqual([3]);
        expect(called.includes("zrevrangebyscore")).toEqual(true);

        const bad = source.pushdown?.({ offset: 1 });
        expect(bad).toBeNull();
    });

    it("sqliteTableSource pushdown applies filters and ordering", async () => {
        const db = new Database(":memory:");
        db.exec("CREATE TABLE items (id INTEGER, name TEXT, score INTEGER);");
        db.exec("INSERT INTO items VALUES (1, 'alpha', 1), (2, 'beta', 2), (3, 'gamma', 3);");
        const source = sqliteTableSource<BasicItem>({
            db,
            schema: Schema.inline<BasicItem>(),
            table: "items",
        });
        const query: PushdownQuery = {
            limit: 1,
            orders: [{ direction: "desc", field: "score" }],
            predicates: [{ field: "score", op: "gte", value: 2 }],
        };
        const pushed = source.pushdown?.(query);
        const items: Array<BasicItem> = [];
        if (pushed) {
            for await (const item of pushed) {items.push(item);}
        }
        expect(items.map(item => item.id)).toEqual([3]);

        const engine = Engine.from(IngressEngine.fromSource(source));
        const result = await engine.greaterThanOrEqual("score", 2).out().orderBy("score", { direction: "desc" }).limit(1).result();
        expect(result.map(item => item.id)).toEqual([3]);
        db.close();
    });
});
