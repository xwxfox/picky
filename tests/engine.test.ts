import { describe, it, expect } from "bun:test";
import { Engine, IngressEngine } from "../src";
import { QueryChain, ChainRegistry, createChain, compileChain } from "@/core/engine/chains/chain";
import { createComparePredicate } from "@/core/engine/predicates/compare";
import { createBetweenPredicate } from "@/core/engine/predicates/range";
import { compareNullable, createComparator, heapPush, heapReplaceRoot } from "@/core/engine/compare";
import { createCacheState, getSegments, toTimestamp } from "@/core/shared/cache";
import {
    someResolvedWithSegments,
    resolveFirstWithSegments,
    forEachResolvedWithSegments,
    everyResolvedWithSegments,
    pathExistsWithSegments,
    resolveOrderValueWithSegments,
} from "@/core/shared/path";
import { Schema } from "@/io/schema";
import { ExecutionEngine } from "@/core/engine/executor";
import type { ResolveObject } from "@/types/core";

type LogEntry = {
    message: string | null;
    tags: Array<string>;
    type: string;
    when: Date | string;
};

type SampleItem = {
    active: boolean;
    created: Date | string | number;
    flags: Array<string>;
    HandledBy: {
        SalesRep: string | null;
    };
    id: number;
    label: string;
    Logs: Array<LogEntry>;
    meta: {
        owner: {
            name: string;
            nickname?: string | null;
        };
    };
    metrics: {
        maybe?: number | null;
        values: Array<number>;
    };
    misc?: {
        code?: string;
        nested?: {
            value?: string | null;
        };
    };
    name: string;
    note?: string | null;
    score: number;
};

const data: Array<SampleItem> = [
    {
        active: true,
        created: new Date("2024-01-01T00:00:00.000Z"),
        flags: ["red", "blue"],
        HandledBy: {
            SalesRep: "NHR",
        },
        id: 1,
        label: "2024-01-01",
        Logs: [
            {
                message: "over",
                tags: ["x"],
                type: "CREDIT_MAX_EXCEEDED",
                when: "2024-01-03T00:00:00.000Z",
            },
        ],
        meta: {
            owner: {
                name: "Alice",
                nickname: null,
            },
        },
        metrics: {
            maybe: 0,
            values: [1, 2, 3],
        },
        misc: {
            code: "X1",
            nested: {
                value: null,
            },
        },
        name: "Alpha",
        note: null,
        score: 10,
    },
    {
        active: false,
        created: "2024-01-02T00:00:00.000Z",
        flags: ["green"],
        HandledBy: {
            SalesRep: null,
        },
        id: 2,
        label: "release",
        Logs: [
            {
                message: null,
                tags: [],
                type: "OTHER",
                when: new Date("2024-01-04T00:00:00.000Z"),
            },
        ],
        meta: {
            owner: {
                name: "Bob",
                nickname: "B",
            },
        },
        metrics: {
            values: [5],
        },
        misc: {},
        name: "Beta",
        note: "ok",
        score: 20,
    },
    {
        active: true,
        created: "not-a-date",
        flags: [],
        HandledBy: {
            SalesRep: "THS",
        },
        id: 3,
        label: "not-a-date",
        Logs: [],
        meta: {
            owner: {
                name: "Cara",
            },
        },
        metrics: {
            maybe: null,
            values: [],
        },
        name: "Gamma",
        score: 5,
    },
    {
        active: true,
        created: "2024-01-01T00:00:00.000Z",
        flags: ["red"],
        HandledBy: {
            SalesRep: "NHR",
        },
        id: 4,
        label: "2024-01-01",
        Logs: [
            {
                message: "dup",
                tags: ["y"],
                type: "CREDIT_MAX_EXCEEDED",
                when: "2024-01-03T00:00:00.000Z",
            },
        ],
        meta: {
            owner: {
                name: "Dee",
                nickname: undefined,
            },
        },
        metrics: {
            values: [10],
        },
        misc: {
            nested: {
                value: "ok",
            },
        },
        name: "Delta",
        note: undefined,
        score: 10,
    },
];

const makeEngine = () => Engine.from(IngressEngine.from(data));
const from = <T extends Record<string, unknown>>(items: ReadonlyArray<T>) => Engine.from(IngressEngine.from(items));

describe("Engine - core basics", () => {
    it("returns all items with no filters", () => {
        const result = makeEngine().out().result().map(item => item.id);
        expect(result).toEqual([1, 2, 3, 4]);
    });

    it("compile matches result", () => {
        const engine = makeEngine().equals("active", true);
        const result = engine.out().result().map(item => item.id);
        expect(result).toEqual([1, 3, 4]);
    });
});

describe("Engine - logical grouping", () => {
    it("and groups predicates", () => {
        const result = makeEngine()
            .and(q => q.equals("active", true).greaterThan("score", 8))
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("or groups compiled predicates", () => {
        const result = makeEngine()
            .equals("id", 1)
            .or(q => q.equals("id", 2))
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 2]);
    });

    it("or on empty engine uses group", () => {
        const result = makeEngine()
            .or(q => q.equals("id", 2))
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([2]);
    });

    it("not negates group", () => {
        const result = makeEngine()
            .not(q => q.equals("id", 1))
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([2, 3, 4]);
    });
});

describe("Engine - equals and notEquals", () => {
    it("matches nested path values", () => {
        const result = makeEngine()
            .equals("meta.owner.name", "Alice")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1]);
    });

    it("matches array values at leaf", () => {
        const result = makeEngine()
            .equals("flags", "green")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([2]);
    });

    it("matches date-like strings by strict equality", () => {
        const result = makeEngine()
            .equals("label", "2024-01-01")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("notEquals is strict negation of equals for arrays", () => {
        const result = makeEngine()
            .notEquals("flags", "red")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([2, 3]);
    });

    it("supports date equality across Date and ISO string", () => {
        const result = makeEngine()
            .dateEquals("created", new Date("2024-01-02T00:00:00.000Z"))
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([2]);
    });

    it("compares Date fields to numeric timestamps", () => {
        const result = makeEngine()
            .dateEquals("created", new Date("2024-01-01T00:00:00.000Z").getTime())
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("keeps reference equality for objects", () => {
        const metaRef = data[0]!.meta;
        const hit = makeEngine().equals("meta", metaRef).out().result().map(item => item.id);
        expect(hit).toEqual([1]);

        const miss = makeEngine().equals("meta", { owner: { name: "Alice" } }).out().result().map(item => item.id);
        expect(miss).toEqual([]);
    });
});

describe("Engine - comparison operators", () => {
    it("handles greater/less variants with numbers", () => {
        expect(makeEngine().greaterThan("score", 9).out().result().map(item => item.id)).toEqual([1, 2, 4]);
        expect(makeEngine().greaterThanOrEqual("score", 10).out().result().map(item => item.id)).toEqual([1, 2, 4]);
        expect(makeEngine().lessThan("score", 10).out().result().map(item => item.id)).toEqual([3]);
        expect(makeEngine().lessThanOrEqual("score", 10).out().result().map(item => item.id)).toEqual([1, 3, 4]);
    });

    it("supports between for numbers", () => {
        const result = makeEngine()
            .between("score", 6, 20)
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 4]);
    });

    it("supports dateBetween for dates", () => {
        const result = makeEngine()
            .dateBetween(
                "created",
                new Date("2024-01-01T00:00:00.000Z"),
                new Date("2024-01-02T00:00:00.000Z")
            )
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 4]);
    });

    it("dateBetween requires both bounds to be date-ish", () => {
        const result = makeEngine()
            .dateBetween("created", "2024-01-01T00:00:00.000Z", "not-a-date")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([]);
    });

    it("compares strings lexicographically", () => {
        const result = makeEngine()
            .greaterThan("name", "Beta")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([3, 4]);
    });
});

describe("Engine - membership and string operations", () => {
    it("supports in and notIn", () => {
        expect(makeEngine().in("id", [1, 3]).out().result().map(item => item.id)).toEqual([1, 3]);
        expect(makeEngine().notIn("id", [1, 2]).out().result().map(item => item.id)).toEqual([3, 4]);
        expect(makeEngine().notIn("flags", ["red"]).out().result().map(item => item.id)).toEqual([2, 3]);
    });

    it("supports contains, startsWith, endsWith", () => {
        expect(makeEngine().contains("name", "ph").out().result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().startsWith("name", "be", true).out().result().map(item => item.id)).toEqual([2]);
        expect(makeEngine().endsWith("name", "MA", true).out().result().map(item => item.id)).toEqual([3]);
    });

    it("supports matches with regex and resets /g state", () => {
        expect(makeEngine().matches("name", /^A/).out().result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().matches("name", /a/g).out().result().map(item => item.id)).toEqual([1, 2, 3, 4]);
    });

    it("matches array leaf values", () => {
        const result = makeEngine()
            .equals("metrics.values", 2)
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1]);
    });
});

describe("Engine - null and existence semantics", () => {
    it("handles isNull and isNotNull", () => {
        expect(makeEngine().isNull("note").out().result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().valueNotNull("note").out().result().map(item => item.id)).toEqual([2]);
    });

    it("checks exists for defined values", () => {
        expect(makeEngine().pathExists("note").out().result().map(item => item.id)).toEqual([1, 2, 4]);
        expect(makeEngine().pathExists("misc.code").out().result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().pathExists("misc.nested.value").out().result().map(item => item.id)).toEqual([1, 4]);
    });

    it("supports pathExistsNullable", () => {
        expect(makeEngine().pathExistsNullable("note").out().result().map(item => item.id)).toEqual([1, 2]);
        expect(makeEngine().pathExistsNullable("misc.nested.value").out().result().map(item => item.id)).toEqual([1, 4]);
    });

    it("treats undefined as not-null false", () => {
        expect(makeEngine().valueNotNull("meta.owner.nickname").out().result().map(item => item.id)).toEqual([2]);
    });
});

describe("Engine - array helpers", () => {
    it("supports arraySome, arrayEvery, arrayNone", () => {
        expect(makeEngine().arraySome("flags", v => v === "green").out().result().map(item => item.id)).toEqual([2]);
        expect(makeEngine().arrayEvery("flags", v => v.length >= 4).out().result().map(item => item.id)).toEqual([2]);
        expect(makeEngine().arrayNone("flags", v => v.length < 4).out().result().map(item => item.id)).toEqual([2, 3]);
    });

    it("arrayEvery fails on empty arrays", () => {
        expect(makeEngine().arrayEvery("flags", () => true).out().result().map(item => item.id)).toEqual([1, 2, 4]);
    });
});

describe("Engine - nested arrays and paths", () => {
    it("supports nested filtering for arrays of objects", () => {
        const result = makeEngine()
            .nested("Logs", q => q.equals("type", "CREDIT_MAX_EXCEEDED"))
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("supports a single array segment in paths", () => {
        const result = makeEngine()
            .equals("Logs.type", "CREDIT_MAX_EXCEEDED")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("matches nested date comparisons in array objects", () => {
        const result = makeEngine()
            .nested("Logs", q => q.dateAfterOrEqual("when", new Date("2024-01-03T00:00:00.000Z")))
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 4]);
    });

    it("supports nested array predicates on array fields", () => {
        const result = makeEngine()
            .nested("Logs", q => q.arraySome("tags", tag => tag === "x"))
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1]);
    });
});

describe("Engine - aggressive edge cases", () => {
    it("handles nullish segments without throwing", () => {
        const result = makeEngine()
            .equals("misc.nested.value", "ok")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([4]);
    });

    it("handles null value comparisons", () => {
        expect(makeEngine().equals("note", null).out().result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().notEquals("note", null).out().result().map(item => item.id)).toEqual([2, 3, 4]);
    });

    it("handles empty resolvers for arrays", () => {
        const result = makeEngine()
            .equals("Logs.type", "MISSING")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([]);
    });

    it("handles exists on optional nested paths", () => {
        const result = makeEngine()
            .pathExists("misc.nested.value")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("handles multiple predicates efficiently", () => {
        const engine = makeEngine()
            .equals("active", true)
            .equals("HandledBy.SalesRep", "NHR")
            .greaterThanOrEqual("score", 10)
            .not(q => q.equals("name", "Gamma"));

        const result = engine.out().result().map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("keeps working when compiled after many chains", () => {
        let engine = makeEngine();
        for (let i = 0; i < 20; i++) {
            engine = engine.and(q => q.equals("active", true));
        }
        expect(engine.out().result().map(item => item.id)).toEqual([1, 3, 4]);
    });
});

describe("Engine - additional edge coverage", () => {
    it("covers configure + clearCaches with shared cache enabled", () => {
        IngressEngine.configure({
            maxDateCache: 8,
            maxPathCache: 8,
            sharedCache: true,
        });

        const result = from(data)
            .equals("id", 1)
            .out().result()
            .map(item => item.id);

        expect(result).toEqual([1]);
        IngressEngine.clearCaches();

        IngressEngine.configure({
            maxDateCache: 2048,
            maxPathCache: 2048,
            sharedCache: false,
        });
    });

    it("covers contains, startsWith, endsWith without ignoreCase", () => {
        expect(makeEngine().contains("name", "Al", false).out().result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().startsWith("name", "Al", false).out().result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().endsWith("name", "ma", false).out().result().map(item => item.id)).toEqual([3]);
    });

    it("covers contains with ignoreCase true", () => {
        const result = makeEngine()
            .contains("name", "AL", true)
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1]);
    });

    it("covers dateAfter/dateBefore/dateBeforeOrEqual and invalid bounds", () => {
        expect(
            makeEngine()
                .dateAfter("created", "2024-01-01T00:00:00.000Z")
                .out().result()
                .map(item => item.id)
        ).toEqual([2]);

        expect(
            makeEngine()
                .dateBefore("created", "2024-01-02T00:00:00.000Z")
                .out().result()
                .map(item => item.id)
        ).toEqual([1, 4]);

        expect(
            makeEngine()
                .dateBeforeOrEqual("created", "2024-01-02T00:00:00.000Z")
                .out().result()
                .map(item => item.id)
        ).toEqual([1, 2, 4]);

        expect(
            makeEngine()
                .dateAfter("created", "not-a-date")
                .out().result()
                .map(item => item.id)
        ).toEqual([]);
    });

    it("covers between for strings, bigints, and dates", () => {
        const stringBetween = makeEngine()
            .between("name", "Beta", "Delta")
            .out().result()
            .map(item => item.id);
        expect(stringBetween).toEqual([2, 4]);

        const bigData: Array<{ big: bigint; id: number; }> = [
            { big: 1n, id: 1 },
            { big: 10n, id: 2 },
            { big: 5n, id: 3 },
        ];

        const bigResult = from(bigData)
            .between("big", 2n, 9n)
            .out().result()
            .map(item => item.id);
        expect(bigResult).toEqual([3]);

        const dateData: Array<{ id: number; when: Date }> = [
            { id: 1, when: new Date("2024-01-01T00:00:00.000Z") },
            { id: 2, when: new Date("2024-01-03T00:00:00.000Z") },
            { id: 3, when: new Date("invalid") },
        ];

        const dateResult = from(dateData)
            .dateBetween("when", new Date("2024-01-01T00:00:00.000Z"), new Date("2024-01-02T00:00:00.000Z"))
            .out().result()
            .map(item => item.id);
        expect(dateResult).toEqual([1]);
    });

    it("returns no matches when between bounds are NaN", () => {
        const result = makeEngine()
            .between("score", Number.NaN, 10)
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([]);
    });
});

describe("Engine - extreme break tests", () => {
    it("ignores second array segment for matches but pathExists still works", () => {
        const equalsResult = makeEngine()
            .nested("Logs", q => q
                .equals("tags", "x")
            )
            .out().result()
            .map(item => item.id);
        expect(equalsResult).toEqual([1]);

        const existsResult = makeEngine()
            .nested("Logs", q => q
                .pathExists("tags")
            )
            .out().result()
            .map(item => item.id);
        expect(existsResult).toEqual([1, 2, 4]);
    });

    it("distinguishes pathExists from pathExistsNullable on undefined", () => {
        const existsResult = makeEngine()
            .pathExists("meta.owner.nickname")
            .out().result()
            .map(item => item.id);
        expect(existsResult).toEqual([1, 2, 4]);

        const existsNullableResult = makeEngine()
            .pathExistsNullable("meta.owner.nickname")
            .out().result()
            .map(item => item.id);
        expect(existsNullableResult).toEqual([1, 2]);
    });

    it("applies array helpers to non-array leaves safely", () => {
        const someResult = makeEngine()
            .arraySome("meta", meta => (meta).owner?.name === "Alice")
            .out().result()
            .map(item => item.id);
        expect(someResult).toEqual([1]);

        const everyResult = makeEngine()
            .arrayEvery("meta", meta => Boolean((meta).owner?.name))
            .out().result()
            .map(item => item.id);
        expect(everyResult).toEqual([1, 2, 3, 4]);

        const noneResult = makeEngine()
            .arrayNone("meta", meta => (meta).owner?.name === "Alice")
            .out().result()
            .map(item => item.id);
        expect(noneResult).toEqual([2, 3, 4]);
    });

    it("treats notIn with empty list as always true", () => {
        const result = makeEngine()
            .notIn("flags", [])
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 3, 4]);
    });

    it("handles NaN equality and inequality", () => {
        const weirdData = [
            { id: 1, score: Number.NaN },
            { id: 2, score: 1 },
        ];

        const equalsNaN = from(weirdData)
            .equals("score", Number.NaN)
            .out().result()
            .map(item => item.id);
        expect(equalsNaN).toEqual([]);

        const notEqualsNaN = from(weirdData)
            .notEquals("score", Number.NaN)
            .out().result()
            .map(item => item.id);
        expect(notEqualsNaN).toEqual([1, 2]);
    });

    it("returns no matches for mismatched comparison types", () => {
        const result = makeEngine()
            //@ts-expect-error - ts will complain as expected bc we try to compare a number to a string.
            .greaterThan("name", 1)
            .out().result()
            .map((item: SampleItem) => item.id);
        expect(result).toEqual([]);
    });

    it("supports dateBetween with numeric timestamps", () => {
        const min = new Date("2024-01-01T00:00:00.000Z").getTime();
        const max = new Date("2024-01-02T00:00:00.000Z").getTime();
        const result = makeEngine()
            .dateBetween("created", min, max)
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 4]);
    });

    it("strips sticky regex flags in matches", () => {
        const result = makeEngine()
            .matches("name", /a/y)
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 3, 4]);
    });

    it("missing paths do not match or exist", () => {
        const equalsResult = makeEngine()
            //@ts-expect-error - ts will complain since paths are typechecked and we test invalid path here
            .equals("misc.missing.value", "x")
            .out().result()
            .map(item => item.id);
        expect(equalsResult).toEqual([]);

        const existsResult = makeEngine()
            //@ts-expect-error - ts will complain since paths are typechecked and we test invalid path here
            .pathExists("misc.missing.value")
            .out().result()
            .map(item => item.id);
        expect(existsResult).toEqual([]);
    });

    it("nested filters handle empty arrays safely", () => {
        const result = makeEngine()
            .nested("Logs", q => q.equals("type", "OTHER"))
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([2]);
    });

    it("ignores second array segment for direct path queries", () => {
        const equalsResult = makeEngine()
            // @ts-expect-error - this path violates one-array rule
            .equals("Logs.tags", "x")
            .out().result()
            .map(item => item.id);
        expect(equalsResult).toEqual([]);

        const existsResult = makeEngine()
            // @ts-expect-error - this path violates one-array rule
            .pathExists("Logs.tags")
            .out().result()
            .map(item => item.id);
        expect(existsResult).toEqual([1, 2, 4]);
    });
});

describe("Engine - order stability and offsets", () => {
    it("keeps stable order when primary keys tie", () => {
        const stableData = [
            { id: 1, name: "b", score: 5 },
            { id: 2, name: "a", score: 5 },
            { id: 3, name: "c", score: 5 },
        ];

        const result = Engine.from(IngressEngine.from(stableData))
            .out().orderBy("score")
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 3]);
    });

    it("applies multi-key ordering with stable ties", () => {
        const items = [
            { id: 1, label: "z", name: "b", score: 1 },
            { id: 2, label: "a", name: "b", score: 1 },
            { id: 3, label: "z", name: "a", score: 1 },
            { id: 4, label: "a", name: "a", score: 2 },
        ];

        const result = Engine.from(IngressEngine.from(items))
            .out().orderBy("score")
            .thenBy("name")
            .thenBy("label")
            .result()
            .map(item => item.id);
        expect(result).toEqual([3, 2, 1, 4]);
    });

    it("respects null ordering with stable ties", () => {
        const items = [
            { id: 1, name: "b" },
            { id: 2, name: null },
            { id: 3, name: "a" },
            { id: 4, name: null },
        ];

        const first = Engine.from(IngressEngine.from(items))
            .out().orderBy("name", { nulls: "first" })
            .result()
            .map(item => item.id);
        expect(first).toEqual([2, 4, 3, 1]);

        const last = Engine.from(IngressEngine.from(items))
            .out().orderBy("name")
            .result()
            .map(item => item.id);
        expect(last).toEqual([3, 1, 2, 4]);
    });

    it("keeps offset+limit consistent with full sort", () => {
        const items: Array<{ id: number; name: string | null; score: number; }> = [];
        for (let i = 0; i < 40; i++) {
            items.push({ id: i, name: i % 7 === 0 ? null : `n${i % 3}`, score: i % 5 });
        }

        const baseline = [...items]
            .map((item, index) => ({ index, item }))
            .toSorted((a, b) => {
                const aKey = a.item.score;
                const bKey = b.item.score;
                if (aKey !== bKey) {return aKey - bKey;}
                if (a.item.name === b.item.name) {return a.index - b.index;}
                if (a.item.name === null) {return 1;}
                if (b.item.name === null) {return -1;}
                return a.item.name < b.item.name ? -1 : (a.item.name > b.item.name ? 1 : a.index - b.index);
            })
            .map(entry => entry.item.id);

        const result = Engine.from(IngressEngine.from(items))
            .out().orderBy("score")
            .thenBy("name")
            .offset(5)
            .limit(10)
            .result()
            .map(item => item.id);
        expect(result).toEqual(baseline.slice(5, 15));
    });
});

describe("Engine - segment fast paths", () => {
    it("handles two- and three-segment paths consistently", () => {
        const result = makeEngine()
            .equals("HandledBy.SalesRep", "NHR")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);

        const nestedResult = makeEngine()
            .equals("meta.owner.name", "Bob")
            .out().result()
            .map(item => item.id);
        expect(nestedResult).toEqual([2]);
    });

    it("matches when leaf is an array at segment 2", () => {
        const result = makeEngine()
            .equals("metrics.values", 2)
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1]);
    });
});

describe("Engine - large randomized dataset", () => {
    it("keeps ordering and filtering consistent on 12k items", () => {
        type LargeItem = {
            active: boolean;
            created: Date | string;
            flags: Array<string>;
            id: number;
            Logs: Array<{ tags: Array<string>; type: string; when: Date | string }>;
            meta: {
                owner: {
                    name: string;
                    nickname?: string | null;
                };
            };
            name: string | null;
            score: number;
        };

        const mulberry32 = (seed: number) => {
            let t = seed;
            return () => {
                t += 0x6D_2B_79_F5;
                let value = t;
                value = Math.imul(value ^ (value >>> 15), value | 1);
                value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
                return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
            };
        };

        const rng = mulberry32(1337);
        const alphabet = "abcdefghijklmnopqrstuvwxyz";
        const randomInt = (max: number) => Math.floor(rng() * max);
        const randomString = (len: number) => {
            let out = "";
            for (let i = 0; i < len; i++) {
                out += alphabet[randomInt(alphabet.length)]!;
            }
            return out;
        };

        const ownerNames = ["Alice", "Bob", "Cara", "Dee", "Eli"];
        const logTypes = ["WARN", "INFO", "ERROR"];
        const tags = ["red", "green", "blue", "amber"];

        const data: Array<LargeItem> = [];
        for (let i = 0; i < 12_000; i++) {
            const baseName = i % 7 === 0 ? `ab${randomString(4)}` : randomString(6);
            const name = i % 31 === 0 ? null : baseName;
            const score = i % 97 === 0 ? Number.NaN : randomInt(50);
            const created = i % 2 === 0
                ? new Date(2024, 0, (i % 28) + 1)
                : `2024-01-${String((i % 28) + 1).padStart(2, "0")}T00:00:00.000Z`;
            const owner = ownerNames[randomInt(ownerNames.length)]!;
            const nick = rng() > 0.8 ? `${owner[0]}${randomInt(9)}` : null;
            const logCount = randomInt(3);
            const Logs: Array<{ tags: Array<string>; type: string; when: Date | string }> = [];
            for (let j = 0; j < logCount; j++) {
                const type = logTypes[randomInt(logTypes.length)]!;
                const tagCount = randomInt(3);
                const logTags: Array<string> = [];
                for (let k = 0; k < tagCount; k++) {
                    logTags.push(tags[randomInt(tags.length)]!);
                }
                const when = j % 2 === 0
                    ? new Date(2024, 0, (i % 28) + 1)
                    : `2024-01-${String((i % 28) + 1).padStart(2, "0")}T00:00:00.000Z`;
                Logs.push({ tags: logTags, type, when });
            }
            const flagCount = randomInt(3);
            const flags: Array<string> = [];
            for (let j = 0; j < flagCount; j++) {
                flags.push(tags[randomInt(tags.length)]!);
            }

            data.push({
                active: i % 2 === 0,
                created,
                flags,
                id: i,
                Logs,
                meta: { owner: { name: owner, nickname: nick } },
                name,
                score,
            });
        }

        const engineResult = from(data)
            .equals("active", true)
            .greaterThanOrEqual("score", 20)
            .contains("name", "ab", true)
            .out()
            .orderBy("score")
            .thenBy("name")
            .offset(25)
            .limit(50)
            .result()
            .map(item => item.id);

        const compareNullable = (
            left: number | string | null,
            right: number | string | null,
            direction: 1 | -1,
            nullsFirst: boolean
        ) => {
            const leftNull = left === null;
            const rightNull = right === null;
            if (leftNull || rightNull) {
                if (leftNull && rightNull) {return 0;}
                if (leftNull) {return nullsFirst ? -1 : 1;}
                return nullsFirst ? 1 : -1;
            }
            if (typeof left === "number") {return (left - (right as number)) * direction;}
            if (typeof left === "string") {return (left < (right as string) ? -1 : (left > (right as string) ? 1 : 0)) * direction;}
            return 0;
        };

        const baseline = data
            .map((item, index) => ({ index, item }))
            .filter(({ item }) => {
                if (!item.active) {return false;}
                if (!(typeof item.score === "number") || Number.isNaN(item.score)) {return false;}
                if (item.score < 20) {return false;}
                if (typeof item.name !== "string") {return false;}
                return item.name.toLowerCase().includes("ab");
            })
            .toSorted((a, b) => {
                const leftScore = Number.isNaN(a.item.score) ? null : a.item.score;
                const rightScore = Number.isNaN(b.item.score) ? null : b.item.score;
                const diff0 = compareNullable(leftScore, rightScore, 1, false);
                if (diff0 !== 0) {return diff0;}
                const leftName = typeof a.item.name === "string" ? a.item.name : null;
                const rightName = typeof b.item.name === "string" ? b.item.name : null;
                const diff1 = compareNullable(leftName, rightName, 1, false);
                if (diff1 !== 0) {return diff1;}
                return a.index - b.index;
            })
            .slice(25, 75)
            .map(({ item }) => item.id);

        expect(engineResult).toEqual(baseline);

        const nestedResult = from(data)
            .nested("Logs", q => q.equals("type", "WARN"))
            .out().result()
            .map(item => item.id);

        const nestedBaseline = data
            .filter(item => item.Logs.some(log => log.type === "WARN"))
            .map(item => item.id);

        expect(nestedResult).toEqual(nestedBaseline);

        const grouped = from(data)
            .equals("active", true)
            .out().groupBy("meta.owner.name");

        const baselineGroups = new Map<string, number>();
        for (const item of data) {
            if (!item.active) {continue;}
            const owner = item.meta.owner.name;
            baselineGroups.set(owner, (baselineGroups.get(owner) ?? 0) + 1);
        }

        expect(grouped.size).toEqual(baselineGroups.size);
        for (const [key, count] of baselineGroups) {
            expect(grouped.get(key)?.length ?? 0).toEqual(count);
        }
    });
});

describe("Engine - ordering, limiting, pagination, grouping", () => {
    it("orders ascending with stable ties", () => {
        const result = makeEngine()
            .out()
            .orderBy("score")
            .result()
            .map(item => item.id);
        expect(result).toEqual([3, 1, 4, 2]);
    });

    it("orders descending with thenBy", () => {
        const result = makeEngine()
            .out()
            .orderBy("score", { direction: "desc" })
            .thenBy("name")
            .result()
            .map(item => item.id);
        expect(result).toEqual([2, 1, 4, 3]);
    });

    it("orders dates with date resolver", () => {
        const result = makeEngine()
            .out()
            .orderByDate("created", { direction: "asc" })
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 4, 2, 3]);
    });

    it("applies offset + limit without sorting", () => {
        const result = makeEngine()
            .equals("active", true)
            .out()
            .offset(1)
            .limit(1)
            .result()
            .map(item => item.id);
        expect(result).toEqual([3]);
    });

    it("applies offset + limit after ordering", () => {
        const result = makeEngine()
            .out()
            .orderBy("name")
            .offset(1)
            .limit(2)
            .result()
            .map(item => item.id);
        expect(result).toEqual([2, 4]);
    });

    it("paginates without ordering (streaming)", () => {
        const cursor = makeEngine()
            .equals("active", true)
            .out()
            .paginate({ pageSize: 2 });
        expect(cursor.data.map(item => item.id)).toEqual([1, 3]);
        expect(cursor.total).toBeUndefined();

        const next = cursor.next();
        expect(next.data.map(item => item.id)).toEqual([4]);
    });

    it("paginates with ordering and total", () => {
        const cursor = makeEngine()
            .equals("active", true)
            .out()
            .orderBy("name")
            .paginate({ pageSize: 2, total: "full" });
        expect(cursor.data.map(item => item.id)).toEqual([1, 4]);
        expect(cursor.total).toEqual(3);

        const next = cursor.next();
        expect(next.data.map(item => item.id)).toEqual([3]);

        const prev = next.previous();
        expect(prev.data.map(item => item.id)).toEqual([1, 4]);
    });

    it("groups by scalar values", () => {
        const grouped = makeEngine().out().groupBy("HandledBy.SalesRep");
        expect(grouped.get("NHR")?.map(item => item.id)).toEqual([1, 4]);
        expect(grouped.get("THS")?.map(item => item.id)).toEqual([3]);
    });

    it("groups by array leaf values", () => {
        const grouped = makeEngine().out().groupBy("flags");
        expect(grouped.get("red")?.map(item => item.id)).toEqual([1, 4]);
        expect(grouped.get("green")?.map(item => item.id)).toEqual([2]);
        expect(grouped.get("blue")?.map(item => item.id)).toEqual([1]);
    });

    it("groups after ordering pipeline", () => {
        const grouped = makeEngine()
            .out()
            .orderBy("name")
            .groupBy("label");
        expect(grouped.get("2024-01-01")?.map(item => item.id)).toEqual([1, 4]);
    });

    it("rejects non-sortable paths at compile time", () => {
        makeEngine()
            // @ts-expect-error - cannot sort by object path
            .out().orderBy("meta");
    });

    it("rejects non-groupable paths at compile time", () => {
        makeEngine()
            // @ts-expect-error - cannot group by object path
            .out().groupBy("meta");
    });
});

describe("Search + tagger config gating", () => {
    it("requires configureFuzzy before search string", () => {
        expect(() =>
            makeEngine()
                // @ts-expect-error - search requires configureFuzzy/configureTagger
                .search("alpha")
                .out().result()
        ).toThrow();
    });

    it("requires configureTagger before tags", () => {
        expect(() =>
            makeEngine()
                // @ts-expect-error - tags requires configureTagger
                .tags({ has: ["x"] })
                .out().result()
        ).toThrow();
    });

    it("supports configureFuzzy then search", () => {
        const result = makeEngine()
            .configureFuzzy({ fields: [{ path: "name" }] })
            .search("alp")
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1]);
    });

    it("supports configureTagger then tags", () => {
        const result = makeEngine()
            .configureTagger({
                rules: [
                    { equals: "red", field: "flags", tag: "red" },
                    { equals: "blue", field: "flags", tag: "blue" },
                ],
                tags: ["red", "blue"],
            })
            .tags({ hasAny: ["red"] })
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });
});

describe("QueryChain - schema, caching, and registry", () => {
    it("caches identical plans by hash", () => {
        const schema = Schema.inline<SampleItem>();
        const chainA = QueryChain.from(schema, q => q.equals("active", true));
        const chainB = QueryChain.from(schema, q => q.equals("active", true));
        expect(chainA.getPlan()).toBe(chainB.getPlan());
        expect(chainA.getPlan().hash).toEqual(chainB.getPlan().hash);
        expect(chainA.getPlan().predicates.length).toEqual(chainB.getPlan().predicates.length);

        const chainC = QueryChain.from(schema, q => q.equals("active", false));
        expect(chainC.getPlan()).toBe(chainA.getPlan());
        expect(chainC.getPlan().hash).toEqual(chainA.getPlan().hash);

        const ingress = IngressEngine.from(data);
        expect(chainC.out(ingress).result().map(item => item.id))
            .toEqual(chainA.out(ingress).result().map(item => item.id));

        const chainD = QueryChain.from(schema, q => q.equals("active", true).equals("score", 10));
        expect(chainD.getPlan().hash).not.toEqual(chainA.getPlan().hash);
        expect(chainD.getPlan()).not.toBe(chainA.getPlan());
    });

    it("builds QueryBuilder from chain and executes with ingress", () => {
        const schema = Schema.inline<SampleItem>();
        const chain = QueryChain.from(schema, q => q.equals("active", true).equals("score", 10));
        const ingress = IngressEngine.from(data);
        const result = chain.builder(ingress).out().result().map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("uses out/execute helpers consistently", () => {
        const schema = Schema.inline<SampleItem>();
        const chain = QueryChain.from(schema, q => q.equals("HandledBy.SalesRep", "NHR").equals("active", true));
        const ingress = IngressEngine.from(data);
        const outResult = chain.out(ingress).result().map(item => item.id);
        const execResult = chain.execute(ingress).result().map(item => item.id);
        expect(outResult).toEqual(execResult);
        expect(outResult).toEqual([1, 4]);
    });

    it("supports QueryBuilder.use with compiled chain", () => {
        const schema = Schema.inline<SampleItem>();
        const chain = QueryChain.from(schema, q => q.equals("active", true).equals("score", 10));
        const result = makeEngine()
            .equals("score", 10)
            .use(chain)
            .out().result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("chain registry registers, resolves, and removes", () => {
        const registry = new ChainRegistry<SampleItem>();
        const active = createChain<SampleItem>((item) => item.active);
        registry.register("active", compileChain(active));

        expect(registry.has("active")).toEqual(true);
        expect(registry.names()).toEqual(["active"]);
        const predicate = registry.get("active");
        expect(predicate?.(data[0]!)).toEqual(true);
        expect(predicate?.(data[1]!)).toEqual(false);

        expect(registry.remove("active")).toEqual(true);
        expect(registry.get("active")).toBeUndefined();
        registry.clear();
        expect(registry.names()).toEqual([]);
    });

    it("reusable chain composition supports and/or/not", () => {
        const isActive = createChain<SampleItem>((item) => item.active);
        const isHigh = createChain<SampleItem>((item) => item.score > 15);
        const combined = isActive.and(isHigh.not());

        const predicate = compileChain(combined);
        const hits = data.filter(item => predicate(item)).map(item => item.id);
        expect(hits).toEqual([1, 3, 4]);

        const orPredicate = compileChain(isActive.or(isHigh));
        const orHits = data.filter(item => orPredicate(item)).map(item => item.id);
        expect(orHits).toEqual([1, 2, 3, 4]);
    });
});

describe("Predicate helpers - compare and range", () => {
    it("compare predicates reject invalid or mismatched types", () => {
        const gtDate = createComparePredicate(new Date("invalid"), "gt");
        expect(gtDate(new Date("2024-01-01T00:00:00.000Z"))).toEqual(false);

        const gtNumber = createComparePredicate(10, "gt");
        expect(gtNumber(11)).toEqual(true);
        expect(gtNumber("11")).toEqual(false);

        const gteString = createComparePredicate("m", "gte");
        expect(gteString("m")).toEqual(true);
        expect(gteString("a")).toEqual(false);

        const ltBig = createComparePredicate(10n, "lt");
        expect(ltBig(9n)).toEqual(true);
        expect(ltBig(11n)).toEqual(false);
    });

    it("between predicate rejects NaN and mismatched types", () => {
        const numBetween = createBetweenPredicate(1, 3);
        expect(numBetween(2)).toEqual(true);
        expect(numBetween("2")).toEqual(false);

        const badBetween = createBetweenPredicate(1, "3");
        expect(badBetween(2)).toEqual(false);

        const nanBetween = createBetweenPredicate(Number.NaN, 2);
        expect(nanBetween(1)).toEqual(false);

        const dateBetween = createBetweenPredicate(
            new Date("2024-01-01T00:00:00.000Z"),
            new Date("2024-01-02T00:00:00.000Z")
        );
        expect(dateBetween(new Date("2024-01-01T12:00:00.000Z"))).toEqual(true);
        expect(dateBetween("2024-01-01")).toEqual(false);
    });
});

describe("Comparator + heap helpers", () => {
    it("compareNullable handles nulls and direction", () => {
        expect(compareNullable(null, null, 1, false)).toEqual(0);
        expect(compareNullable(null, 1, 1, true)).toBeLessThan(0);
        expect(compareNullable(null, 1, 1, false)).toBeGreaterThan(0);
        expect(compareNullable(2, 1, 1, false)).toBeGreaterThan(0);
        expect(compareNullable(2, 1, -1, false)).toBeLessThan(0);
        expect(compareNullable(2n, 3n, 1, false)).toBeLessThan(0);
    });

    it("createComparator respects multiple keys and stability", () => {
        const orders = [
            { direction: 1 as const, nullsFirst: false, resolve: (v: unknown) => typeof v === "number" ? v : null, segments: ["score"] },
            { direction: 1 as const, nullsFirst: false, resolve: (v: unknown) => typeof v === "string" ? v : null, segments: ["name"] },
            { direction: -1 as const, nullsFirst: true, resolve: (v: unknown) => typeof v === "string" ? v : null, segments: ["label"] },
            { direction: 1 as const, nullsFirst: false, resolve: (v: unknown) => typeof v === "number" ? v : null, segments: ["id"] },
        ];
        const compare = createComparator<{ id: number }>(orders);
        const entries = [
            { index: 0, item: { id: 1 }, keys: [2, "b", "z", 10] },
            { index: 1, item: { id: 2 }, keys: [2, "a", "z", 9] },
            { index: 2, item: { id: 3 }, keys: [2, "a", "a", 8] },
            { index: 3, item: { id: 4 }, keys: [1, "c", "z", 7] },
        ];
        entries.sort(compare);
        expect(entries.map(entry => entry.item.id)).toEqual([4, 2, 3, 1]);
    });

    it("heap helpers keep max-heap order", () => {
        const compare = (a: number, b: number) => a - b;
        const heap: Array<number> = [];
        heapPush(heap, 3, compare);
        heapPush(heap, 1, compare);
        heapPush(heap, 5, compare);
        expect(heap[0]).toEqual(5);
        heapReplaceRoot(heap, 2, compare);
        expect(heap[0]).toEqual(3);
    });
});

describe("Cache + path helpers", () => {
    it("segments cache evicts when maxPathCache exceeded", () => {
        const cache = createCacheState({ maxDateCache: 2, maxPathCache: 2 });
        getSegments(cache, "a.b");
        getSegments(cache, "c.d");
        getSegments(cache, "e.f");
        expect(cache.pathSegmentsCache.size).toEqual(2);
    });

    it("toTimestamp parses Date/number/ISO and rejects invalid", () => {
        const cache = createCacheState({ maxDateCache: 2, maxPathCache: 2 });
        const dateTime = toTimestamp(new Date("2024-01-01T00:00:00.000Z"), cache.parseIsoDate);
        expect(dateTime).not.toBeNull();
        expect(toTimestamp(123, cache.parseIsoDate)).toEqual(123);
        const isoTime = toTimestamp("2024-01-01T00:00:00.000Z", cache.parseIsoDate);
        expect(isoTime).not.toBeNull();
        expect(toTimestamp("not-a-date", cache.parseIsoDate)).toBeNull();
        expect(toTimestamp(true, cache.parseIsoDate)).toBeNull();
    });

    it("path helpers resolve and iterate across deep arrays", () => {
        const obj: ResolveObject = {
            a: { b: [{ c: 1 }, { c: 2 }, { c: 3 }] },
        };
        const segments = ["a", "b", "c", "value"];
        expect(pathExistsWithSegments(obj, ["a"])).toEqual(true);
        expect(pathExistsWithSegments(obj, segments)).toEqual(false);

        const flatSegments = ["a", "b", "c"];
        const found = someResolvedWithSegments(obj, flatSegments, value => value === 2);
        expect(found).toEqual(true);

        const first = resolveFirstWithSegments(obj, flatSegments);
        expect(first).toEqual(1);

        const values: Array<number> = [];
        forEachResolvedWithSegments(obj, flatSegments, value => {
            if (typeof value === "number") {values.push(value);}
        });
        expect(values).toEqual([1, 2, 3]);

        const every = everyResolvedWithSegments(obj, flatSegments, value => typeof value === "number");
        expect(every).toEqual(true);

        const deepObj: ResolveObject = {
            a: { b: { c: [{ d: 1 }, { d: 2 }] } },
        };
        const deepSegments = ["a", "b", "c", "d"];
        expect(someResolvedWithSegments(deepObj, deepSegments, value => value === 2)).toEqual(true);
        expect(resolveFirstWithSegments(deepObj, deepSegments)).toEqual(1);
        const deepValues: Array<number> = [];
        forEachResolvedWithSegments(deepObj, deepSegments, value => {
            if (typeof value === "number") {deepValues.push(value);}
        });
        expect(deepValues).toEqual([1, 2]);
        expect(everyResolvedWithSegments(deepObj, deepSegments, value => typeof value === "number")).toEqual(true);
    });

    it("resolveOrderValueWithSegments returns null for unsupported types", () => {
        const cache = createCacheState({ maxDateCache: 4, maxPathCache: 4 });
        const orderValue = resolveOrderValueWithSegments(
            { val: { nested: { obj: { x: 1 } } } } as ResolveObject,
            ["val", "nested", "obj"],
            cache.orderResolver
        );
        expect(orderValue).toBeNull();
    });
});

describe("Ingress + schema helpers", () => {
    it("supports schema.inline and schema.infer", () => {
        const inline = Schema.inline<SampleItem>();
        const inferred = Schema.infer({ id: 1, name: "x" });
        expect(inline.source).toEqual("inline");
        expect(inferred.source).toEqual("inferred");
        expect(inferred.sample).toEqual({ id: 1, name: "x" });
    });

    it("ingress load, loadFrom, clear, length, isEmpty", () => {
        const ingress = IngressEngine.from(data);
        expect(ingress.length).toEqual(4);
        expect(ingress.isEmpty()).toEqual(false);

        const cleared = ingress.clear();
        expect(cleared.length).toEqual(0);
        expect(cleared.isEmpty()).toEqual(true);

        const reloaded = cleared.load(data);
        expect(reloaded.length).toEqual(4);

        const loadedFrom = ingress.loadFrom({ payload: data.slice(0, 2) }, (input) => input.payload);
        expect(loadedFrom.length).toEqual(2);
    });

    it("ingress create respects shared cache usage", () => {
        IngressEngine.configure({ maxDateCache: 2, maxPathCache: 2, sharedCache: true });
        const a = IngressEngine.create<SampleItem>();
        const b = IngressEngine.create<SampleItem>();
        expect(a.cache).toBe(b.cache);
        IngressEngine.clearCaches();
        IngressEngine.configure({ maxDateCache: 2048, maxPathCache: 2048, sharedCache: false });
    });
});

describe("Execution + egress edge cases", () => {
    it("execution returns clone when no predicates", () => {
        const ingress = IngressEngine.from(data);
        const plan = Engine.from(ingress).compilePlan();
        const executor = new ExecutionEngine<SampleItem>();
        const result = executor.execute(ingress, plan);
        expect(result).toEqual(data);
        expect(result).not.toBe(data);
    });

    it("egress limit/offset/page sanitize invalid values", () => {
        const offsetOnly = makeEngine()
            .out()
            .offset(-5)
            .result()
            .map(item => item.id);
        expect(offsetOnly).toEqual([1, 2, 3, 4]);

        const limited = makeEngine()
            .out()
            .limit(Number.NaN)
            .result()
            .map(item => item.id);
        expect(limited).toEqual([]);

        const paged = makeEngine().out().page(0, 0).result().map(item => item.id);
        expect(paged).toEqual([1]);
    });

    it("egress paginate handles out-of-range pages", () => {
        const cursor = makeEngine()
            .out()
            .paginate({ page: 10, pageSize: 2, total: "lazy" });
        expect(cursor.data).toEqual([]);
        expect(cursor.total).toEqual(4);
        const prev = cursor.previous();
        expect(prev.page).toEqual(9);
    });

    it("groupBy date option normalizes date values", () => {
        const grouped = makeEngine()
            .out()
            .groupBy("created", { date: true });
        const keys = [...grouped.keys()].filter(key => typeof key === "number");
        expect(keys.length).toBeGreaterThan(0);
    });
});
