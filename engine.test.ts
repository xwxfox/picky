import { describe, it, expect } from "bun:test";
import { FilterEngine } from ".";

type LogEntry = {
    type: string;
    message: string | null;
    tags: string[];
    when: Date | string;
};

type SampleItem = {
    id: number;
    name: string;
    label: string;
    active: boolean;
    score: number;
    created: Date | string | number;
    note?: string | null;
    meta: {
        owner: {
            name: string;
            nickname?: string | null;
        };
    };
    HandledBy: {
        SalesRep: string | null;
    };
    Logs: LogEntry[];
    flags: string[];
    metrics: {
        values: number[];
        maybe?: number | null;
    };
    misc?: {
        code?: string;
        nested?: {
            value?: string | null;
        };
    };
};

const data: SampleItem[] = [
    {
        id: 1,
        name: "Alpha",
        label: "2024-01-01",
        active: true,
        score: 10,
        created: new Date("2024-01-01T00:00:00.000Z"),
        note: null,
        meta: {
            owner: {
                name: "Alice",
                nickname: null,
            },
        },
        HandledBy: {
            SalesRep: "NHR",
        },
        Logs: [
            {
                type: "CREDIT_MAX_EXCEEDED",
                message: "over",
                tags: ["x"],
                when: "2024-01-03T00:00:00.000Z",
            },
        ],
        flags: ["red", "blue"],
        metrics: {
            values: [1, 2, 3],
            maybe: 0,
        },
        misc: {
            code: "X1",
            nested: {
                value: null,
            },
        },
    },
    {
        id: 2,
        name: "Beta",
        label: "release",
        active: false,
        score: 20,
        created: "2024-01-02T00:00:00.000Z",
        note: "ok",
        meta: {
            owner: {
                name: "Bob",
                nickname: "B",
            },
        },
        HandledBy: {
            SalesRep: null,
        },
        Logs: [
            {
                type: "OTHER",
                message: null,
                tags: [],
                when: new Date("2024-01-04T00:00:00.000Z"),
            },
        ],
        flags: ["green"],
        metrics: {
            values: [5],
        },
        misc: {},
    },
    {
        id: 3,
        name: "Gamma",
        label: "not-a-date",
        active: true,
        score: 5,
        created: "not-a-date",
        meta: {
            owner: {
                name: "Cara",
            },
        },
        HandledBy: {
            SalesRep: "THS",
        },
        Logs: [],
        flags: [],
        metrics: {
            values: [],
            maybe: null,
        },
    },
    {
        id: 4,
        name: "Delta",
        label: "2024-01-01",
        active: true,
        score: 10,
        created: "2024-01-01T00:00:00.000Z",
        note: undefined,
        meta: {
            owner: {
                name: "Dee",
                nickname: undefined,
            },
        },
        HandledBy: {
            SalesRep: "NHR",
        },
        Logs: [
            {
                type: "CREDIT_MAX_EXCEEDED",
                message: "dup",
                tags: ["y"],
                when: "2024-01-03T00:00:00.000Z",
            },
        ],
        flags: ["red"],
        metrics: {
            values: [10],
        },
        misc: {
            nested: {
                value: "ok",
            },
        },
    },
];

const makeEngine = () => FilterEngine.from(data);

describe("FilterEngine fast - core basics", () => {
    it("returns all items with no filters", () => {
        const result = makeEngine().result().map(item => item.id);
        expect(result).toEqual([1, 2, 3, 4]);
    });

    it("compile matches result", () => {
        const engine = makeEngine().equals("active", true);
        const predicate = engine.compile();
        expect(data.filter(predicate).map(item => item.id)).toEqual([1, 3, 4]);
        expect(engine.result().map(item => item.id)).toEqual([1, 3, 4]);
    });
});

describe("FilterEngine fast - logical grouping", () => {
    it("and groups predicates", () => {
        const result = makeEngine()
            .and(q => q.equals("active", true).greaterThan("score", 8))
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("or groups compiled predicates", () => {
        const result = makeEngine()
            .equals("id", 1)
            .or(q => q.equals("id", 2))
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 2]);
    });

    it("or on empty engine uses group", () => {
        const result = makeEngine()
            .or(q => q.equals("id", 2))
            .result()
            .map(item => item.id);
        expect(result).toEqual([2]);
    });

    it("not negates group", () => {
        const result = makeEngine()
            .not(q => q.equals("id", 1))
            .result()
            .map(item => item.id);
        expect(result).toEqual([2, 3, 4]);
    });
});

describe("FilterEngine fast - equals and notEquals", () => {
    it("matches nested path values", () => {
        const result = makeEngine()
            .equals("meta.owner.name", "Alice")
            .result()
            .map(item => item.id);
        expect(result).toEqual([1]);
    });

    it("matches array values at leaf", () => {
        const result = makeEngine()
            .equals("flags", "green")
            .result()
            .map(item => item.id);
        expect(result).toEqual([2]);
    });

    it("matches date-like strings by strict equality", () => {
        const result = makeEngine()
            .equals("label", "2024-01-01")
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("notEquals is strict negation of equals for arrays", () => {
        const result = makeEngine()
            .notEquals("flags", "red")
            .result()
            .map(item => item.id);
        expect(result).toEqual([2, 3]);
    });

    it("supports date equality across Date and ISO string", () => {
        const result = makeEngine()
            .dateEquals("created", new Date("2024-01-02T00:00:00.000Z"))
            .result()
            .map(item => item.id);
        expect(result).toEqual([2]);
    });

    it("compares Date fields to numeric timestamps", () => {
        const result = makeEngine()
            .dateEquals("created", new Date("2024-01-01T00:00:00.000Z").getTime())
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("keeps reference equality for objects", () => {
        const metaRef = data[0]!.meta;
        const hit = makeEngine().equals("meta", metaRef).result().map(item => item.id);
        expect(hit).toEqual([1]);

        const miss = makeEngine().equals("meta", { owner: { name: "Alice" } }).result().map(item => item.id);
        expect(miss).toEqual([]);
    });
});

describe("FilterEngine fast - comparison operators", () => {
    it("handles greater/less variants with numbers", () => {
        expect(makeEngine().greaterThan("score", 9).result().map(item => item.id)).toEqual([1, 2, 4]);
        expect(makeEngine().greaterThanOrEqual("score", 10).result().map(item => item.id)).toEqual([1, 2, 4]);
        expect(makeEngine().lessThan("score", 10).result().map(item => item.id)).toEqual([3]);
        expect(makeEngine().lessThanOrEqual("score", 10).result().map(item => item.id)).toEqual([1, 3, 4]);
    });

    it("supports between for numbers", () => {
        const result = makeEngine()
            .between("score", 6, 20)
            .result()
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
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 4]);
    });

    it("dateBetween requires both bounds to be date-ish", () => {
        const result = makeEngine()
            .dateBetween("created", "2024-01-01T00:00:00.000Z", "not-a-date")
            .result()
            .map(item => item.id);
        expect(result).toEqual([]);
    });

    it("compares strings lexicographically", () => {
        const result = makeEngine()
            .greaterThan("name", "Beta")
            .result()
            .map(item => item.id);
        expect(result).toEqual([3, 4]);
    });
});

describe("FilterEngine fast - membership and string operations", () => {
    it("supports in and notIn", () => {
        expect(makeEngine().in("id", [1, 3]).result().map(item => item.id)).toEqual([1, 3]);
        expect(makeEngine().notIn("id", [1, 2]).result().map(item => item.id)).toEqual([3, 4]);
        expect(makeEngine().notIn("flags", ["red"]).result().map(item => item.id)).toEqual([2, 3]);
    });

    it("supports contains, startsWith, endsWith", () => {
        expect(makeEngine().contains("name", "ph").result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().startsWith("name", "be", true).result().map(item => item.id)).toEqual([2]);
        expect(makeEngine().endsWith("name", "MA", true).result().map(item => item.id)).toEqual([3]);
    });

    it("supports matches with regex and resets /g state", () => {
        expect(makeEngine().matches("name", /^A/).result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().matches("name", /a/g).result().map(item => item.id)).toEqual([1, 2, 3, 4]);
    });

    it("matches array leaf values", () => {
        const result = makeEngine()
            .equals("metrics.values", 2)
            .result()
            .map(item => item.id);
        expect(result).toEqual([1]);
    });
});

describe("FilterEngine fast - null and existence semantics", () => {
    it("handles isNull and isNotNull", () => {
        expect(makeEngine().isNull("note").result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().valueNotNull("note").result().map(item => item.id)).toEqual([2]);
    });

    it("checks exists for defined values", () => {
        expect(makeEngine().pathExists("note").result().map(item => item.id)).toEqual([1, 2, 4]);
        expect(makeEngine().pathExists("misc.code").result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().pathExists("misc.nested.value").result().map(item => item.id)).toEqual([1, 4]);
    });

    it("supports pathExistsNullable", () => {
        expect(makeEngine().pathExistsNullable("note").result().map(item => item.id)).toEqual([1, 2]);
        expect(makeEngine().pathExistsNullable("misc.nested.value").result().map(item => item.id)).toEqual([1, 4]);
    });

    it("treats undefined as not-null false", () => {
        expect(makeEngine().valueNotNull("meta.owner.nickname").result().map(item => item.id)).toEqual([2]);
    });
});

describe("FilterEngine fast - array helpers", () => {
    it("supports arraySome, arrayEvery, arrayNone", () => {
        expect(makeEngine().arraySome("flags", v => v === "green").result().map(item => item.id)).toEqual([2]);
        expect(makeEngine().arrayEvery("flags", v => v.length >= 4).result().map(item => item.id)).toEqual([2]);
        expect(makeEngine().arrayNone("flags", v => v.length < 4).result().map(item => item.id)).toEqual([2, 3]);
    });

    it("arrayEvery fails on empty arrays", () => {
        expect(makeEngine().arrayEvery("flags", () => true).result().map(item => item.id)).toEqual([1, 2, 4]);
    });
});

describe("FilterEngine fast - nested arrays and paths", () => {
    it("supports nested filtering for arrays of objects", () => {
        const result = makeEngine()
            .nested("Logs", q => q.equals("type", "CREDIT_MAX_EXCEEDED"))
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("supports a single array segment in paths", () => {
        const result = makeEngine()
            .equals("Logs.type", "CREDIT_MAX_EXCEEDED")
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("matches nested date comparisons in array objects", () => {
        const result = makeEngine()
            .nested("Logs", q => q.dateAfterOrEqual("when", new Date("2024-01-03T00:00:00.000Z")))
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 4]);
    });

    it("supports nested array predicates on array fields", () => {
        const result = makeEngine()
            .nested("Logs", q => q.arraySome("tags", tag => tag === "x"))
            .result()
            .map(item => item.id);
        expect(result).toEqual([1]);
    });
});

describe("FilterEngine fast - aggressive edge cases", () => {
    it("handles nullish segments without throwing", () => {
        const result = makeEngine()
            .equals("misc.nested.value", "ok")
            .result()
            .map(item => item.id);
        expect(result).toEqual([4]);
    });

    it("handles null value comparisons", () => {
        expect(makeEngine().equals("note", null).result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().notEquals("note", null).result().map(item => item.id)).toEqual([2, 3, 4]);
    });

    it("handles empty resolvers for arrays", () => {
        const result = makeEngine()
            .equals("Logs.type", "MISSING")
            .result()
            .map(item => item.id);
        expect(result).toEqual([]);
    });

    it("handles exists on optional nested paths", () => {
        const result = makeEngine()
            .pathExists("misc.nested.value")
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("handles multiple predicates efficiently", () => {
        const engine = makeEngine()
            .equals("active", true)
            .equals("HandledBy.SalesRep", "NHR")
            .greaterThanOrEqual("score", 10)
            .not(q => q.equals("name", "Gamma"));

        const result = engine.result().map(item => item.id);
        expect(result).toEqual([1, 4]);
    });

    it("keeps working when compiled after many chains", () => {
        let engine = makeEngine();
        for (let i = 0; i < 20; i++) {
            engine = engine.and(q => q.equals("active", true));
        }
        expect(engine.result().map(item => item.id)).toEqual([1, 3, 4]);
        expect(engine.compile()(data[0]!)).toEqual(true);
    });
});

describe("FilterEngine fast - additional edge coverage", () => {
    it("covers configure + clearCaches with shared cache enabled", () => {
        FilterEngine.configure({
            sharedCache: true,
            maxDateCache: 8,
            maxPathCache: 8,
        });

        const result = FilterEngine.from(data)
            .equals("id", 1)
            .result()
            .map(item => item.id);

        expect(result).toEqual([1]);
        FilterEngine.clearCaches();

        FilterEngine.configure({
            sharedCache: false,
            maxDateCache: 2048,
            maxPathCache: 2048,
        });
    });

    it("covers contains, startsWith, endsWith without ignoreCase", () => {
        expect(makeEngine().contains("name", "Al", false).result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().startsWith("name", "Al", false).result().map(item => item.id)).toEqual([1]);
        expect(makeEngine().endsWith("name", "ma", false).result().map(item => item.id)).toEqual([3]);
    });

    it("covers contains with ignoreCase true", () => {
        const result = makeEngine()
            .contains("name", "AL", true)
            .result()
            .map(item => item.id);
        expect(result).toEqual([1]);
    });

    it("covers dateAfter/dateBefore/dateBeforeOrEqual and invalid bounds", () => {
        expect(
            makeEngine()
                .dateAfter("created", "2024-01-01T00:00:00.000Z")
                .result()
                .map(item => item.id)
        ).toEqual([2]);

        expect(
            makeEngine()
                .dateBefore("created", "2024-01-02T00:00:00.000Z")
                .result()
                .map(item => item.id)
        ).toEqual([1, 4]);

        expect(
            makeEngine()
                .dateBeforeOrEqual("created", "2024-01-02T00:00:00.000Z")
                .result()
                .map(item => item.id)
        ).toEqual([1, 2, 4]);

        expect(
            makeEngine()
                .dateAfter("created", "not-a-date")
                .result()
                .map(item => item.id)
        ).toEqual([]);
    });

    it("covers between for strings, bigints, and dates", () => {
        const stringBetween = makeEngine()
            .between("name", "Beta", "Delta")
            .result()
            .map(item => item.id);
        expect(stringBetween).toEqual([2, 4]);

        const bigData: Array<{ id: number; big: bigint }> = [
            { id: 1, big: 1n },
            { id: 2, big: 10n },
            { id: 3, big: 5n },
        ];

        const bigResult = FilterEngine.from(bigData)
            .between("big", 2n, 9n)
            .result()
            .map(item => item.id);
        expect(bigResult).toEqual([3]);

        const dateData: any[] = [
            { id: 1, when: new Date("2024-01-01T00:00:00.000Z") },
            { id: 2, when: new Date("2024-01-03T00:00:00.000Z") },
            { id: 3, when: new Date("invalid") },
        ];

        const dateResult = FilterEngine.from(dateData)
            .between("when", new Date("2024-01-01T00:00:00.000Z"), new Date("2024-01-02T00:00:00.000Z"))
            .result()
            .map(item => item.id);
        expect(dateResult).toEqual([1]);
    });

    it("returns no matches when between bounds are NaN", () => {
        const result = makeEngine()
            .between("score", Number.NaN, 10)
            .result()
            .map(item => item.id);
        expect(result).toEqual([]);
    });
});

describe("FilterEngine fast - extreme break tests", () => {
    it("ignores second array segment for matches but pathExists still works", () => {
        const equalsResult = makeEngine()
            .nested("Logs", q => q
                .equals("tags", "x")
            )
            .result()
            .map(item => item.id);
        expect(equalsResult).toEqual([1]);

        const existsResult = makeEngine()
            .nested("Logs", q => q
                .pathExists("tags")
            )
            .result()
            .map(item => item.id);
        expect(existsResult).toEqual([1, 2, 4]);
    });

    it("distinguishes pathExists from pathExistsNullable on undefined", () => {
        const existsResult = makeEngine()
            .pathExists("meta.owner.nickname")
            .result()
            .map(item => item.id);
        expect(existsResult).toEqual([1, 2, 4]);

        const existsNullableResult = makeEngine()
            .pathExistsNullable("meta.owner.nickname")
            .result()
            .map(item => item.id);
        expect(existsNullableResult).toEqual([1, 2]);
    });

    it("applies array helpers to non-array leaves safely", () => {
        const someResult = makeEngine()
            .arraySome("meta", meta => (meta).owner?.name === "Alice")
            .result()
            .map(item => item.id);
        expect(someResult).toEqual([1]);

        const everyResult = makeEngine()
            .arrayEvery("meta", meta => Boolean((meta).owner?.name))
            .result()
            .map(item => item.id);
        expect(everyResult).toEqual([1, 2, 3, 4]);

        const noneResult = makeEngine()
            .arrayNone("meta", meta => (meta).owner?.name === "Alice")
            .result()
            .map(item => item.id);
        expect(noneResult).toEqual([2, 3, 4]);
    });

    it("treats notIn with empty list as always true", () => {
        const result = makeEngine()
            .notIn("flags", [])
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 3, 4]);
    });

    it("handles NaN equality and inequality", () => {
        const weirdData = [
            { id: 1, score: Number.NaN },
            { id: 2, score: 1 },
        ];

        const equalsNaN = FilterEngine.from(weirdData)
            .equals("score", Number.NaN)
            .result()
            .map(item => item.id);
        expect(equalsNaN).toEqual([]);

        const notEqualsNaN = FilterEngine.from(weirdData)
            .notEquals("score", Number.NaN)
            .result()
            .map(item => item.id);
        expect(notEqualsNaN).toEqual([1, 2]);
    });

    it("returns no matches for mismatched comparison types", () => {
        const result = makeEngine()
            //@ts-expect-error - ts will complain as expected bc we try to compare a number to a string.
            .greaterThan("name", 1)
            .result()
            .map((item: SampleItem) => item.id);
        expect(result).toEqual([]);
    });

    it("supports dateBetween with numeric timestamps", () => {
        const min = new Date("2024-01-01T00:00:00.000Z").getTime();
        const max = new Date("2024-01-02T00:00:00.000Z").getTime();
        const result = makeEngine()
            .dateBetween("created", min, max)
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 4]);
    });

    it("strips sticky regex flags in matches", () => {
        const result = makeEngine()
            .matches("name", /a/y)
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 3, 4]);
    });

    it("missing paths do not match or exist", () => {
        const equalsResult = makeEngine()
            //@ts-expect-error - ts will complain since paths are typechecked and we test invalid path here
            .equals("misc.missing.value", "x")
            .result()
            .map(item => item.id);
        expect(equalsResult).toEqual([]);

        const existsResult = makeEngine()
            //@ts-expect-error - ts will complain since paths are typechecked and we test invalid path here
            .pathExists("misc.missing.value")
            .result()
            .map(item => item.id);
        expect(existsResult).toEqual([]);
    });

    it("nested filters handle empty arrays safely", () => {
        const result = makeEngine()
            .nested("Logs", q => q.equals("type", "OTHER"))
            .result()
            .map(item => item.id);
        expect(result).toEqual([2]);
    });

    it("ignores second array segment for direct path queries", () => {
        const equalsResult = makeEngine()
            // @ts-expect-error - this path violates one-array rule
            .equals("Logs.tags", "x")
            .result()
            .map(item => item.id);
        expect(equalsResult).toEqual([]);

        const existsResult = makeEngine()
            // @ts-expect-error - this path violates one-array rule
            .pathExists("Logs.tags")
            .result()
            .map(item => item.id);
        expect(existsResult).toEqual([1, 2, 4]);
    });
});

describe("FilterEngine fast - order stability and offsets", () => {
    it("keeps stable order when primary keys tie", () => {
        const stableData = [
            { id: 1, score: 5, name: "b" },
            { id: 2, score: 5, name: "a" },
            { id: 3, score: 5, name: "c" },
        ];

        const result = FilterEngine.from(stableData)
            .orderBy("score")
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 2, 3]);
    });

    it("applies multi-key ordering with stable ties", () => {
        const items = [
            { id: 1, score: 1, name: "b", label: "z" },
            { id: 2, score: 1, name: "b", label: "a" },
            { id: 3, score: 1, name: "a", label: "z" },
            { id: 4, score: 2, name: "a", label: "a" },
        ];

        const result = FilterEngine.from(items)
            .orderBy("score")
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

        const first = FilterEngine.from(items)
            .orderBy("name", { nulls: "first" })
            .result()
            .map(item => item.id);
        expect(first).toEqual([2, 4, 3, 1]);

        const last = FilterEngine.from(items)
            .orderBy("name")
            .result()
            .map(item => item.id);
        expect(last).toEqual([3, 1, 2, 4]);
    });

    it("keeps offset+limit consistent with full sort", () => {
        const items: Array<{ id: number; score: number; name: string | null }> = [];
        for (let i = 0; i < 40; i++) {
            items.push({ id: i, score: i % 5, name: i % 7 === 0 ? null : `n${i % 3}` });
        }

        const baseline = [...items]
            .map((item, index) => ({ item, index }))
            .sort((a, b) => {
                const aKey = a.item.score;
                const bKey = b.item.score;
                if (aKey !== bKey) return aKey - bKey;
                if (a.item.name === b.item.name) return a.index - b.index;
                if (a.item.name === null) return 1;
                if (b.item.name === null) return -1;
                return a.item.name < b.item.name ? -1 : a.item.name > b.item.name ? 1 : a.index - b.index;
            })
            .map(entry => entry.item.id);

        const result = FilterEngine.from(items)
            .orderBy("score")
            .thenBy("name")
            .offset(5)
            .limit(10)
            .result()
            .map(item => item.id);
        expect(result).toEqual(baseline.slice(5, 15));
    });
});

describe("FilterEngine fast - segment fast paths", () => {
    it("handles two- and three-segment paths consistently", () => {
        const result = makeEngine()
            .equals("HandledBy.SalesRep", "NHR")
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 4]);

        const nestedResult = makeEngine()
            .equals("meta.owner.name", "Bob")
            .result()
            .map(item => item.id);
        expect(nestedResult).toEqual([2]);
    });

    it("matches when leaf is an array at segment 2", () => {
        const result = makeEngine()
            .equals("metrics.values", 2)
            .result()
            .map(item => item.id);
        expect(result).toEqual([1]);
    });
});

describe("FilterEngine fast - large randomized dataset", () => {
    it("keeps ordering and filtering consistent on 12k items", () => {
        type LargeItem = {
            id: number;
            active: boolean;
            score: number;
            name: string | null;
            created: Date | string;
            meta: {
                owner: {
                    name: string;
                    nickname?: string | null;
                };
            };
            Logs: Array<{ type: string; tags: string[]; when: Date | string }>;
            flags: string[];
        };

        const mulberry32 = (seed: number) => {
            let t = seed;
            return () => {
                t += 0x6D2B79F5;
                let value = t;
                value = Math.imul(value ^ (value >>> 15), value | 1);
                value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
                return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
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

        const data: LargeItem[] = [];
        for (let i = 0; i < 12000; i++) {
            const baseName = i % 7 === 0 ? `ab${randomString(4)}` : randomString(6);
            const name = i % 31 === 0 ? null : baseName;
            const score = i % 97 === 0 ? Number.NaN : randomInt(50);
            const created = i % 2 === 0
                ? new Date(2024, 0, (i % 28) + 1)
                : `2024-01-${String((i % 28) + 1).padStart(2, "0")}T00:00:00.000Z`;
            const owner = ownerNames[randomInt(ownerNames.length)]!;
            const nick = rng() > 0.8 ? `${owner[0]}${randomInt(9)}` : null;
            const logCount = randomInt(3);
            const Logs: Array<{ type: string; tags: string[]; when: Date | string }> = [];
            for (let j = 0; j < logCount; j++) {
                const type = logTypes[randomInt(logTypes.length)]!;
                const tagCount = randomInt(3);
                const logTags: string[] = [];
                for (let k = 0; k < tagCount; k++) {
                    logTags.push(tags[randomInt(tags.length)]!);
                }
                const when = j % 2 === 0
                    ? new Date(2024, 0, (i % 28) + 1)
                    : `2024-01-${String((i % 28) + 1).padStart(2, "0")}T00:00:00.000Z`;
                Logs.push({ type, tags: logTags, when });
            }
            const flagCount = randomInt(3);
            const flags: string[] = [];
            for (let j = 0; j < flagCount; j++) {
                flags.push(tags[randomInt(tags.length)]!);
            }

            data.push({
                id: i,
                active: i % 2 === 0,
                score,
                name,
                created,
                meta: { owner: { name: owner, nickname: nick } },
                Logs,
                flags,
            });
        }

        const engineResult = FilterEngine.from(data)
            .equals("active", true)
            .greaterThanOrEqual("score", 20)
            .contains("name", "ab", true)
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
                if (leftNull && rightNull) return 0;
                if (leftNull) return nullsFirst ? -1 : 1;
                return nullsFirst ? 1 : -1;
            }
            if (typeof left === "number") return (left - (right as number)) * direction;
            if (typeof left === "string") return (left < (right as string) ? -1 : left > (right as string) ? 1 : 0) * direction;
            return 0;
        };

        const baseline = data
            .map((item, index) => ({ item, index }))
            .filter(({ item }) => {
                if (!item.active) return false;
                if (!(typeof item.score === "number") || Number.isNaN(item.score)) return false;
                if (item.score < 20) return false;
                if (typeof item.name !== "string") return false;
                return item.name.toLowerCase().includes("ab");
            })
            .sort((a, b) => {
                const leftScore = Number.isNaN(a.item.score) ? null : a.item.score;
                const rightScore = Number.isNaN(b.item.score) ? null : b.item.score;
                const diff0 = compareNullable(leftScore, rightScore, 1, false);
                if (diff0 !== 0) return diff0;
                const leftName = typeof a.item.name === "string" ? a.item.name : null;
                const rightName = typeof b.item.name === "string" ? b.item.name : null;
                const diff1 = compareNullable(leftName, rightName, 1, false);
                if (diff1 !== 0) return diff1;
                return a.index - b.index;
            })
            .slice(25, 75)
            .map(({ item }) => item.id);

        expect(engineResult).toEqual(baseline);

        const nestedResult = FilterEngine.from(data)
            .nested("Logs", q => q.equals("type", "WARN"))
            .result()
            .map(item => item.id);

        const nestedBaseline = data
            .filter(item => item.Logs.some(log => log.type === "WARN"))
            .map(item => item.id);

        expect(nestedResult).toEqual(nestedBaseline);

        const grouped = FilterEngine.from(data)
            .equals("active", true)
            .groupBy("meta.owner.name");

        const baselineGroups = new Map<string, number>();
        for (const item of data) {
            if (!item.active) continue;
            const owner = item.meta.owner.name;
            baselineGroups.set(owner, (baselineGroups.get(owner) ?? 0) + 1);
        }

        expect(grouped.size).toEqual(baselineGroups.size);
        for (const [key, count] of baselineGroups) {
            expect(grouped.get(key)?.length ?? 0).toEqual(count);
        }
    });
});

describe("FilterEngine fast - ordering, limiting, pagination, grouping", () => {
    it("orders ascending with stable ties", () => {
        const result = makeEngine()
            .orderBy("score")
            .result()
            .map(item => item.id);
        expect(result).toEqual([3, 1, 4, 2]);
    });

    it("orders descending with thenBy", () => {
        const result = makeEngine()
            .orderBy("score", { direction: "desc" })
            .thenBy("name")
            .result()
            .map(item => item.id);
        expect(result).toEqual([2, 1, 4, 3]);
    });

    it("orders dates with date resolver", () => {
        const result = makeEngine()
            .orderByDate("created", { direction: "asc" })
            .result()
            .map(item => item.id);
        expect(result).toEqual([1, 4, 2, 3]);
    });

    it("applies offset + limit without sorting", () => {
        const result = makeEngine()
            .equals("active", true)
            .offset(1)
            .limit(1)
            .result()
            .map(item => item.id);
        expect(result).toEqual([3]);
    });

    it("applies offset + limit after ordering", () => {
        const result = makeEngine()
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
            .resultPaginated({ pageSize: 2 });
        expect(cursor.data.map(item => item.id)).toEqual([1, 3]);
        expect(cursor.total).toBeUndefined();

        const next = cursor.next();
        expect(next.data.map(item => item.id)).toEqual([4]);
    });

    it("paginates with ordering and total", () => {
        const cursor = makeEngine()
            .equals("active", true)
            .orderBy("name")
            .resultPaginated({ pageSize: 2, total: "full" });
        expect(cursor.data.map(item => item.id)).toEqual([1, 4]);
        expect(cursor.total).toEqual(3);

        const next = cursor.next();
        expect(next.data.map(item => item.id)).toEqual([3]);

        const prev = next.previous();
        expect(prev.data.map(item => item.id)).toEqual([1, 4]);
    });

    it("groups by scalar values", () => {
        const grouped = makeEngine().groupBy("HandledBy.SalesRep");
        expect(grouped.get("NHR")?.map(item => item.id)).toEqual([1, 4]);
        expect(grouped.get("THS")?.map(item => item.id)).toEqual([3]);
    });

    it("groups by array leaf values", () => {
        const grouped = makeEngine().groupBy("flags");
        expect(grouped.get("red")?.map(item => item.id)).toEqual([1, 4]);
        expect(grouped.get("green")?.map(item => item.id)).toEqual([2]);
        expect(grouped.get("blue")?.map(item => item.id)).toEqual([1]);
    });

    it("groups after ordering pipeline", () => {
        const grouped = makeEngine()
            .orderBy("name")
            .groupBy("label");
        expect(grouped.get("2024-01-01")?.map(item => item.id)).toEqual([1, 4]);
    });

    it("rejects non-sortable paths at compile time", () => {
        makeEngine()
            // @ts-expect-error - cannot sort by object path
            .orderBy("meta");
    });

    it("rejects non-groupable paths at compile time", () => {
        makeEngine()
            // @ts-expect-error - cannot group by object path
            .groupBy("meta");
    });
});
