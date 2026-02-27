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
});
