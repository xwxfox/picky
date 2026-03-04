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
        created: new Date("2026-01-01T00:00:00.000Z"),
        flags: ["red", "blue"],
        HandledBy: {
            SalesRep: "OWO",
        },
        id: 1,
        label: "2026-01-01",
        Logs: [
            {
                message: "over",
                tags: ["x"],
                type: "CREDIT_MAX_EXCEEDED",
                when: "2026-01-03T00:00:00.000Z",
            },
                        {
                message: "over",
                tags: ["x"],
                type: "OTHER",
                when: "2026-01-03T00:00:00.000Z",
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
        created: "2026-01-02T00:00:00.000Z",
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
                when: new Date("2026-01-04T00:00:00.000Z"),
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
            SalesRep: "PAW",
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
        created: "2026-01-01T00:00:00.000Z",
        flags: ["red"],
        HandledBy: {
            SalesRep: "OWO",
        },
        id: 4,
        label: "2026-01-01",
        Logs: [
            {
                message: "dup",
                tags: ["y"],
                type: "CREDIT_MAX_EXCEEDED",
                when: "2026-01-03T00:00:00.000Z",
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
import { Engine, IngressEngine } from "../src";

const input = IngressEngine.from(data)

const filter = Engine.from(input);
const res = filter.in("HandledBy.SalesRep", ["PAW", "OWO"])
    .nested("Logs", p =>
        p.arraySome("tags", tag => tag === "x")
    )
    .configureFuzzy({
        fields: [
            {path: "label"}
        ]
    })
    .search("rele")
    .dateBetween("created", "2026-01-01", "2026-02-31")
    .out()
    .orderByDate("created", { direction: "desc" })
    .limit(5)

    .result();

console.dir(res, { depth: null });
const search = filter
    .configureFuzzy({
        fields: [
            {path: "label"}
        ]
    })
    .out()
    .orderByDate("created", { direction: "desc" })
    .limit(5)
    .search("r")
    .result();

console.dir(search, { depth: null });
const tagging = filter
    .configureTagger({
        rules: [
            {
                equals: "OTHER",
                field: "Logs.type",
                tag: "logs_other"
            }
        ],
        tags: ["logs_other"]
    })
    .out()
    .orderByDate("created", { direction: "desc" })
    .limit(5)
    .tags({
        notAny: ["logs_other"]
    })
    .result();

console.dir(tagging, { depth: null });

const grouped = filter
    .equals("active", true)
    .out()
    .groupBy("HandledBy.SalesRep");

console.dir(grouped, { depth: null });

const cursor = filter
    .out()
    .orderByDate("created", { direction: "desc" })
    .paginate({ pageSize: 2, total: "lazy" });

console.dir(cursor.data, { depth: null });
console.dir(cursor.next().data, { depth: null });
