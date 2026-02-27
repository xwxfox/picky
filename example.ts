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
        label: "2026-01-01",
        active: true,
        score: 10,
        created: new Date("2026-01-01T00:00:00.000Z"),
        note: null,
        meta: {
            owner: {
                name: "Alice",
                nickname: null,
            },
        },
        HandledBy: {
            SalesRep: "OWO",
        },
        Logs: [
            {
                type: "CREDIT_MAX_EXCEEDED",
                message: "over",
                tags: ["x"],
                when: "2026-01-03T00:00:00.000Z",
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
        created: "2026-01-02T00:00:00.000Z",
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
                when: new Date("2026-01-04T00:00:00.000Z"),
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
            SalesRep: "PAW",
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
        label: "2026-01-01",
        active: true,
        score: 10,
        created: "2026-01-01T00:00:00.000Z",
        note: undefined,
        meta: {
            owner: {
                name: "Dee",
                nickname: undefined,
            },
        },
        HandledBy: {
            SalesRep: "OWO",
        },
        Logs: [
            {
                type: "CREDIT_MAX_EXCEEDED",
                message: "dup",
                tags: ["y"],
                when: "2026-01-03T00:00:00.000Z",
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
import { FilterEngine } from ".";



const filter = FilterEngine.from(data);
const res = filter.in("HandledBy.SalesRep", ["PAW", "OWO"])
    .nested("Logs", p => 
        p.arraySome("tags", tag => tag === "x")
    )
    .dateBetween("created", "2026-01-01", "2026-02-31")
    .result();

console.dir(res, { depth: null });