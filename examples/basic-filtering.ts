import { Engine, IngressEngine } from "../src";

type LogEntry = {
    message: string;
    tags: Array<string>;
    type: "error" | "warn" | "info";
    when: string;
};

type Ticket = {
    active: boolean;
    created: Date | string | number;
    flags: Array<string>;
    id: number;
    logs: Array<LogEntry>;
    meta?: {
        source?: string | null;
    };
    owner: {
        name: string;
        team?: string | null;
    };
    score: number;
    title: string;
};

const data: Array<Ticket> = [
    {
        active: true,
        created: "2026-01-02T12:00:00.000Z",
        flags: ["urgent", "vip"],
        id: 101,
        logs: [
            { message: "timeout while syncing", tags: ["net"], type: "error", when: "2026-01-03T09:00:00.000Z" },
            { message: "retry ok", tags: ["net"], type: "info", when: "2026-01-03T09:05:00.000Z" },
        ],
        meta: { source: "api" },
        owner: { name: "Ada", team: "infra" },
        score: 92,
        title: "sync lag in us-east",
    },
    {
        active: true,
        created: new Date("2026-01-04T08:00:00.000Z"),
        flags: [],
        id: 102,
        logs: [
            { message: "user confused", tags: ["ux"], type: "warn", when: "2026-01-04T09:00:00.000Z" },
        ],
        owner: { name: "Bea", team: null },
        score: 61,
        title: "docs mismatch",
    },
    {
        active: false,
        created: 1_765_000_000_000,
        flags: ["vip"],
        id: 103,
        logs: [
            { message: "timeout in worker", tags: ["queue"], type: "error", when: "2026-01-05T10:00:00.000Z" },
        ],
        owner: { name: "Cy" },
        score: 88,
        title: "worker stall",
    },
];

const ingress = IngressEngine.from(data);

const result = Engine.from(ingress)
    .equals("active", true)
    .arraySome("flags", flag => flag === "urgent")
    .nested("logs", q => q.equals("type", "error").contains("message", "timeout", true))
    .dateAfter("created", "2026-01-01")
    .out()
    .orderBy("score", { direction: "desc" })
    .limit(5)
    .result();

console.dir(result, { depth: null });
