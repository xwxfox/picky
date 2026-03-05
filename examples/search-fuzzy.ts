import { Engine, IngressEngine } from "../src";

type Doc = {
    body: string;
    created: string;
    id: number;
    owner: {
        name: string;
    };
    title: string;
};

const data: Array<Doc> = [
    {
        body: "timeout after 3 retries, tcp reset",
        created: "2026-02-01T10:00:00.000Z",
        id: 1,
        owner: { name: "Rhea" },
        title: "queue worker timeout",
    },
    {
        body: "billing webhook signature mismatch",
        created: "2026-02-02T10:00:00.000Z",
        id: 2,
        owner: { name: "Jay" },
        title: "stripe retry storm",
    },
    {
        body: "client reported lag, likely dns",
        created: "2026-02-03T10:00:00.000Z",
        id: 3,
        owner: { name: "Rhea" },
        title: "regional slowdown",
    },
];

const ingress = IngressEngine.from(data);

const fuzzy = Engine.from(ingress)
    .configureFuzzy({
        fields: [
            { path: "title", weight: 2 },
            { path: "body" },
            { path: "owner.name" },
        ],
        minScore: 8,
        order: "scoreThenOrder",
        requireAll: false,
    })
    .out()
    .search({ fuzzy: { minScore: 12, query: "timeout" } })
    .resultWithMetadata();

console.dir(fuzzy, { depth: null });
