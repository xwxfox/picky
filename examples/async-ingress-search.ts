import { Engine, IngressEngine } from "../src";
import type { AsyncIngressSource } from "../src";

type RecordItem = {
    id: number;
    name: string;
    score: number;
};

const data: Array<RecordItem> = [
    { id: 1, name: "alpha", score: 10 },
    { id: 2, name: "alpine", score: 8 },
    { id: 3, name: "beta", score: 5 },
];

const source: AsyncIngressSource<RecordItem> = {
    capabilities: { search: true },
    hints: { estimatedCount: data.length },
    mode: "async",
    stream: async function* () {
        for (let i = 0; i < data.length; i++) {yield data[i]!;}
    },
};

const ingress = IngressEngine.fromSource(source);

const result = await Engine.from(ingress)
    .configureFuzzy({ fields: [{ path: "name" }], order: "score" })
    .search("al")
    .out()
    .limit(2)
    .result();

console.dir(result, { depth: null });
