import { Engine, IngressEngine } from "../../../src";
import type { AsyncIngressSource } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";
import type { LargeItem } from "../random_data";

export const schema: BenchSchema = {
    datasets: [{ key: "large-items", size: 80_000, seed: 9090 }],
    name: "async-search-score",
};

export const run = async () => {
    const data = await loadDataset("large-items", 80_000);
    const source: AsyncIngressSource<LargeItem> = {
        capabilities: { search: true },
        hints: { estimatedCount: data.length },
        mode: "async",
        stream: async function* () {
            for (let i = 0; i < data.length; i++) {yield data[i]!;}
        },
    };
    const ingress = IngressEngine.fromSource(source);

    const filter = Engine.from(ingress).configureFuzzy({
        fields: [
            { path: "name", weight: 0.8 },
            { path: "meta.owner.name", weight: 1 },
        ],
        order: "score",
    });

    const start = performance.now();
    const res = await filter
        .search("al")
        .out()
        .limit(50)
        .result();
    const end = performance.now();

    console.log(`async-search-score: size=${data.length} res=${res.length} time=${(end - start).toFixed(2)}ms`);
};

if (import.meta.main) {
    await run();
}
