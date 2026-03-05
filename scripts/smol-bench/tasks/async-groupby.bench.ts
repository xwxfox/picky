import { Engine, IngressEngine } from "../../../src";
import type { AsyncIngressSource } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";
import type { LargeItem } from "../random_data";

export const schema: BenchSchema = {
    datasets: [{ key: "large-items", size: 70_000, seed: 7070 }],
    name: "async-groupby",
};

export const run = async () => {
    const data = await loadDataset("large-items", 70_000);
    const source: AsyncIngressSource<LargeItem> = {
        hints: { estimatedCount: data.length },
        mode: "async",
        stream: async function* () {
            for (let i = 0; i < data.length; i++) {yield data[i]!;}
        },
    };
    const ingress = IngressEngine.fromSource(source);

    const start = performance.now();
    const grouped = await Engine.from(ingress)
        .greaterThan("score", 5)
        .out()
        .groupBy("meta.owner.name");
    const end = performance.now();

    console.log(`async-groupby: size=${data.length} groups=${grouped.size} time=${(end - start).toFixed(2)}ms`);
};

if (import.meta.main) {
    await run();
}
