import { Engine, IngressEngine } from "../../../src";
import type { AsyncIngressSource } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";
import type { LargeItem } from "../random_data";

export const schema: BenchSchema = {
    datasets: [{ key: "large-items", size: 60_000, seed: 6060 }],
    name: "async-paginate-order",
};

export const run = async () => {
    const data = await loadDataset("large-items", 60_000);
    const source: AsyncIngressSource<LargeItem> = {
        hints: { estimatedCount: data.length },
        mode: "async",
        stream: async function* () {
            for (let i = 0; i < data.length; i++) {yield data[i]!;}
        },
    };
    const ingress = IngressEngine.fromSource(source);

    const filter = Engine.from(ingress)
        .greaterThan("score", 8)
        .out()
        .orderByDate("created", { direction: "desc" });

    const start = performance.now();
    const cursor = await filter.paginate({ pageSize: 40, total: "lazy" });
    let pages = 0;
    let total = 0;
    for (let i = 0; i < 25; i++) {
        total += cursor.data.length;
        cursor.next();
        pages++;
    }
    const end = performance.now();

    console.log(`async-paginate-order: pages=${pages} total=${total} time=${(end - start).toFixed(2)}ms`);
};

if (import.meta.main) {
    await run();
}
