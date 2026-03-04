import { Engine, IngressEngine } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";

export const schema: BenchSchema = {
    name: "pagination-cursor",
    datasets: [{ key: "large-items", size: 80_000, seed: 404 }],
};

export const run = async () => {
    const data = await loadDataset("large-items", 80_000);
    const input = IngressEngine.from(data);
    const filter = Engine.from(input);

    const start = performance.now();
    const cursor = filter
        .greaterThan("score", 10)
        .out()
        .orderByDate("created", { direction: "desc" })
        .paginate({ pageSize: 50, total: "lazy" });

    let pages = 0;
    let total = 0;
    for (let i = 0; i < 30; i++) {
        total += cursor.data.length;
        cursor.next();
        pages++;
    }

    const end = performance.now();
    console.log(`pagination-cursor: pages=${pages} total=${total} time=${(end - start).toFixed(2)}ms`);
};

if (import.meta.main) {
    await run();
}
