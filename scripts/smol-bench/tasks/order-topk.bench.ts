import { Engine, IngressEngine } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";

export const schema: BenchSchema = {
    datasets: [{ key: "large-items", size: 120_000, seed: 777 }],
    name: "order-topk",
};

export const run = async () => {
    const data = await loadDataset("large-items", 120_000);
    const input = IngressEngine.from(data);
    const filter = Engine.from(input);

    const start = performance.now();
    const res = filter
        .greaterThan("score", 10)
        .out()
        .orderBy("score", { direction: "desc" })
        .limit(50)
        .result();
    const end = performance.now();

    console.log(`order-topk: size=${input.length} res=${res.length} time=${(end - start).toFixed(2)}ms`);
};

if (import.meta.main) {
    await run();
}
