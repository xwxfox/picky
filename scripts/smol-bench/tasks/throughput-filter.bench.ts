import { Engine, IngressEngine } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";

export const schema: BenchSchema = {
    name: "throughput-filter",
    datasets: [{ key: "large-items", size: 50_000, seed: 2024 }],
};

export const run = async () => {
    const data = await loadDataset("large-items", 50_000);
    const input = IngressEngine.from(data);
    const filter = Engine.from(input);

    const iterations = 200;
    const start = performance.now();
    let total = 0;

    for (let i = 0; i < iterations; i++) {
        total += filter
            .equals("active", true)
            .greaterThan("score", 12)
            .nested("Logs", q => q.arraySome("tags", tag => tag === "red"))
            .out()
            .orderBy("score", { direction: "desc" })
            .limit(25)
            .result().length;
    }

    const end = performance.now();
    const elapsed = end - start;
    const per = elapsed / iterations;
    const throughput = (iterations / elapsed) * 1000;

    console.log(`throughput-filter: iterations=${iterations} total=${total}`);
    console.log(`total=${elapsed.toFixed(2)}ms per=${per.toFixed(3)}ms ops/s=${throughput.toFixed(2)}`);
};

if (import.meta.main) {
    await run();
}
