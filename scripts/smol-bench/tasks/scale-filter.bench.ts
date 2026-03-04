import { Engine, IngressEngine } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";
import { getRandomNamesArray } from "../random_data";

export const schema: BenchSchema = {
    name: "scale-filter",
    datasets: [
        { key: "large-items", size: 1_000, seed: 2_000 },
        { key: "large-items", size: 12_000, seed: 13_000 },
        { key: "large-items", size: 50_000, seed: 51_000 },
        { key: "large-items", size: 200_000, seed: 201_000 },
    ],
};

const sizes = [1_000, 12_000, 50_000, 200_000];

export const run = async () => {
    const runSize = async (count: number) => {
        const data = await loadDataset("large-items", count);
        const input = IngressEngine.from(data);
        const filter = Engine.from(input);
        const start = performance.now();
        const res = filter
            .in("meta.owner.name", getRandomNamesArray())
            .nested("Logs", p => p.arraySome("tags", tag => tag === "x"))
            .dateBetween("created", "2026-01-01", "2026-02-31")
            .out()
            .orderByDate("created", { direction: "desc" })
            .limit(10)
            .result();
        const end = performance.now();
        return { res: res.length, ms: end - start };
    };

    console.log("scale-filter: start");
    for (const size of sizes) {
        const result = await runSize(size);
        console.log(`size=${size} res=${result.res} time=${result.ms.toFixed(2)}ms`);
    }
};

if (import.meta.main) {
    await run();
}
