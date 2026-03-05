import { Engine, IngressEngine } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";

export const schema: BenchSchema = {
    datasets: [
        { key: "large-items", size: 10_000, seed: 9100 },
        { key: "large-items", size: 50_000, seed: 9500 },
        { key: "large-items", size: 200_000, seed: 11_000 },
    ],
    name: "memory-scale",
};

const sizes = [10_000, 50_000, 200_000];

const formatBytes = (value: number) => `${(value / (1024 * 1024)).toFixed(2)}MB`;

export const run = async () => {
    console.log("memory-scale: start");
    for (const size of sizes) {
        const data = await loadDataset("large-items", size);
        const input = IngressEngine.from(data);
        const filter = Engine.from(input);

        if (globalThis.gc) {globalThis.gc();}
        const before = process.memoryUsage().heapUsed;

        const res = filter
            .greaterThan("score", 5)
            .nested("Logs", q => q.arraySome("tags", tag => tag === "amber"))
            .out()
            .orderByDate("created", { direction: "desc" })
            .limit(100)
            .result();

        if (globalThis.gc) {globalThis.gc();}
        const after = process.memoryUsage().heapUsed;

        console.log(`size=${size} res=${res.length} heap=${formatBytes(after)} delta=${formatBytes(after - before)}`);
    }
    console.log("memory-scale: done (run with --smol --gc if supported)");
};

if (import.meta.main) {
    await run();
}
