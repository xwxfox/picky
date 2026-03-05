import { Engine, IngressEngine } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";
import { tags } from "../random_data";

export const schema: BenchSchema = {
    datasets: [{ key: "large-items", size: 50_000, seed: 505 }],
    name: "tagger-filter",
};

export const run = async () => {
    const data = await loadDataset("large-items", 50_000);
    const input = IngressEngine.from(data);

    const filter = Engine.from(input).configureTagger({
        rules: [
            { tag: "red", field: "flags", equals: "red" },
            { tag: "blue", field: "flags", equals: "blue" },
            { tag: "green", field: "flags", equals: "green" },
            { tag: "amber", field: "flags", equals: "amber" },
        ],
        tags,
    });

    const start = performance.now();
    const res = filter
        .tags({ hasAny: ["red", "blue"] })
        .out()
        .limit(50)
        .result();
    const end = performance.now();

    console.log(`tagger-filter: size=${input.length} res=${res.length} time=${(end - start).toFixed(2)}ms`);
};

if (import.meta.main) {
    await run();
}
