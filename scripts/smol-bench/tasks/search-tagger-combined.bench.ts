import { Engine, IngressEngine } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";
import { tags } from "../random_data";

export const schema: BenchSchema = {
    name: "search-tagger-combined",
    datasets: [{ key: "large-items", size: 80_000, seed: 808 }],
};

export const run = async () => {
    const data = await loadDataset("large-items", 80_000);
    const input = IngressEngine.from(data);

    const filter = Engine.from(input)
        .configureFuzzy({
            fields: [
                { path: "name", weight: 0.7 },
                { path: "meta.owner.name", weight: 1 },
            ],
        })
        .configureTagger({
            tags,
            rules: [
                { tag: "red", field: "flags", equals: "red" },
                { tag: "blue", field: "flags", equals: "blue" },
                { tag: "green", field: "flags", equals: "green" },
                { tag: "amber", field: "flags", equals: "amber" },
            ],
        });

    const start = performance.now();
    const res = filter
        .search({
            fuzzy: "al",
            tags: { hasAny: ["red", "blue"] },
        })
        .out()
        .orderByDate("created", { direction: "desc" })
        .limit(20)
        .result();
    const end = performance.now();

    console.log(`search-tagger-combined: size=${input.length} res=${res.length} time=${(end - start).toFixed(2)}ms`);
};

if (import.meta.main) {
    await run();
}
