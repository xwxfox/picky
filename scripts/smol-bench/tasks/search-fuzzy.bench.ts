import { Engine, IngressEngine } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";

export const schema: BenchSchema = {
    name: "search-fuzzy",
    datasets: [{ key: "large-items", size: 60_000, seed: 909 }],
};

export const run = async () => {
    const data = await loadDataset("large-items", 60_000);
    const input = IngressEngine.from(data);

    const filter = Engine.from(input)
        .configureFuzzy({
            fields: [
                { path: "name", weight: 0.5 },
                { path: "meta.owner.name", weight: 1 },
            ],
        });

    const start = performance.now();
    const res = filter
        .search("al")
        .out()
        .orderByDate("created", { direction: "desc" })
        .limit(20)
        .result();
    const end = performance.now();

    console.log(`search-fuzzy: size=${input.length} res=${res.length} time=${(end - start).toFixed(2)}ms`);
};

if (import.meta.main) {
    await run();
}
