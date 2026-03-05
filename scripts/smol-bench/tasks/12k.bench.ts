
import { Engine, IngressEngine } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";
import { getRandomNamesArray } from "../random_data";

export const schema: BenchSchema = {
    datasets: [{ key: "large-items", size: 12_000, seed: 1337 }],
    name: "12k",
};

export const run = async () => {
    const data = await loadDataset("large-items", 12_000);

    const start = performance.now();
    const input = IngressEngine.from(data);
    const filter = Engine.from(input);
    const res = filter.in("meta.owner.name", getRandomNamesArray())
        .nested("Logs", p =>
            p.arraySome("tags", tag => tag === "x")
        )
        .dateBetween("created", "2026-01-01", "2026-02-31")
        .out()
        .orderByDate("created", { direction: "desc" })
        .limit(5)
        .result();
    const grouped = filter
        .equals("active", true)
        .out()
        .groupBy("meta.owner.name");
    const cursor = filter
        .out()
        .orderByDate("created", { direction: "desc" })
        .paginate({ pageSize: 2, total: "lazy" });
    const end = performance.now();
    console.log(`Query took ${end - start}ms - sizes: input=${input.length}, res=${res.length}, grouped=${grouped.size}, cursor=${cursor.data.length}`);
};

if (import.meta.main) {
    await run();
}
