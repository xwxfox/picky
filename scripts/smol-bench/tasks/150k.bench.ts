
import { Engine, IngressEngine } from "../../../src";
import { loadDataset } from "../data";
import type { BenchSchema } from "../schema";
import { getRandomNamesArray } from "../random_data";

export const schema: BenchSchema = {
    datasets: [{ key: "large-items", size: 150_000, seed: 6969 }],
    name: "150k",
};

export const run = async () => {
    const data = await loadDataset("large-items", 150_000);
    const start = performance.now();
    const input = IngressEngine.from(data);
    const filter = Engine.from(input);
    const res = filter.in("meta.owner.name", getRandomNamesArray())
        /*
            .configureTagger({
                tags: ["falgs:green"],
                rules: [
                    {
                        field: "flags",
                        in: ["green"],
                        tag: "falgs:green"
                    }
                ]
            })
                */ // <-- has to analyze item.flags (string[]) on every single item in input[] (150k items)
        .greaterThan("score", 9)
        .valueNotNull("name")
        .dateBetween("created", "2026-01-01", "2026-02-31")
        .configureTagger({
            rules: [
                {
                    field: "flags",
                    in: ["green"],
                    tag: "falgs:green"
                }
            ],
            tags: ["falgs:green"]
        }) // <-- placing it here means that it has to analyze and tag wayyyy less items, making it 20.93% faster in this bench
        .out()
        .orderByDate("created", { direction: "desc" })
        .result();
    const end = performance.now();
    console.log(`Query took ${end - start}ms - sizes: input=${input.length}, res=${res.length}`);
};
/*
res with configureTagger at the start
Timing benchmark smol-bench 150k...
getRandomNamesArray called.
got random, i 0 / 3
got random, i 1 / 3
got random, i 2 / 3
Query took 41.35244500000002ms - sizes: input=150000, res=66381
real 0.21
user 0.33
sys 0.06

Profiling benchmark smol-bench 150k...
getRandomNamesArray called.
got random, i 0 / 4
got random, i 1 / 4
got random, i 2 / 4
got random, i 3 / 4
Query took 43.298394ms - sizes: input=150000, res=66511
Heap profile written to: /home/jay/projects/pickie/perf/smol-bench-150k-heap-prof.md
Saved profiles:
 - /home/jay/projects/pickie/perf/smol-bench-150k-cpu-prof.md
 - /home/jay/projects/pickie/perf/smol-bench-150k-heap-prof.md


 res with configureTagger at the end of the chain
 Benchmark: smol-bench / 150k
----------------------------
Timing benchmark smol-bench 150k...
getRandomNamesArray called.
got random, i 0 / 2
got random, i 1 / 2
Query took 38.776977999999986ms - sizes: input=150000, res=44333
real 0.22
user 0.37
sys 0.09

Profiling benchmark smol-bench 150k...
getRandomNamesArray called.
got random, i 0 / 2
got random, i 1 / 2
Query took 34.960666ms - sizes: input=150000, res=44333
Heap profile written to: /home/jay/projects/pickie/perf/smol-bench-150k-heap-prof.md
Saved profiles:
 - /home/jay/projects/pickie/perf/smol-bench-150k-cpu-prof.md
*/

if (import.meta.main) {
    await run();
}
