import { mkdir, rm, writeFile } from "node:fs/promises";
import { glob } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { generateData } from "./random_data";
import { DATA_DIR, getDatasetPath } from "./data";
import type { BenchDataSpec, BenchSchema } from "./schema";

const TASKS_DIR = fileURLToPath(new URL("./tasks/", import.meta.url));

const ensureBenchDataDir = async () => {
    await rm(DATA_DIR, { force: true, recursive: true });
    await mkdir(DATA_DIR, { recursive: true });
};

const loadSchemas = async (): Promise<Array<BenchSchema>> => {
    const schemas: Array<BenchSchema> = [];
    for await (const path of glob(join(TASKS_DIR, "*.bench.ts"))) {
        const mod = await import(pathToFileURL(path).href);
        if (mod && mod.schema) {
            schemas.push(mod.schema as BenchSchema);
        }
    }
    return schemas;
};

const mergeDatasets = (schemas: ReadonlyArray<BenchSchema>): Array<BenchDataSpec> => {
    const merged = new Map<string, BenchDataSpec>();
    for (const schema of schemas) {
        for (const dataset of schema.datasets) {
            const key = `${dataset.key}:${dataset.size}:${dataset.seed}`;
            if (!merged.has(key)) {
                merged.set(key, dataset);
            }
        }
    }
    return [...merged.values()];
};

const writeDataset = async (spec: BenchDataSpec) => {
    const target = getDatasetPath(spec.key, spec.size);
    await mkdir(new URL("./", target), { recursive: true });
    const data = generateData(spec.size, spec.seed);
    await writeFile(target, JSON.stringify(data));
    return target;
};

const main = async () => {
    await ensureBenchDataDir();
    const schemas = await loadSchemas();
    const datasets = mergeDatasets(schemas);
    if (datasets.length === 0) {
        console.log("No benchmark schemas found. Skipping data generation.");
        return;
    }
    for (const dataset of datasets) {
        await writeDataset(dataset);
    }
};

await main();
