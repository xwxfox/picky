import { readFile } from "node:fs/promises";
import type { LargeItem } from "./random_data";

export const DATA_DIR = new URL("../../perf/bench-data/", import.meta.url);

export const getDatasetPath = (key: string, size: number) =>
    new URL(`${key}/${size}.json`, DATA_DIR);

export const loadDataset = async (key: string, size: number): Promise<Array<LargeItem>> => {
    const file = getDatasetPath(key, size);
    const content = await readFile(file, "utf8");
    return JSON.parse(content) as Array<LargeItem>;
};
