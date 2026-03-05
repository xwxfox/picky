import type { LargeItem } from "./random_data";

export const DATA_DIR = new URL("../../perf/bench-data/", import.meta.url);

export const getDatasetPath = (key: string, size: number) =>
    new URL(`${key}/${size}.json`, DATA_DIR);

export const loadDataset = async (key: string, size: number): Promise<Array<LargeItem>> => {
    const file = getDatasetPath(key, size);
    return await Bun.file(file).json() as Array<LargeItem>;
};
