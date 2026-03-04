export type BenchDataSpec = {
    key: string;
    seed: number;
    size: number;
};

export type BenchSchema = {
    datasets: ReadonlyArray<BenchDataSpec>;
    name: string;
};
