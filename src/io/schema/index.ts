export type SchemaSource = "inline" | "inferred" | "explicit";

export type Schema<T> = {
    sample?: T;
    source: SchemaSource;
};

export const Schema = {
    infer<T>(sample: T): Schema<T> {
        return { sample, source: "inferred" } as Schema<T>;
    },
    inline<T>(): Schema<T> {
        return { source: "inline" } as Schema<T>;
    },
};
