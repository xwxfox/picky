import { Database } from "bun:sqlite";
import { Engine, IngressEngine, Schema, sqliteTableSource } from "../../../src";
import type { BenchSchema } from "../schema";
import { loadDataset } from "../data";

type SqliteRow = {
    created: string | number;
    id: number;
    name: string | null;
    score: number;
};

export const schema: BenchSchema = {
    datasets: [{ key: "large-items", size: 50_000, seed: 5050 }],
    name: "pushdown-sqlite",
};

const toSqliteRow = (item: { created: Date | string; id: number; name: string | null; score: number; }): SqliteRow => {
    const created = typeof item.created === "string"
        ? item.created
        : item.created instanceof Date
            ? item.created.toISOString()
            : item.created;
    return {
        created,
        id: item.id,
        name: item.name,
        score: item.score,
    };
};

export const run = async () => {
    const data = await loadDataset("large-items", 50_000);
    const db = new Database(":memory:");
    db.exec("CREATE TABLE items (id INTEGER, name TEXT, score INTEGER, created TEXT);");
    const insert = db.prepare("INSERT INTO items (id, name, score, created) VALUES (?, ?, ?, ?);");
    const batch = db.transaction((rows: Array<SqliteRow>) => {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i]!;
            insert.run(row.id, row.name, row.score, row.created);
        }
    });
    batch(data.map(item => toSqliteRow(item)));

    const source = sqliteTableSource<SqliteRow>({
        db,
        schema: Schema.inline<SqliteRow>(),
        table: "items",
    });
    const ingress = IngressEngine.fromSource(source);

    const start = performance.now();
    const result = await Engine.from(ingress)
        .greaterThanOrEqual("score", 10)
        .out()
        .orderBy("score", { direction: "desc" })
        .limit(100)
        .result();
    const end = performance.now();

    console.log(`pushdown-sqlite: size=${data.length} res=${result.length} time=${(end - start).toFixed(2)}ms`);
    db.close();
};

if (import.meta.main) {
    await run();
}
