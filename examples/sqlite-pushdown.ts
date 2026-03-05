import { Database } from "bun:sqlite";
import { Engine, IngressEngine, Schema, sqliteTableSource } from "../src";

type Row = {
    id: number;
    name: string;
    score: number;
};

const db = new Database(":memory:");
db.exec("CREATE TABLE items (id INTEGER, name TEXT, score INTEGER);");
db.exec("INSERT INTO items VALUES (1, 'alpha', 1), (2, 'beta', 2), (3, 'gamma', 3);");

const source = sqliteTableSource<Row>({
    db,
    schema: Schema.inline<Row>(),
    table: "items",
});

const ingress = IngressEngine.fromSource(source);

const result = await Engine.from(ingress)
    .greaterThanOrEqual("score", 2)
    .out()
    .orderBy("score", { direction: "desc" })
    .limit(1)
    .result();

console.dir(result, { depth: null });
db.close();
