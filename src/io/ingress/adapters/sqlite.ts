import type { Database, SQLQueryBindings } from "bun:sqlite";
import type { Schema } from "@/io/schema";
import type { PushdownOrder, PushdownPredicate, PushdownQuery } from "@/io/ingress/adapters/pushdown";
import type { IngressCapabilities, IngressHints, AsyncIngressSource } from "@/io/ingress/types";
import { isRecord } from "@/io/ingress/utils";

export type SqliteTableOptions<T extends Record<string, unknown>> = {
    caseInsensitive?: boolean;
    capabilities?: Partial<IngressCapabilities>;
    columns?: ReadonlyArray<string>;
    db: Database;
    hints?: IngressHints<T>;
    schema: Schema<T>;
    table: string;
};

export type SqliteQueryOptions<T extends Record<string, unknown>> = {
    capabilities?: Partial<IngressCapabilities>;
    db: Database;
    hints?: IngressHints<T>;
    params?: ReadonlyArray<SQLQueryBindings> | SQLQueryBindings;
    query: string;
    schema: Schema<T>;
};

export function sqliteTableSource<T extends Record<string, unknown>>(
    options: SqliteTableOptions<T>
): AsyncIngressSource<T> {
    const stream = (_options?: import("@/io/ingress/prefilter").PrefilterStreamOptions) => streamSqlite<T>({
        db: options.db,
        params: undefined,
        query: buildSelectAll(options.table, options.columns),
    });
    const materialize = async () => {
        const items: Array<T> = [];
        for await (const item of stream()) {items.push(item);}
        return items as ReadonlyArray<T>;
    };
    return {
        capabilities: options.capabilities,
        hints: options.hints,
        materialize,
        mode: "async",
        pushdown: (query) => pushdownSqlite<T>(options, query),
        schema: options.schema,
        stream,
    };
}

export function sqliteQuerySource<T extends Record<string, unknown>>(
    options: SqliteQueryOptions<T>
): AsyncIngressSource<T> {
    const stream = (_options?: import("@/io/ingress/prefilter").PrefilterStreamOptions) => streamSqlite<T>(options);
    const materialize = async () => {
        const items: Array<T> = [];
        for await (const item of stream()) {items.push(item);}
        return items as ReadonlyArray<T>;
    };
    return {
        capabilities: options.capabilities,
        hints: options.hints,
        materialize,
        mode: "async",
        schema: options.schema,
        stream,
    };
}

async function* streamSqlite<T extends Record<string, unknown>>(
    options: { db: Database; params?: ReadonlyArray<SQLQueryBindings> | SQLQueryBindings; query: string; }
): AsyncIterable<T> {
    const stmt = options.db.query<T, SQLQueryBindings | Array<SQLQueryBindings>>(options.query);
    const params = normalizeSqliteParams(options.params);
    const iterator = stmt.iterate(...params);
    for (const row of iterator) {
        if (!isRecord(row)) {continue;}
        yield row as T;
    }
}

function pushdownSqlite<T extends Record<string, unknown>>(
    options: SqliteTableOptions<T>,
    query: PushdownQuery
): AsyncIterable<T> | null {
    const { params, sql } = buildSqliteQuery(options, query);
    return streamSqlite({ db: options.db, params, query: sql });
}

function buildSqliteQuery<T extends Record<string, unknown>>(
    options: SqliteTableOptions<T>,
    query: PushdownQuery
): { params: Array<SQLQueryBindings>; sql: string } {
    const whereParts: Array<string> = [];
    const params: Array<SQLQueryBindings> = [];
    const predicates = query.predicates ?? [];
    for (let i = 0; i < predicates.length; i++) {
        const clause = predicateToSql(predicates[i]!, params, options.caseInsensitive ?? true);
        if (clause.length > 0) {whereParts.push(clause);}
    }
    const orders = query.orders ?? [];
    const orderParts: Array<string> = [];
    for (let i = 0; i < orders.length; i++) {
        orderParts.push(orderToSql(orders[i]!));
    }
    const limit = query.limit ?? null;
    const offset = query.offset ?? 0;
    const fields = query.fields ?? options.columns;

    let sql = `SELECT ${resolveColumns(fields)} FROM ${escapeIdent(options.table)}`;
    if (whereParts.length > 0) {
        sql += ` WHERE ${whereParts.join(" AND ")}`;
    }
    if (orderParts.length > 0) {
        sql += ` ORDER BY ${orderParts.join(", ")}`;
    }
    if (limit !== null) {
        sql += ` LIMIT ?`;
        params.push(limit);
        if (offset > 0) {
            sql += ` OFFSET ?`;
            params.push(offset);
        }
    }

    return { params, sql };
}

function predicateToSql(
    predicate: PushdownPredicate,
    params: Array<SQLQueryBindings>,
    caseInsensitive: boolean
): string {
    const field = escapeIdent(predicate.field);
    const op = predicate.op;
    if (op === "eq") {
        params.push(predicate.value as SQLQueryBindings);
        return caseInsensitive && predicate.ignoreCase ? `${field} = ? COLLATE NOCASE` : `${field} = ?`;
    }
    if (op === "ne") {
        params.push(predicate.value as SQLQueryBindings);
        return caseInsensitive && predicate.ignoreCase ? `${field} != ? COLLATE NOCASE` : `${field} != ?`;
    }
    if (op === "gt" || op === "gte" || op === "lt" || op === "lte") {
        params.push(predicate.value as SQLQueryBindings);
        const suffix = caseInsensitive && predicate.ignoreCase ? " COLLATE NOCASE" : "";
        return `${field} ${compareOpToSql(op)} ?${suffix}`;
    }
    if (op === "between") {
        const range = predicate.value as { max: SQLQueryBindings; min: SQLQueryBindings };
        params.push(range.min);
        params.push(range.max);
        return caseInsensitive && predicate.ignoreCase
            ? `${field} BETWEEN ? AND ? COLLATE NOCASE`
            : `${field} BETWEEN ? AND ?`;
    }
    if (op === "in" || op === "notIn") {
        const list = Array.isArray(predicate.value) ? predicate.value : [];
        if (list.length === 0) {return op === "in" ? "1 = 0" : "1 = 1";}
        const placeholders = new Array<string>(list.length);
        for (let i = 0; i < list.length; i++) {
            params.push(list[i] as SQLQueryBindings);
            placeholders[i] = "?";
        }
        return `${field} ${op === "in" ? "IN" : "NOT IN"} (${placeholders.join(", ")})`;
    }
    if (op === "contains" || op === "startsWith" || op === "endsWith") {
        const raw = String(predicate.value ?? "");
        const pattern = op === "contains"
            ? `%${escapeLike(raw)}%`
            : op === "startsWith"
                ? `${escapeLike(raw)}%`
                : `%${escapeLike(raw)}`;
        params.push(pattern);
        return caseInsensitive ? `${field} LIKE ? COLLATE NOCASE` : `${field} LIKE ?`;
    }
    if (op === "matches") {
        params.push(String(predicate.value ?? ""));
        return `${field} REGEXP ?`;
    }
    if (op === "isNull") {return `${field} IS NULL`;}
    if (op === "notNull") {return `${field} IS NOT NULL`;}
    return "";
}

function orderToSql(order: PushdownOrder): string {
    const dir = order.direction === "desc" ? "DESC" : "ASC";
    const nulls = order.nulls ? ` NULLS ${order.nulls.toUpperCase()}` : "";
    return `${escapeIdent(order.field)} ${dir}${nulls}`;
}

function compareOpToSql(op: "gt" | "gte" | "lt" | "lte"): string {
    if (op === "gt") {return ">";}
    if (op === "gte") {return ">=";}
    if (op === "lt") {return "<";}
    return "<=";
}

function escapeIdent(value: string): string {
    const cleaned = value.replaceAll(/[^a-zA-Z0-9_.]/g, "");
    if (cleaned.length === 0) {throw new Error("Invalid identifier.");}
    return cleaned
        .split(".")
        .map(part => `"${part}"`)
        .join(".");
}

function buildSelectAll(table: string, columns?: ReadonlyArray<string>): string {
    return `SELECT ${resolveColumns(columns)} FROM ${escapeIdent(table)}`;
}

function resolveColumns(columns?: ReadonlyArray<string>): string {
    if (!columns || columns.length === 0) {return "*";}
    const parts = new Array<string>(columns.length);
    for (let i = 0; i < columns.length; i++) {
        parts[i] = escapeIdent(columns[i]!);
    }
    return parts.join(", ");
}

function escapeLike(value: string): string {
    return value.replaceAll(/[%_\\]/g, (match) => `\\${match}`);
}

function normalizeSqliteParams(
    params: SqliteQueryOptions<Record<string, unknown>>["params"]
): Array<SQLQueryBindings> {
    if (!params) {return [] as Array<SQLQueryBindings>;}
    if (Array.isArray(params)) {return params as Array<SQLQueryBindings>;}
    return [params as SQLQueryBindings];
}
