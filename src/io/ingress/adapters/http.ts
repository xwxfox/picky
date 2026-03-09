import type { Schema } from "@/io/schema";
import type { IngressCapabilities, IngressHints, AsyncIngressSource } from "@/io/ingress/types";
import { applyJsonArrayPrefilter } from "@/io/ingress/prefilter-runtime";
import { AsyncQueue, isRecord } from "@/io/ingress/utils";
import { startTiming, endTiming } from "@/core/engine/telemetry";

export type ApiPaginationMode = "offset" | "page" | "cursor" | "none";

export type ApiPaginationConfig = {
    mode: ApiPaginationMode;
    cursorParam?: string;
    limitParam?: string;
    nextCursorPath?: string;
    offsetParam?: string;
    pageParam?: string;
    pageSize?: number;
};

export type ApiFilterConfig<T extends Record<string, unknown>> = {
    field: keyof T & string;
    queryParam: string;
};

export type ApiBehavior<T extends Record<string, unknown>> = {
    baseUrl: string;
    body?: Record<string, unknown>;
    dataPath?: string;
    filters?: ReadonlyArray<ApiFilterConfig<T>>;
    headers?: Record<string, string>;
    method?: "GET" | "POST";
    pagination?: ApiPaginationConfig;
    path: string;
    query?: Record<string, string | number | boolean>;
};

export type HttpIngressOptions<T extends Record<string, unknown>> = {
    behavior: ApiBehavior<T>;
    capabilities?: Partial<IngressCapabilities>;
    hints?: IngressHints<T>;
    prefilterMode?: "auto" | "off";
    schema?: Schema<T>;
};

export function httpSource<T extends Record<string, unknown>>(
    options: HttpIngressOptions<T>
): AsyncIngressSource<T> {
    const stream = (streamOptions?: import("@/io/ingress/prefilter").PrefilterStreamOptions) => streamApi<T>(options.behavior, mergePrefilterOptions(streamOptions, options.prefilterMode));
    const materialize = async () => {
        const items: Array<T> = [];
        for await (const item of stream()) {
            items.push(item);
        }
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

async function* streamApi<T extends Record<string, unknown>>(
    behavior: ApiBehavior<T>,
    options?: import("@/io/ingress/prefilter").PrefilterStreamOptions
): AsyncIterable<T> {
    const queue = new AsyncQueue<T>();
    const pending = (async () => {
        const pageSize = behavior.pagination?.pageSize ?? 100;
        let cursor: string | null = null;
        let page = 1;
        let offset = 0;
        const planId = options?.planId ?? "";
        const tp = options?.timingParent ?? null;
        while (true) {
            const fetchTiming = startTiming("ingress", "ingress.http.fetch", planId, tp);
            const response = await fetch(
                buildUrl(behavior, pageSize, offset, cursor, page),
                buildRequestInit(behavior, pageSize, offset, cursor, page)
            );
            if (!response.ok) {
                endTiming(fetchTiming, { skipData: true });
                throw new Error(`HTTP ingress failed (${response.status}).`);
            }
            const rawBody = await response.arrayBuffer();
            endTiming(fetchTiming, { skipData: true });

            const prefiltered = applyJsonArrayPrefilter(new Uint8Array(rawBody), options);
            if (!behavior.dataPath && prefiltered && behavior.pagination?.mode !== "cursor") {
                for (let i = 0; i < prefiltered.length; i++) {
                    const item = prefiltered[i]!;
                    if (!isRecord(item)) {continue;}
                    queue.push(item as T);
                }
                const pagination = behavior.pagination?.mode ?? "none";
                if (pagination === "none") {break;}
                if (pagination === "page") {
                    if (prefiltered.length < pageSize) {break;}
                    page += 1;
                    continue;
                }
                if (pagination === "offset") {
                    if (prefiltered.length < pageSize) {break;}
                    offset += pageSize;
                    continue;
                }
                break;
            }
            const parseTiming = startTiming("ingress", "ingress.json.parse", planId, tp);
            const payload = JSON.parse(new TextDecoder().decode(rawBody)) as unknown;
            endTiming(parseTiming, { skipData: true });
            const items = extractItems<T>(payload, behavior.dataPath);
            for (let i = 0; i < items.length; i++) {queue.push(items[i]!);}

            const pagination = behavior.pagination?.mode ?? "none";
            if (pagination === "none") {break;}
            if (pagination === "page") {
                if (items.length < pageSize) {break;}
                page += 1;
                continue;
            }
            if (pagination === "offset") {
                if (items.length < pageSize) {break;}
                offset += pageSize;
                continue;
            }
            if (pagination === "cursor") {
                const next = resolvePath(payload, behavior.pagination?.nextCursorPath);
                if (typeof next !== "string" || next.length === 0) {break;}
                cursor = next;
                continue;
            }
            break;
        }
        queue.close();
    })();

    for await (const item of queue) {
        yield item;
    }
    await pending;
}

function mergePrefilterOptions(
    options: import("@/io/ingress/prefilter").PrefilterStreamOptions | undefined,
    mode: "auto" | "off" | undefined
): import("@/io/ingress/prefilter").PrefilterStreamOptions | undefined {
    if (!mode || mode === "auto") {return options;}
    return { ...options, prefilterMode: mode };
}

function buildUrl<T extends Record<string, unknown>>(
    behavior: ApiBehavior<T>,
    pageSize: number,
    offset: number,
    cursor: string | null,
    page: number
): string {
    const url = new URL(behavior.path, behavior.baseUrl);
    const query = behavior.query;
    if (query) {
        for (const key of Object.keys(query)) {
            url.searchParams.set(key, String(query[key]));
        }
    }
    const pagination = behavior.pagination;
    if (pagination) {
        if (pagination.limitParam) {url.searchParams.set(pagination.limitParam, String(pageSize));}
        if (pagination.mode === "offset" && pagination.offsetParam) {
            url.searchParams.set(pagination.offsetParam, String(offset));
        }
        if (pagination.mode === "page" && pagination.pageParam) {
            url.searchParams.set(pagination.pageParam, String(page));
        }
        if (pagination.mode === "cursor" && pagination.cursorParam && cursor) {
            url.searchParams.set(pagination.cursorParam, cursor);
        }
    }
    return url.toString();
}

function buildRequestInit<T extends Record<string, unknown>>(
    behavior: ApiBehavior<T>,
    pageSize: number,
    offset: number,
    cursor: string | null,
    page: number
): RequestInit {
    const method = behavior.method ?? "GET";
    if (method === "GET") {
        return { headers: behavior.headers, method };
    }
    const body: Record<string, unknown> = { ...behavior.body };
    const pagination = behavior.pagination;
    if (pagination?.limitParam) {body[pagination.limitParam] = pageSize;}
    if (pagination?.mode === "offset" && pagination.offsetParam) {body[pagination.offsetParam] = offset;}
    if (pagination?.mode === "page" && pagination.pageParam) {body[pagination.pageParam] = page;}
    if (pagination?.mode === "cursor" && pagination.cursorParam && cursor) {body[pagination.cursorParam] = cursor;}
    return {
        body: JSON.stringify(body),
        headers: { "content-type": "application/json", ...behavior.headers },
        method,
    };
}

function extractItems<T extends Record<string, unknown>>(
    payload: unknown,
    path?: string
): Array<T> {
    const target = resolvePath(payload, path);
    if (!Array.isArray(target)) {return [] as Array<T>;}
    const output: Array<T> = [];
    for (let i = 0; i < target.length; i++) {
        const item = target[i];
        if (!isRecord(item)) {continue;}
        output.push(item as T);
    }
    return output;
}

function resolvePath(payload: unknown, path?: string): unknown {
    if (!path || path.length === 0) {return payload;}
    if (!isRecord(payload)) {return undefined;}
    const segments = path.split(".");
    let current: unknown = payload;
    for (let i = 0; i < segments.length; i++) {
        if (!isRecord(current)) {return undefined;}
        const segment = segments[i]!;
        current = current[segment];
    }
    return current;
}
