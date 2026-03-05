import type { Schema } from "@/io/schema";
import type { IngressCapabilities, IngressHints, AsyncIngressSource } from "@/io/ingress/types";
import { AsyncQueue, LineDecoder, isRecord } from "@/io/ingress/utils";

export type JsonFileFormat = "json" | "ndjson";

export type FileIngressOptions<T extends Record<string, unknown>> = {
    capabilities?: Partial<IngressCapabilities>;
    format?: JsonFileFormat;
    hints?: IngressHints<T>;
    schema?: Schema<T>;
};

export function fileSource<T extends Record<string, unknown>>(
    path: string,
    options?: FileIngressOptions<T>
): AsyncIngressSource<T> {
    const format = options?.format ?? "json";
    const stream = format === "ndjson"
        ? () => streamNdjson<T>(path)
        : () => streamJsonArray<T>(path);
    const materialize = async () => {
        const file = Bun.file(path);
        const text = await file.text();
        const parsed = JSON.parse(text);
        if (!Array.isArray(parsed)) {
            throw new Error("File JSON root must be an array.");
        }
        const output: Array<T> = [];
        for (let i = 0; i < parsed.length; i++) {
            const item = parsed[i];
            if (!isRecord(item)) {continue;}
            output.push(item as T);
        }
        return output as ReadonlyArray<T>;
    };
    return {
        capabilities: options?.capabilities,
        hints: options?.hints,
        materialize,
        mode: "async",
        schema: options?.schema,
        stream,
    };
}

async function* streamJsonArray<T extends Record<string, unknown>>(
    path: string
): AsyncIterable<T> {
    const file = Bun.file(path);
    const text = await file.text();
    const parsed = JSON.parse(text);
    if (!Array.isArray(parsed)) {
        throw new Error("File JSON root must be an array.");
    }
    for (let i = 0; i < parsed.length; i++) {
        const item = parsed[i];
        if (!isRecord(item)) {continue;}
        yield item as T;
    }
}

async function* streamNdjson<T extends Record<string, unknown>>(
    path: string
): AsyncIterable<T> {
    const file = Bun.file(path);
    const stream = file.stream();
    const reader = stream.getReader();
    const decoder = new LineDecoder();
    const queue = new AsyncQueue<T>();
    const pending = (async () => {
        while (true) {
            const result = await reader.read();
            if (result.done) {break;}
            const chunk = result.value;
            if (!(chunk instanceof Uint8Array)) {
                const bytes = new Uint8Array(chunk as ArrayBuffer);
                decoder.push(bytes, line => pushJsonLine(queue, line));
                continue;
            }
            decoder.push(chunk, line => pushJsonLine(queue, line));
        }
        decoder.flush(line => pushJsonLine(queue, line));
        queue.close();
    })();

    for await (const item of queue) {
        yield item;
    }
    await pending;
}

function pushJsonLine<T extends Record<string, unknown>>(queue: AsyncQueue<T>, line: string): void {
    if (line.length === 0) {return;}
    const parsed = JSON.parse(line);
    if (!isRecord(parsed)) {return;}
    queue.push(parsed as T);
}
