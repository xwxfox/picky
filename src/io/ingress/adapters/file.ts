import type { Schema } from "@/io/schema";
import type { IngressCapabilities, IngressHints, AsyncIngressSource } from "@/io/ingress/types";
import type { PrefilterStreamOptions } from "@/io/ingress/prefilter";
import { batchPrefilterNdjson, applyJsonArrayPrefilter } from "@/io/ingress/prefilter-runtime";
import { isRecord } from "@/io/ingress/utils";
import { startTiming, endTiming } from "@/core/engine/telemetry";

export type JsonFileFormat = "json" | "ndjson";

export type FileIngressOptions<T extends Record<string, unknown>> = {
    capabilities?: Partial<IngressCapabilities>;
    format?: JsonFileFormat;
    hints?: IngressHints<T>;
    prefilterMode?: "auto" | "off";
    schema?: Schema<T>;
};

export function fileSource<T extends Record<string, unknown>>(
    path: string,
    options?: FileIngressOptions<T>
): AsyncIngressSource<T> {
    const format = options?.format ?? "json";
    const stream = format === "ndjson"
        ? (streamOptions?: PrefilterStreamOptions) => streamNdjson<T>(path, mergePrefilterOptions(streamOptions, options?.prefilterMode))
        : (streamOptions?: PrefilterStreamOptions) => streamJsonArray<T>(path, mergePrefilterOptions(streamOptions, options?.prefilterMode));
    const materialize = format === "ndjson"
        ? (streamOptions?: PrefilterStreamOptions) => materializeNdjson<T>(path, mergePrefilterOptions(streamOptions, options?.prefilterMode))
        : (streamOptions?: PrefilterStreamOptions) => materializeJsonArray<T>(path, mergePrefilterOptions(streamOptions, options?.prefilterMode));
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
    path: string,
    options?: PrefilterStreamOptions
): AsyncIterable<T> {
    const planId = options?.planId ?? "";
    const tp = options?.timingParent ?? null;

    const readTiming = startTiming("ingress", "ingress.file.read", planId, tp);
    const bytes = new Uint8Array(await Bun.file(path).arrayBuffer());
    endTiming(readTiming, { skipData: true });

    const matched = applyJsonArrayPrefilter(bytes, options);

    if (matched !== null) {
        for (let i = 0; i < matched.length; i++) {
            yield matched[i]! as T;
        }
        return;
    }
    // Fallback: no prefilter or error - full parse
    const parseTiming = startTiming("ingress", "ingress.json.parse", planId, tp);
    const parsed = JSON.parse(bytesToString(bytes));
    endTiming(parseTiming, { skipData: true });
    if (!Array.isArray(parsed)) {
        throw new Error("File JSON root must be an array.");
    }
    for (let i = 0; i < parsed.length; i++) {
        const item = parsed[i];
        if (!isRecord(item)) { continue; }
        yield item as T;
    }
}

async function materializeJsonArray<T extends Record<string, unknown>>(
    path: string,
    options?: PrefilterStreamOptions
): Promise<ReadonlyArray<T>> {
    const planId = options?.planId ?? "";
    const tp = options?.timingParent ?? null;

    const readTiming = startTiming("ingress", "ingress.file.read", planId, tp);
    const bytes = new Uint8Array(await Bun.file(path).arrayBuffer());
    endTiming(readTiming, { skipData: true });

    const matched = applyJsonArrayPrefilter(bytes, options);

    if (matched !== null) {
        return matched as Array<T>;
    }
    // Fallback: bulk parse entire array
    const parseTiming = startTiming("ingress", "ingress.json.parse", planId, tp);
    const parsed = JSON.parse(bytesToString(bytes));
    endTiming(parseTiming, { skipData: true });
    if (!Array.isArray(parsed)) {
        throw new Error("File JSON root must be an array.");
    }
    return parsed as Array<T>;
}

async function materializeNdjson<T extends Record<string, unknown>>(
    path: string,
    options?: PrefilterStreamOptions
): Promise<ReadonlyArray<T>> {
    const planId = options?.planId ?? "";
    const tp = options?.timingParent ?? null;

    const readTiming = startTiming("ingress", "ingress.file.read", planId, tp);
    const bytes = new Uint8Array(await Bun.file(path).arrayBuffer());
    endTiming(readTiming, { skipData: true });

    const matched = batchPrefilterNdjson(bytes, options);

    if (matched !== null) {
        const parseTiming = startTiming("ingress", "ingress.json.parse", planId, tp);
        const output: Array<T> = new Array(matched.length);
        let count = 0;
        for (let i = 0; i < matched.length; i++) {
            const parsed = JSON.parse(matched[i]!);
            if (isRecord(parsed)) { output[count++] = parsed as T; }
        }
        output.length = count;
        endTiming(parseTiming, { skipData: true });
        return output;
    }
    // Fallback: decode and split by lines
    const parseTiming = startTiming("ingress", "ingress.json.parse", planId, tp);
    const text = bytesToString(bytes);
    const lines = text.split("\n");
    const output: Array<T> = [];
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i]!.trim();
        if (line.length === 0) { continue; }
        const parsed = JSON.parse(line);
        if (isRecord(parsed)) { output.push(parsed as T); }
    }
    endTiming(parseTiming, { skipData: true });
    return output;
}

async function* streamNdjson<T extends Record<string, unknown>>(
    path: string,
    options?: PrefilterStreamOptions
): AsyncIterable<T> {
    const planId = options?.planId ?? "";
    const tp = options?.timingParent ?? null;

    const readTiming = startTiming("ingress", "ingress.file.read", planId, tp);
    const bytes = new Uint8Array(await Bun.file(path).arrayBuffer());
    endTiming(readTiming, { skipData: true });

    // Try batch prefilter path
    const matched = batchPrefilterNdjson(bytes, options);

    if (matched !== null) {
        // Batch prefilter succeeded - parse only matched items
        for (let i = 0; i < matched.length; i++) {
            const parsed = JSON.parse(matched[i]!);
            if (!isRecord(parsed)) { continue; }
            yield parsed as T;
        }
        return;
    }

    // Fallback: no prefilter or error - decode and split by lines
    const text = bytesToString(bytes);
    const lines = text.split("\n");
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i]!.trim();
        if (line.length === 0) { continue; }
        const parsed = JSON.parse(line);
        if (!isRecord(parsed)) { continue; }
        yield parsed as T;
    }
}

function bytesToString(bytes: Uint8Array): string {
    return new TextDecoder().decode(bytes);
}

function mergePrefilterOptions(
    options: PrefilterStreamOptions | undefined,
    mode: "auto" | "off" | undefined
): PrefilterStreamOptions | undefined {
    if (!mode || mode === "auto") { return options; }
    return { ...options, prefilterMode: mode };
}
