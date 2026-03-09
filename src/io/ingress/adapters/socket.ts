import type { Socket } from "bun";
import type { Schema } from "@/io/schema";
import type { IngressCapabilities, IngressHints, AsyncIngressSource } from "@/io/ingress/types";
import type { PrefilterStreamOptions } from "@/io/ingress/prefilter";
import { applyNdjsonPrefilter } from "@/io/ingress/prefilter-runtime";
import { AsyncQueue, LineDecoder, isRecord } from "@/io/ingress/utils";

export type SocketIngressOptions<T extends Record<string, unknown>> = {
    capabilities?: Partial<IngressCapabilities>;
    hints?: IngressHints<T>;
    prefilterMode?: "auto" | "off";
    schema?: Schema<T>;
    socket: Promise<Socket> | Socket;
};

export function socketSource<T extends Record<string, unknown>>(
    options: SocketIngressOptions<T>
): AsyncIngressSource<T> {
    const stream = (streamOptions?: PrefilterStreamOptions) => streamSocket<T>(options, mergePrefilterOptions(streamOptions, options.prefilterMode));
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

async function* streamSocket<T extends Record<string, unknown>>(
    options: SocketIngressOptions<T>,
    streamOptions?: PrefilterStreamOptions
): AsyncIterable<T> {
    const socket = await options.socket;
    const queue = new AsyncQueue<T>();
    const decoder = new LineDecoder();
    const handler = socket.data;
    if (!handler || typeof handler !== "object") {
        return;
    }
    const typed = handler as {
        close?: (socket: Socket, error?: Error) => void | Promise<void>;
        data?: (socket: Socket, data: ArrayBuffer | ArrayBufferView) => void | Promise<void>;
        end?: (socket: Socket) => void | Promise<void>;
        error?: (socket: Socket, error?: Error) => void | Promise<void>;
    };

    const wrapClose = (
        original?: (socket: Socket, error?: Error) => void | Promise<void>
    ) => (sock: Socket, error?: Error) => {
        decoder.flush(line => pushJsonLine(queue, line, streamOptions));
        queue.close();
        if (original) {return original(sock, error);}
    };

    if (typeof typed.close === "function") {typed.close = wrapClose(typed.close);}
    if (typeof typed.end === "function") {typed.end = wrapClose(typed.end);}
    if (typeof typed.error === "function") {
        const originalError = typed.error;
        typed.error = (sock, err) => wrapClose(originalError)(sock, err);
    }
    if (typeof typed.data === "function") {
        const originalData = typed.data;
        typed.data = (_socket, data) => {
            const chunk = normalizeChunk(data);
            if (chunk) {decoder.push(chunk, line => pushJsonLine(queue, line, streamOptions));}
            return originalData(_socket, data);
        };
    }

    for await (const item of queue) {
        yield item;
    }
}

function normalizeChunk(data: ArrayBuffer | ArrayBufferView): Uint8Array | null {
    if (data instanceof ArrayBuffer) {return new Uint8Array(data);}
    return new Uint8Array(data.buffer);
}

function pushJsonLine<T extends Record<string, unknown>>(
    queue: AsyncQueue<T>,
    line: string,
    options?: PrefilterStreamOptions
): void {
    if (line.length === 0) {return;}
    if (!applyNdjsonPrefilter(line, options)) {return;}
    const parsed = JSON.parse(line);
    if (!isRecord(parsed)) {return;}
    queue.push(parsed as T);
}

function mergePrefilterOptions(
    options: PrefilterStreamOptions | undefined,
    mode: "auto" | "off" | undefined
): PrefilterStreamOptions | undefined {
    if (!mode || mode === "auto") {return options;}
    return { ...options, prefilterMode: mode };
}
