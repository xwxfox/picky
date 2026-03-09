import { ptr } from "bun:ffi";
import type { PrefilterStreamOptions } from "@/io/ingress/prefilter";
import { createPrefilterStats } from "@/io/ingress/prefilter-utils";
import { getPrefilterProgram, runPrefilter } from "@/io/ingress/prefilter-c";
import { startTiming, endTiming } from "@/core/engine/telemetry";

const encoder = new TextEncoder();
const decoder = new TextDecoder();


const INITIAL_OUTPUT_SLOTS = 4096; // 4096 uint32 values = 2048 match slots
const MAX_POOL_SIZE = 4;
const bufferPool: Array<Uint32Array> = [];

function acquireOutputBuffer(minSlots: number): Uint32Array {
    for (let i = bufferPool.length - 1; i >= 0; i--) {
        if (bufferPool[i]!.length >= minSlots) {
            return bufferPool.splice(i, 1)[0]!;
        }
    }
    const size = Math.max(INITIAL_OUTPUT_SLOTS, minSlots);
    return new Uint32Array(size);
}

function releaseOutputBuffer(buf: Uint32Array): void {
    if (bufferPool.length < MAX_POOL_SIZE) {
        bufferPool.push(buf);
    }
}


export function applyNdjsonPrefilter(
    line: string,
    options?: PrefilterStreamOptions
): boolean {
    if (!options?.prefilter || options.prefilterMode === "off") {
        return true;
    }
    const plan = options.prefilter;
    const stats = options.stats;
    stats && (stats.checked += 1);
    const bytes = encoder.encode(line);
    const program = options.prefilterProgram ?? getPrefilterProgram(plan);
    const result = runPrefilter(program, bytes);
    if (result < 0) {
        stats && (stats.unknown += 1);
        stats && (stats.parsed += 1);
        return true;
    }
    if (result === 0) {
        stats && (stats.skipped += 1);
        return false;
    }
    stats && (stats.matched += 1);
    stats && (stats.parsed += 1);
    return true;
}

export function batchPrefilterNdjson(
    bytes: Uint8Array,
    options?: PrefilterStreamOptions
): Array<string> | null {
    if (!options?.prefilter || options.prefilterMode === "off") {
        return null;
    }
    const plan = options.prefilter;
    const stats = options.stats;
    const program = options.prefilterProgram ?? getPrefilterProgram(plan);
    const planId = options.planId ?? "";
    const tp = options.timingParent ?? null;

    // Estimate max possible matches. Each JSON object is at minimum ~10 bytes.
    // Use byteLength/10 as a conservative upper bound to avoid massive over-allocation.
    const estimatedMaxItems = Math.max(256, Math.ceil(bytes.byteLength / 10));
    const neededSlots = estimatedMaxItems * 2 + 2;
    const outputBuf = acquireOutputBuffer(neededSlots);

    try {
        const ffiTiming = startTiming("ingress", "ingress.prefilter.ffi", planId, tp);
        const result = program.fnNdjson(
            ptr(bytes),
            bytes.byteLength,
            ptr(outputBuf),
            outputBuf.length
        );
        endTiming(ffiTiming, { skipData: true });

        if (result < 0) {
            // Overflow or error - fallback to parse-all
            return null;
        }

        const matchCount = result;

        const decodeTiming = startTiming("ingress", "ingress.prefilter.decode", planId, tp);
        const matched: Array<string> = new Array(matchCount);
        for (let i = 0; i < matchCount; i++) {
            const offset = outputBuf[2 + i * 2]!;
            const length = outputBuf[2 + i * 2 + 1]!;
            matched[i] = decoder.decode(bytes.subarray(offset, offset + length));
        }
        endTiming(decodeTiming, { skipData: true });

        if (stats) {
            const totalItems = outputBuf[0]!;
            stats.checked += totalItems;
            stats.matched += matchCount;
            stats.parsed += matchCount;
            stats.skipped += totalItems - matchCount;
        }

        return matched;
    } finally {
        releaseOutputBuffer(outputBuf);
    }
}


export function applyJsonArrayPrefilter(
    bytes: Uint8Array,
    options?: PrefilterStreamOptions
): Array<Record<string, unknown>> | null {
    if (!options?.prefilter || options.prefilterMode === "off") {
        return null;
    }
    const plan = options.prefilter;
    const stats = options.stats;
    const program = options.prefilterProgram ?? getPrefilterProgram(plan);
    const planId = options.planId ?? "";
    const tp = options.timingParent ?? null;

    // Estimate max possible matches. Each JSON object is at minimum ~10 bytes.
    const estimatedMaxItems = Math.max(256, Math.ceil(bytes.byteLength / 10));
    const neededSlots = estimatedMaxItems * 2 + 2;
    const outputBuf = acquireOutputBuffer(neededSlots);

    try {
        const ffiTiming = startTiming("ingress", "ingress.prefilter.ffi", planId, tp);
        const result = program.fnJsonArray(
            ptr(bytes),
            bytes.byteLength,
            ptr(outputBuf),
            outputBuf.length
        );
        endTiming(ffiTiming, { skipData: true });

        if (result < 0) {
            // Parse error or overflow - fallback to full parse
            return null;
        }

        const matchCount = result;

        if (stats) {
            const totalItems = outputBuf[0]!;
            stats.checked += totalItems;
            stats.matched += matchCount;
            stats.parsed += matchCount;
            stats.skipped += totalItems - matchCount;
        }

        if (matchCount === 0) {
            return [];
        }

        // Build a single JSON array from matched byte slices, then bulk parse.
        // This avoids N TextDecoder.decode + N JSON.parse calls.
        const assembleTiming = startTiming("ingress", "ingress.prefilter.assemble", planId, tp);
        let totalBytes = 2; // for '[' and ']'
        for (let i = 0; i < matchCount; i++) {
            totalBytes += outputBuf[2 + i * 2 + 1]!;
        }
        totalBytes += matchCount - 1; // commas between items

        const assembled = new Uint8Array(totalBytes);
        assembled[0] = 0x5b; // '['
        let pos = 1;
        for (let i = 0; i < matchCount; i++) {
            if (i > 0) { assembled[pos++] = 0x2c; } // ','
            const offset = outputBuf[2 + i * 2]!;
            const length = outputBuf[2 + i * 2 + 1]!;
            assembled.set(bytes.subarray(offset, offset + length), pos);
            pos += length;
        }
        assembled[pos] = 0x5d; // ']'
        endTiming(assembleTiming, { skipData: true });

        const parseTiming = startTiming("ingress", "ingress.prefilter.parse", planId, tp);
        const parsed = JSON.parse(decoder.decode(assembled));
        endTiming(parseTiming, { skipData: true });

        return parsed;
    } finally {
        releaseOutputBuffer(outputBuf);
    }
}

export const initPrefilterStats = createPrefilterStats;
