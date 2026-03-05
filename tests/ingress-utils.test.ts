import { describe, it, expect } from "bun:test";
import { AsyncQueue, LineDecoder, isRecord } from "@/io/ingress/utils";

describe("Ingress utils", () => {
    it("isRecord accepts objects and rejects arrays/null", () => {
        expect(isRecord({})).toEqual(true);
        expect(isRecord({ a: 1 })).toEqual(true);
        expect(isRecord(null)).toEqual(false);
        expect(isRecord([])).toEqual(false);
    });

    it("LineDecoder splits lines and trims CR", () => {
        const decoder = new LineDecoder();
        const lines: Array<string> = [];
        const encoder = new TextEncoder();
        decoder.push(encoder.encode("alpha\r\nbeta\nchar"), line => lines.push(line));
        expect(lines).toEqual(["alpha", "beta"]);
        decoder.flush(line => lines.push(line));
        expect(lines).toEqual(["alpha", "beta", "char"]);
    });

    it("LineDecoder buffers across chunks", () => {
        const decoder = new LineDecoder();
        const lines: Array<string> = [];
        const encoder = new TextEncoder();
        decoder.push(encoder.encode("hello"), line => lines.push(line));
        decoder.push(encoder.encode(" world\nnext"), line => lines.push(line));
        decoder.flush(line => lines.push(line));
        expect(lines).toEqual(["hello world", "next"]);
    });

    it("AsyncQueue yields queued values and resolves on close", async () => {
        const queue = new AsyncQueue<number>();
        queue.push(5);
        const iterator = queue[Symbol.asyncIterator]();
        const first = await iterator.next();
        expect(first).toEqual({ done: false, value: 5 });

        const pending = iterator.next();
        queue.close();
        const done = await pending;
        expect(done).toEqual({ done: true, value: undefined });
    });

    it("AsyncQueue resolves pending next when push arrives", async () => {
        const queue = new AsyncQueue<number>();
        const iterator = queue[Symbol.asyncIterator]();
        const pending = iterator.next();
        queue.push(7);
        const result = await pending;
        expect(result).toEqual({ done: false, value: 7 });
    });
});
