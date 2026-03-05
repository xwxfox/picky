export function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}

export class LineDecoder {
    private buffer = "";
    private readonly decoder = new TextDecoder();

    push(chunk: Uint8Array, onLine: (line: string) => void): void {
        this.buffer += this.decoder.decode(chunk, { stream: true });
        this.drain(onLine);
    }

    flush(onLine: (line: string) => void): void {
        this.buffer += this.decoder.decode();
        this.drain(onLine);
        if (this.buffer.length > 0) {
            const line = trimLineEnding(this.buffer);
            this.buffer = "";
            if (line.length > 0) {onLine(line);}
        }
    }

    private drain(onLine: (line: string) => void): void {
        let start = 0;
        while (true) {
            const idx = this.buffer.indexOf("\n", start);
            if (idx === -1) {
                if (start > 0) {this.buffer = this.buffer.slice(start);}
                return;
            }
            const raw = this.buffer.slice(start, idx);
            const line = trimLineEnding(raw);
            if (line.length > 0) {onLine(line);}
            start = idx + 1;
        }
    }
}

function trimLineEnding(input: string): string {
    if (input.length === 0) {return input;}
    if (input.charCodeAt(input.length - 1) === 13) {return input.slice(0, -1);} // \r
    return input;
}

type Resolver<T> = (result: IteratorResult<T>) => void;

export class AsyncQueue<T> implements AsyncIterable<T> {
    private readonly values: Array<T> = [];
    private readonly resolvers: Array<Resolver<T>> = [];
    private closed = false;

    push(value: T): void {
        if (this.closed) {return;}
        const resolve = this.resolvers.shift();
        if (resolve) {
            resolve({ done: false, value });
            return;
        }
        this.values.push(value);
    }

    close(): void {
        if (this.closed) {return;}
        this.closed = true;
        while (this.resolvers.length > 0) {
            const resolve = this.resolvers.shift();
            if (resolve) {resolve({ done: true, value: undefined });}
        }
    }

    [Symbol.asyncIterator](): AsyncIterator<T> {
        return {
            next: () => {
                if (this.values.length > 0) {
                    const value = this.values.shift();
                    if (value !== undefined) {return Promise.resolve({ done: false, value });}
                }
                if (this.closed) {return Promise.resolve({ done: true, value: undefined });}
                return new Promise<IteratorResult<T>>((resolve) => {
                    this.resolvers.push(resolve);
                });
            },
        };
    }
}
