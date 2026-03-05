import { describe, it, expect } from "bun:test";
import { AsyncExecutionEngine } from "@/core/engine/executor-async";
import { planIngress } from "@/core/engine/ingress-plan";
import type { AsyncIngressSource, IngressCapabilities, IngressHints } from "@/io/ingress/types";
import { Engine, IngressEngine } from "../src";

type AsyncItem = {
    id: number;
    name: string;
    score: number;
    tags: Array<string>;
};

const baseItems: Array<AsyncItem> = [
    { id: 1, name: "al", score: 5, tags: ["red"] },
    { id: 2, name: "a_l", score: 10, tags: ["blue"] },
    { id: 3, name: "alpha", score: 8, tags: ["red", "blue"] },
    { id: 4, name: "gamma", score: 2, tags: [] },
];

const emptyCaps: IngressCapabilities = {
    count: false,
    filter: false,
    group: false,
    order: false,
    paginate: false,
    search: false,
};

const makeAsyncIngress = <T extends Record<string, unknown>>(
    items: ReadonlyArray<T>,
    options?: {
        hints?: IngressHints<T>;
        capabilities?: Partial<IngressCapabilities>;
        withMaterialize?: boolean;
    }
): {
    ingress: ReturnType<typeof IngressEngine.fromSource<T>>;
    stats: { materializeCalls: number; streamCalls: number; streamedItems: number };
} => {
    const stats = { materializeCalls: 0, streamCalls: 0, streamedItems: 0 };
    const source: AsyncIngressSource<T> = {
        capabilities: options?.capabilities,
        hints: options?.hints,
        mode: "async",
        stream: async function* () {
            stats.streamCalls++;
            for (let i = 0; i < items.length; i++) {
                stats.streamedItems++;
                yield items[i]!;
            }
        },
    };
    if (options?.withMaterialize !== false) {
        source.materialize = async () => {
            stats.materializeCalls++;
            return items;
        };
    }
    const ingress = IngressEngine.fromSource(source);
    return { ingress, stats };
};

describe("Async ingress planning", () => {
    it("prefers streaming when requested and no ordering/grouping/search", () => {
        const plan = planIngress({ preferStreaming: true }, emptyCaps, false, false, false);
        expect(plan.strategy).toEqual("stream");
    });

    it("streams for ordering when source supports order", () => {
        const plan = planIngress(undefined, { ...emptyCaps, order: true }, true, false, false);
        expect(plan.strategy).toEqual("stream");
    });

    it("eagers for ordering when source lacks order", () => {
        const plan = planIngress(undefined, emptyCaps, true, false, false);
        expect(plan.strategy).toEqual("eager");
    });

    it("streams for search when capability is enabled", () => {
        const plan = planIngress(undefined, { ...emptyCaps, search: true }, false, false, true);
        expect(plan.strategy).toEqual("stream");
    });

    it("streams when estimated size exceeds thresholds", () => {
        const highCount = planIngress({ eagerThreshold: 10, estimatedCount: 100 }, emptyCaps, false, false, false);
        expect(highCount.strategy).toEqual("stream");

        const highBytes = planIngress(
            { avgRowBytes: 1000, estimatedCount: 10, maxMemoryBytes: 5000 },
            emptyCaps,
            false,
            false,
            false
        );
        expect(highBytes.strategy).toEqual("stream");
    });
});

describe("AsyncExecutionEngine", () => {
    it("streams when plan chooses stream and respects windowLimit", async () => {
        const { ingress, stats } = makeAsyncIngress(baseItems, { hints: { preferStreaming: true } });
        if (ingress.mode !== "async") { throw new Error("Expected async ingress."); }
        const plan = Engine.from(ingress).compilePlan();
        const executor = new AsyncExecutionEngine<AsyncItem>();
        const result = await executor.execute(ingress, plan, { windowLimit: 2 });
        expect(result.map(item => item.id)).toEqual([1, 2]);
        expect(stats.materializeCalls).toEqual(0);
        expect(stats.streamCalls).toEqual(1);
        expect(stats.streamedItems).toEqual(2);
    });

    it("materializes when ordering is required but unsupported", async () => {
        const { ingress, stats } = makeAsyncIngress(baseItems, { hints: { estimatedCount: 2 } });
        if (ingress.mode !== "async") { throw new Error("Expected async ingress."); }
        const plan = Engine.from(ingress).greaterThan("score", 4).compilePlan();
        const executor = new AsyncExecutionEngine<AsyncItem>();
        const result = await executor.execute(ingress, plan, { requiresOrdering: true });
        expect(result.map(item => item.id)).toEqual([1, 2, 3]);
        expect(stats.materializeCalls).toEqual(1);
        expect(stats.streamCalls).toEqual(0);
    });

    it("runs async fuzzy search with score ordering", async () => {
        const { ingress, stats } = makeAsyncIngress(baseItems, {
            capabilities: { search: true },
            hints: { preferStreaming: true },
        });
        if (ingress.mode !== "async") { throw new Error("Expected async ingress."); }
        const plan = Engine.from(ingress)
            .configureFuzzy({ fields: [{ path: "name" }], order: "score" })
            .search("al")
            .compilePlan();
        const executor = new AsyncExecutionEngine<AsyncItem>();
        const result = await executor.execute(ingress, plan, { windowLimit: 1 });
        expect(result.map(item => item.id)).toEqual([2]);
        expect(stats.materializeCalls).toEqual(0);
    });
});

describe("Async ingress helpers", () => {
    it("materialize iterates stream when source lacks materialize", async () => {
        const source: AsyncIngressSource<AsyncItem> = {
            mode: "async",
            stream: async function* () {
                for (let i = 0; i < baseItems.length; i++) { yield baseItems[i]!; }
            },
        };
        const ingress = IngressEngine.fromSource(source);
        if (ingress.mode !== "async") { throw new Error("Expected async ingress."); }
        const result = await ingress.materialize();
        expect(result.map(item => item.id)).toEqual([1, 2, 3, 4]);
    });

    it("close awaits async close handler", async () => {
        let closed = 0;
        const source: AsyncIngressSource<AsyncItem> = {
            close: async () => { closed += 1; },
            mode: "async",
            stream: async function* () {
                for (let i = 0; i < baseItems.length; i++) { yield baseItems[i]!; }
            },
        };
        const ingress = IngressEngine.fromSource(source);
        if (ingress.mode !== "async") { throw new Error("Expected async ingress."); }
        await ingress.close();
        expect(closed).toEqual(1);
    });
});

describe("Async egress", () => {
    it("supports search, tags, pagination, grouping, and metadata", async () => {
        const { ingress } = makeAsyncIngress(baseItems, { hints: { estimatedCount: baseItems.length } });
        if (ingress.mode !== "async") { throw new Error("Expected async ingress."); }

        const fuzzyResult = await Engine.from(ingress)
            .configureFuzzy({ fields: [{ path: "name" }], order: "score" })
            .out()
            .search("al")
            .result();
        expect(fuzzyResult.map(item => item.id)).toEqual([2, 1, 3]);

        const tagResult = await Engine.from(ingress)
            .configureTagger({
                rules: [{ contains: "al", field: "name", tag: "red" }],
                tags: ["red"],
            })
            .out()
            .tags({ hasAny: ["red"] })
            .result();
        expect(tagResult.map(item => item.id)).toEqual([1, 3]);

        const cursor = await Engine.from(ingress)
            .out()
            .paginate({ pageSize: 2 });
        expect(cursor.data.map(item => item.id)).toEqual([1, 2]);

        const grouped = await Engine.from(ingress)
            .out()
            .groupBy("score");
        expect(grouped.get(10)?.map(item => item.id)).toEqual([2]);

        const metadata = await Engine.from(ingress)
            .configureFuzzy({ fields: [{ path: "name" }], order: "score" })
            .search("al")
            .out()
            .resultWithMetadata();
        expect(metadata[0]?.score).toBeDefined();
    });

    it("builder orderBy pushes orders into the plan", () => {
        const ingress = IngressEngine.from(baseItems);
        const plan = Engine.from(ingress)
            .orderBy("name", { direction: "desc", nulls: "first" })
            .thenBy("score")
            .compilePlan();
        expect(plan.pushdownOrders).toEqual([
            { direction: "desc", field: "name", nulls: "first" },
            { direction: "asc", field: "score", nulls: undefined },
        ]);
    });

    it("custom predicates disable pushdown safety", () => {
        const ingress = IngressEngine.from(baseItems);
        const plan = Engine.from(ingress)
            .custom(() => true)
            .compilePlan();
        expect(plan.pushdownSafe).toEqual(false);
    });
});
