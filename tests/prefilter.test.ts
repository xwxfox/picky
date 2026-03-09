import { describe, expect, it } from "bun:test";
import { getPrefilterProgram, runPrefilter } from "@/io/ingress/prefilter-c";
import { batchPrefilterNdjson, applyJsonArrayPrefilter } from "@/io/ingress/prefilter-runtime";
import type { PrefilterPlan } from "@/io/ingress/prefilter";

describe("Prefilter C runtime", () => {
    const plan: PrefilterPlan = {
        fields: ["id", "name", "active", "score", "flag"],
        key: "test-plan",
        predicates: [
            { field: "id", op: "gt", type: "number", value: 2 },
            { field: "name", op: "eq", type: "string", value: "alpha" },
            { field: "active", op: "eq", type: "boolean", value: true },
            { field: "score", op: "ne", type: "number", value: 5 },
            { field: "flag", op: "isNull", type: "null" },
        ],
    };

    const program = getPrefilterProgram(plan);

    it("matches when all predicates pass", () => {
        const input = new TextEncoder().encode(
            '{"id":3,"name":"alpha","active":true,"score":4,"flag":null}'
        );
        const result = runPrefilter(program, input);
        expect(result).toEqual(1);
    });

    it("fails when a predicate fails", () => {
        const input = new TextEncoder().encode(
            '{"id":1,"name":"alpha","active":true,"score":4,"flag":null}'
        );
        const result = runPrefilter(program, input);
        expect(result).toEqual(0);
    });

    it("treats missing ne field as pass with other predicates", () => {
        const input = new TextEncoder().encode(
            '{"id":3,"name":"alpha","active":true,"flag":null}'
        );
        const result = runPrefilter(program, input);
        expect(result).toEqual(1);
    });

    it("handles escaped strings via unescape path", () => {
        const planEscaped: PrefilterPlan = {
            fields: ["name"],
            key: "escaped",
            predicates: [{ field: "name", op: "eq", type: "string", value: "a\"b" }],
        };
        const escapedProgram = getPrefilterProgram(planEscaped);
        const input = new TextEncoder().encode('{"name":"a\\\"b"}');
        const result = runPrefilter(escapedProgram, input);
        expect(result).toEqual(1);
    });

    it("returns unknown for nested objects", () => {
        const input = new TextEncoder().encode('{"id":3,"name":{"value":"alpha"}}');
        const result = runPrefilter(program, input);
        expect(result).toEqual(-1);
    });

    it("allows ne to pass when field missing", () => {
        const planNe: PrefilterPlan = {
            fields: ["name"],
            key: "ne-missing",
            predicates: [{ field: "name", op: "ne", type: "string", value: "alpha" }],
        };
        const neProgram = getPrefilterProgram(planNe);
        const input = new TextEncoder().encode('{"id":3}');
        const result = runPrefilter(neProgram, input);
        expect(result).toEqual(1);
    });

    it("supports in for string", () => {
        const planIn: PrefilterPlan = {
            fields: ["name"],
            key: "in-string",
            predicates: [{ field: "name", op: "in", type: "string", value: ["alpha", "beta"] }],
        };
        const inProgram = getPrefilterProgram(planIn);
        const input = new TextEncoder().encode('{"name":"beta"}');
        const result = runPrefilter(inProgram, input);
        expect(result).toEqual(1);
    });

    it("supports notIn for string", () => {
        const planNotIn: PrefilterPlan = {
            fields: ["name"],
            key: "notin-string",
            predicates: [{ field: "name", op: "notIn", type: "string", value: ["alpha", "beta"] }],
        };
        const notInProgram = getPrefilterProgram(planNotIn);
        const input = new TextEncoder().encode('{"name":"gamma"}');
        const result = runPrefilter(notInProgram, input);
        expect(result).toEqual(1);
    });
});

describe("Batch prefilter", () => {
    const batchPlan: PrefilterPlan = {
        fields: ["status"],
        key: "batch-test",
        predicates: [{ field: "status", op: "eq", type: "string", value: "active" }],
    };
    const batchProgram = getPrefilterProgram(batchPlan);
    const encode = (s: string) => new TextEncoder().encode(s);

    describe("batchPrefilterNdjson", () => {
        it("matches correct items from NDJSON buffer", () => {
            const ndjson = [
                '{"status":"active","id":1}',
                '{"status":"inactive","id":2}',
                '{"status":"active","id":3}',
                '{"status":"closed","id":4}',
            ].join("\n");
            const result = batchPrefilterNdjson(encode(ndjson), {
                prefilter: batchPlan,
                prefilterProgram: batchProgram,
            });
            expect(result).not.toBeNull();
            expect(result).toHaveLength(2);
            expect(result![0]).toBe('{"status":"active","id":1}');
            expect(result![1]).toBe('{"status":"active","id":3}');
        });

        it("returns null when no prefilter is active (undefined options)", () => {
            const buf = encode('{"status":"active"}');
            const result = batchPrefilterNdjson(buf, undefined);
            expect(result).toBeNull();
        });

        it("returns null when prefilterMode is off", () => {
            const buf = encode('{"status":"active"}');
            const result = batchPrefilterNdjson(buf, {
                prefilter: batchPlan,
                prefilterProgram: batchProgram,
                prefilterMode: "off",
            });
            expect(result).toBeNull();
        });

        it("returns empty array when no items match", () => {
            const ndjson = [
                '{"status":"inactive","id":1}',
                '{"status":"closed","id":2}',
            ].join("\n");
            const result = batchPrefilterNdjson(encode(ndjson), {
                prefilter: batchPlan,
                prefilterProgram: batchProgram,
            });
            expect(result).not.toBeNull();
            expect(result).toEqual([]);
        });

        it("handles empty lines in NDJSON", () => {
            const ndjson = '{"status":"active","id":1}\n\n{"status":"inactive","id":2}\n\n{"status":"active","id":3}\n';
            const result = batchPrefilterNdjson(encode(ndjson), {
                prefilter: batchPlan,
                prefilterProgram: batchProgram,
            });
            expect(result).not.toBeNull();
            expect(result).toHaveLength(2);
            expect(result![0]).toBe('{"status":"active","id":1}');
            expect(result![1]).toBe('{"status":"active","id":3}');
        });

        it("handles single item buffer without trailing newline", () => {
            const buf = encode('{"status":"active","id":1}');
            const result = batchPrefilterNdjson(buf, {
                prefilter: batchPlan,
                prefilterProgram: batchProgram,
            });
            expect(result).not.toBeNull();
            expect(result).toHaveLength(1);
            expect(result![0]).toBe('{"status":"active","id":1}');
        });
    });

    describe("applyJsonArrayPrefilter", () => {
        it("filters JSON array correctly", () => {
            const jsonArray = '[{"status":"active","id":1},{"status":"inactive","id":2},{"status":"active","id":3}]';
            const result = applyJsonArrayPrefilter(encode(jsonArray), {
                prefilter: batchPlan,
                prefilterProgram: batchProgram,
            });
            expect(result).not.toBeNull();
            expect(result).toHaveLength(2);
            expect(result![0]!).toEqual({ status: "active", id: 1 });
            expect(result![1]!).toEqual({ status: "active", id: 3 });
        });

        it("returns null when no prefilter active", () => {
            const buf = encode('[{"status":"active"}]');
            const result = applyJsonArrayPrefilter(buf, undefined);
            expect(result).toBeNull();
        });

        it("returns empty array string when no items match", () => {
            const jsonArray = '[{"status":"inactive","id":1},{"status":"closed","id":2}]';
            const result = applyJsonArrayPrefilter(encode(jsonArray), {
                prefilter: batchPlan,
                prefilterProgram: batchProgram,
            });
            expect(result).not.toBeNull();
            expect(result).toEqual([]);
        });
    });

    describe("stats tracking", () => {
        it("tracks checked/matched/skipped for NDJSON batch", () => {
            const ndjson = [
                '{"status":"active","id":1}',
                '{"status":"inactive","id":2}',
                '{"status":"active","id":3}',
                '{"status":"closed","id":4}',
            ].join("\n");
            const stats = { checked: 0, matched: 0, parsed: 0, skipped: 0, unknown: 0 };
            batchPrefilterNdjson(encode(ndjson), {
                prefilter: batchPlan,
                prefilterProgram: batchProgram,
                stats,
            });
            expect(stats.checked).toBe(4);
            expect(stats.matched).toBe(2);
            expect(stats.parsed).toBe(2);
            expect(stats.skipped).toBe(2);
        });

        it("tracks checked/matched/skipped for JSON array batch", () => {
            const jsonArray = '[{"status":"active","id":1},{"status":"inactive","id":2},{"status":"active","id":3}]';
            const stats = { checked: 0, matched: 0, parsed: 0, skipped: 0, unknown: 0 };
            applyJsonArrayPrefilter(encode(jsonArray), {
                prefilter: batchPlan,
                prefilterProgram: batchProgram,
                stats,
            });
            expect(stats.checked).toBe(3);
            expect(stats.matched).toBe(2);
            expect(stats.parsed).toBe(2);
            expect(stats.skipped).toBe(1);
        });
    });
});

describe("dateBetween prefilter via plan", () => {
    it("decomposes dateBetween into gte+lte string predicates", () => {
        const plan: PrefilterPlan = {
            fields: ["created"],
            key: "datebetween-test",
            predicates: [
                { field: "created", op: "gte", type: "string", value: "2026-01-01" },
                { field: "created", op: "lte", type: "string", value: "2026-02-28" },
            ],
        };
        const program = getPrefilterProgram(plan);
        // In range
        const inRange = new TextEncoder().encode('{"created":"2026-01-15"}');
        expect(runPrefilter(program, inRange)).toEqual(1);
        // Before range
        const beforeRange = new TextEncoder().encode('{"created":"2025-12-31"}');
        expect(runPrefilter(program, beforeRange)).toEqual(0);
        // After range
        const afterRange = new TextEncoder().encode('{"created":"2026-03-01"}');
        expect(runPrefilter(program, afterRange)).toEqual(0);
        // At lower bound (gte)
        const atMin = new TextEncoder().encode('{"created":"2026-01-01"}');
        expect(runPrefilter(program, atMin)).toEqual(1);
        // At upper bound (lte)
        const atMax = new TextEncoder().encode('{"created":"2026-02-28"}');
        expect(runPrefilter(program, atMax)).toEqual(1);
    });

    it("handles ISO datetime strings with time component", () => {
        const plan: PrefilterPlan = {
            fields: ["ts"],
            key: "datebetween-iso-test",
            predicates: [
                { field: "ts", op: "gte", type: "string", value: "2026-01-01T00:00:00.000Z" },
                { field: "ts", op: "lte", type: "string", value: "2026-01-31T23:59:59.999Z" },
            ],
        };
        const program = getPrefilterProgram(plan);
        const match = new TextEncoder().encode('{"ts":"2026-01-15T12:30:00.000Z"}');
        expect(runPrefilter(program, match)).toEqual(1);
        const noMatch = new TextEncoder().encode('{"ts":"2026-02-01T00:00:00.000Z"}');
        expect(runPrefilter(program, noMatch)).toEqual(0);
    });

    it("rejects non-string values for string gte/lte", () => {
        const plan: PrefilterPlan = {
            fields: ["created"],
            key: "datebetween-type-mismatch",
            predicates: [
                { field: "created", op: "gte", type: "string", value: "2026-01-01" },
                { field: "created", op: "lte", type: "string", value: "2026-02-28" },
            ],
        };
        const program = getPrefilterProgram(plan);
        // Number value for a string predicate should fail
        const numVal = new TextEncoder().encode('{"created":1234567890}');
        expect(runPrefilter(program, numVal)).toEqual(0);
        // Null value should fail
        const nullVal = new TextEncoder().encode('{"created":null}');
        expect(runPrefilter(program, nullVal)).toEqual(0);
    });
});

describe("Nested field path prefilter", () => {
    it("matches nested path eq predicate", () => {
        const plan: PrefilterPlan = {
            fields: ["meta.owner.name"],
            key: "nested-eq-test",
            predicates: [
                { field: "meta.owner.name", op: "eq", type: "string", value: "Alice", segments: ["meta", "owner", "name"] },
            ],
        };
        const program = getPrefilterProgram(plan);
        const match = new TextEncoder().encode('{"meta":{"owner":{"name":"Alice"}}}');
        expect(runPrefilter(program, match)).toEqual(1);
        const noMatch = new TextEncoder().encode('{"meta":{"owner":{"name":"Bob"}}}');
        expect(runPrefilter(program, noMatch)).toEqual(0);
    });

    it("matches nested path in predicate", () => {
        const plan: PrefilterPlan = {
            fields: ["meta.owner.name"],
            key: "nested-in-test",
            predicates: [
                { field: "meta.owner.name", op: "in", type: "string", value: ["Alice", "Bob", "Cara"], segments: ["meta", "owner", "name"] },
            ],
        };
        const program = getPrefilterProgram(plan);
        const match = new TextEncoder().encode('{"meta":{"owner":{"name":"Bob"}}}');
        expect(runPrefilter(program, match)).toEqual(1);
        const noMatch = new TextEncoder().encode('{"meta":{"owner":{"name":"Zara"}}}');
        expect(runPrefilter(program, noMatch)).toEqual(0);
    });

    it("handles missing intermediate key (field not seen = fail for mandatory op)", () => {
        const plan: PrefilterPlan = {
            fields: ["meta.owner.name"],
            key: "nested-missing-test",
            predicates: [
                { field: "meta.owner.name", op: "eq", type: "string", value: "Alice", segments: ["meta", "owner", "name"] },
            ],
        };
        const program = getPrefilterProgram(plan);
        // meta key missing entirely
        const noMeta = new TextEncoder().encode('{"id":1}');
        expect(runPrefilter(program, noMeta)).toEqual(0);
        // meta.owner missing
        const noOwner = new TextEncoder().encode('{"meta":{"id":1}}');
        expect(runPrefilter(program, noOwner)).toEqual(0);
    });

    it("handles non-object intermediate value", () => {
        const plan: PrefilterPlan = {
            fields: ["meta.owner.name"],
            key: "nested-nonobj-test",
            predicates: [
                { field: "meta.owner.name", op: "eq", type: "string", value: "Alice", segments: ["meta", "owner", "name"] },
            ],
        };
        const program = getPrefilterProgram(plan);
        // meta is a string, not an object
        const metaStr = new TextEncoder().encode('{"meta":"not-an-object"}');
        expect(runPrefilter(program, metaStr)).toEqual(0);
        // meta.owner is a number, not an object
        const ownerNum = new TextEncoder().encode('{"meta":{"owner":42}}');
        expect(runPrefilter(program, ownerNum)).toEqual(0);
    });

    it("combines nested and flat predicates", () => {
        const plan: PrefilterPlan = {
            fields: ["name", "meta.owner.name"],
            key: "nested-combined-test",
            predicates: [
                { field: "name", op: "notNull", type: "null" },
                { field: "meta.owner.name", op: "eq", type: "string", value: "Alice", segments: ["meta", "owner", "name"] },
            ],
        };
        const program = getPrefilterProgram(plan);
        const match = new TextEncoder().encode('{"name":"test","meta":{"owner":{"name":"Alice"}}}');
        expect(runPrefilter(program, match)).toEqual(1);
        const failFlat = new TextEncoder().encode('{"name":null,"meta":{"owner":{"name":"Alice"}}}');
        expect(runPrefilter(program, failFlat)).toEqual(0);
        const failNested = new TextEncoder().encode('{"name":"test","meta":{"owner":{"name":"Bob"}}}');
        expect(runPrefilter(program, failNested)).toEqual(0);
    });

    it("handles 2-level nested path", () => {
        const plan: PrefilterPlan = {
            fields: ["address.city"],
            key: "nested-2level-test",
            predicates: [
                { field: "address.city", op: "eq", type: "string", value: "NYC", segments: ["address", "city"] },
            ],
        };
        const program = getPrefilterProgram(plan);
        const match = new TextEncoder().encode('{"address":{"city":"NYC"}}');
        expect(runPrefilter(program, match)).toEqual(1);
        const noMatch = new TextEncoder().encode('{"address":{"city":"LA"}}');
        expect(runPrefilter(program, noMatch)).toEqual(0);
    });

    it("works in NDJSON batch mode with nested paths", () => {
        const plan: PrefilterPlan = {
            fields: ["meta.owner.name"],
            key: "nested-ndjson-test",
            predicates: [
                { field: "meta.owner.name", op: "in", type: "string", value: ["Alice", "Bob"], segments: ["meta", "owner", "name"] },
            ],
        };
        const program = getPrefilterProgram(plan);
        const ndjson = [
            '{"meta":{"owner":{"name":"Alice"}},"id":1}',
            '{"meta":{"owner":{"name":"Cara"}},"id":2}',
            '{"meta":{"owner":{"name":"Bob"}},"id":3}',
        ].join("\n");
        const result = batchPrefilterNdjson(new TextEncoder().encode(ndjson), {
            prefilter: plan,
            prefilterProgram: program,
        });
        expect(result).not.toBeNull();
        expect(result).toHaveLength(2);
        expect(JSON.parse(result![0]!).id).toBe(1);
        expect(JSON.parse(result![1]!).id).toBe(3);
    });

    it("works in JSON array batch mode with nested paths", () => {
        const plan: PrefilterPlan = {
            fields: ["meta.owner.name"],
            key: "nested-jsonarray-test",
            predicates: [
                { field: "meta.owner.name", op: "eq", type: "string", value: "Alice", segments: ["meta", "owner", "name"] },
            ],
        };
        const program = getPrefilterProgram(plan);
        const jsonArray = '[{"meta":{"owner":{"name":"Alice"}},"id":1},{"meta":{"owner":{"name":"Bob"}},"id":2}]';
        const result = applyJsonArrayPrefilter(new TextEncoder().encode(jsonArray), {
            prefilter: plan,
            prefilterProgram: program,
        });
        expect(result).not.toBeNull();
        expect(result).toHaveLength(1);
        expect((result![0]! as Record<string, unknown>).id).toBe(1);
    });
});
