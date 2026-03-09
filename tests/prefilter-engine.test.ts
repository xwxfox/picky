import { describe, expect, it } from "bun:test";
import { Engine, IngressEngine } from "../src";

type BaseItem = {
    id?: number;
    name?: unknown;
    active?: boolean;
    score?: number | null;
    flag?: null;
    meta?: { owner?: { name?: string } };
};

const runBasePlan = (input: string): number => {
    const item = JSON.parse(input) as BaseItem;
    const result = Engine.from(IngressEngine.from([item]))
        .greaterThan("id", 2)
        .equals("name", "alpha")
        .equals("active", true)
        .notEquals("score", 5)
        .isNull("flag")
        .out()
        .result();
    return result.length;
};

const runNotEqualsPlan = (input: string): number => {
    const item = JSON.parse(input) as BaseItem;
    const result = Engine.from(IngressEngine.from([item]))
        .notEquals("name", "alpha")
        .out()
        .result();
    return result.length;
};

const runInPlan = (input: string): number => {
    const item = JSON.parse(input) as BaseItem;
    const result = Engine.from(IngressEngine.from([item]))
        .in("meta.owner.name", ["Bob"])
        .out()
        .result();
    return result.length;
};

const runNotInPlan = (input: string): number => {
    const item = JSON.parse(input) as BaseItem;
    const result = Engine.from(IngressEngine.from([item]))
        .notIn("meta.owner.name", ["Bob"])
        .out()
        .result();
    return result.length;
};

describe("Prefilter engine parity", () => {
    it("matches when all predicates pass", () => {
        const result = runBasePlan('{"id":3,"name":"alpha","active":true,"score":4,"flag":null}');
        expect(result).toEqual(1);
    });

    it("fails when a predicate fails", () => {
        const result = runBasePlan('{"id":1,"name":"alpha","active":true,"score":4,"flag":null}');
        expect(result).toEqual(0);
    });

    it("treats missing ne field as pass with other predicates", () => {
        const result = runBasePlan('{"id":3,"name":"alpha","active":true,"flag":null}');
        expect(result).toEqual(1);
    });

    it("handles escaped strings", () => {
        const item = JSON.parse('{"name":"a\\"b"}') as BaseItem;
        const result = Engine.from(IngressEngine.from([item]))
            .equals("name", "a\"b")
            .out()
            .result();
        expect(result.length).toEqual(1);
    });

    it("treats nested objects as non-matching", () => {
        const result = runBasePlan('{"id":3,"name":{"value":"alpha"}}');
        expect(result).toEqual(0);
    });

    it("allows ne to pass when field missing", () => {
        const result = runNotEqualsPlan('{"id":3}');
        expect(result).toEqual(1);
    });

    it("supports in for string", () => {
        const result = runInPlan('{"meta":{"owner":{"name":"Bob"}}}');
        expect(result).toEqual(1);
    });

    it("supports notIn for string", () => {
        const result = runNotInPlan('{"meta":{"owner":{"name":"Cara"}}}');
        expect(result).toEqual(1);
    });
});
