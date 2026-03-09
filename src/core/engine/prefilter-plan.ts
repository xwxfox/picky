import type { QueryPlan, PredicateSpec } from "@/core/engine/plan";
import type { PrefilterPlan, PrefilterPredicate, PrefilterStats, PrefilterStreamOptions } from "@/io/ingress/prefilter";
import { emitMetrics, startTiming, endTiming } from "@/core/engine/telemetry";
import type { TimingToken } from "@/core/engine/telemetry";
import { createPrefilterStats } from "@/io/ingress/prefilter-utils";
import { getPrefilterProgram } from "@/io/ingress/prefilter-c";

export type PrefilterContext = {
    plan: PrefilterPlan;
    program: import("@/io/ingress/prefilter-c").PrefilterProgram;
    stats: PrefilterStats;
    streamOptions: PrefilterStreamOptions;
};

export function createPrefilterContext<T extends Record<string, unknown>>(
    plan: QueryPlan<T>,
    timingParent?: TimingToken | null
): PrefilterContext | null {
    const planTiming = startTiming("ingress", "ingress.prefilter.plan", plan.id, timingParent ?? null);
    const prefilter = buildPrefilterPlan(plan);
    endTiming(planTiming, { skipData: true });
    if (!prefilter) {return null;}
    const compileTiming = startTiming("ingress", "ingress.prefilter.compile", plan.id, timingParent ?? null);
    const stats = createPrefilterStats();
    const program = getPrefilterProgram(prefilter);
    endTiming(compileTiming, { skipData: true });
    return {
        plan: prefilter,
        program,
        stats,
        streamOptions: {
            planId: plan.id,
            prefilter,
            prefilterProgram: program,
            prefilterMode: "auto",
            stats,
        },
    };
}

export function emitPrefilterMetrics(
    planId: string,
    context: PrefilterContext | null,
    extras?: { phase?: string }
): void {
    if (!context) {return;}
    emitMetrics({
        source: "ingress",
        planId,
        metrics: {
            extras: {
                prefilterChecked: context.stats.checked,
                prefilterMatched: context.stats.matched,
                prefilterParsed: context.stats.parsed,
                prefilterSkipped: context.stats.skipped,
                prefilterUnknown: context.stats.unknown,
                prefilterFields: context.plan.fields.length,
                prefilterPredicates: context.plan.predicates.length,
                phase: extras?.phase,
            },
        },
    });
}

export function appendPrefilterMetrics(
    base: Record<string, unknown>,
    context: PrefilterContext | null
): Record<string, unknown> {
    if (!context) {return base;}
    return {
        ...base,
        prefilterChecked: context.stats.checked,
        prefilterMatched: context.stats.matched,
        prefilterParsed: context.stats.parsed,
        prefilterSkipped: context.stats.skipped,
        prefilterUnknown: context.stats.unknown,
        prefilterFields: context.plan.fields.length,
        prefilterPredicates: context.plan.predicates.length,
    };
}

function buildPrefilterPlan<T extends Record<string, unknown>>(
    plan: QueryPlan<T>
): PrefilterPlan | null {
    const specs = plan.predicateSpecs ?? [];
    if (specs.length === 0) {return null;}
    const predicates: Array<PrefilterPredicate> = [];
    const fields: Array<string> = [];
    const seenFields = new Set<string>();

    for (let i = 0; i < specs.length; i++) {
        const spec = specs[i]!;
        const result = toPrefilterPredicate(spec);
        if (!result) {continue;}
        const items = Array.isArray(result) ? result : [result];
        for (let j = 0; j < items.length; j++) {
            const predicate = items[j]!;
            predicates.push(predicate);
            if (!seenFields.has(predicate.field)) {
                seenFields.add(predicate.field);
                fields.push(predicate.field);
            }
        }
    }

    if (predicates.length === 0) {return null;}
    const key = `pf:${predicates.map(formatPredicateKey).join("|")}`;
    return { fields, key, predicates };
}

function toPrefilterPredicate<T>(spec: PredicateSpec<T>): PrefilterPredicate | Array<PrefilterPredicate> | null {
    if (spec.kind !== "builtin") {return null;}
    if (!spec.accessors || spec.accessors.segments.length < 1) {return null;}
    const segments = spec.accessors.segments;
    const field = segments.join(".");
    if (!spec.op) {return null;}
    const extra = segments.length > 1 ? { segments } : {};
    if (spec.op === "isNull") {
        return { field, op: "isNull", type: "null", ...extra };
    }
    if (spec.op === "notNull") {
        return { field, op: "notNull", type: "null", ...extra };
    }
    if (spec.op === "gt" || spec.op === "gte" || spec.op === "lt" || spec.op === "lte") {
        if (typeof spec.value !== "number" || !Number.isFinite(spec.value)) {return null;}
        return { field, op: spec.op, type: "number", value: spec.value, ...extra };
    }
    if (spec.op === "in" || spec.op === "notIn") {
        if (!spec.valueSet || spec.valueSet.size === 0) {return null;}
        const values: Array<PrefilterPredicate["value"] extends ReadonlyArray<infer T> ? T : never> = Array.from(spec.valueSet) as Array<PrefilterPredicate["value"] extends ReadonlyArray<infer T> ? T : never>;
        let type: PrefilterPredicate["type"] | null = null;
        for (let i = 0; i < values.length; i++) {
            const value = values[i];
            let nextType: PrefilterPredicate["type"] | null = null;
            if (value === null) {
                nextType = "null";
            } else if (typeof value === "string") {
                nextType = "string";
            } else if (typeof value === "number" && Number.isFinite(value)) {
                nextType = "number";
            } else if (typeof value === "boolean") {
                nextType = "boolean";
            }
            if (!nextType) { return null; }
            if (!type) {
                type = nextType;
            } else if (type !== nextType) {
                return null;
            }
        }
        if (!type) {return null;}
        return { field, op: spec.op, type, value: values, ...extra };
    }
    if (spec.op === "eq" || spec.op === "ne") {
        if (spec.value === undefined) {return null;}
        if (spec.value === null) {
            return { field, op: spec.op, type: "null", ...extra };
        }
        if (typeof spec.value === "string") {
            return { field, op: spec.op, type: "string", value: spec.value, ...extra };
        }
        if (typeof spec.value === "number" && Number.isFinite(spec.value)) {
            return { field, op: spec.op, type: "number", value: spec.value, ...extra };
        }
        if (typeof spec.value === "boolean") {
            return { field, op: spec.op, type: "boolean", value: spec.value, ...extra };
        }
    }
    if (spec.op === "dateBetween") {
        const val = spec.value as { min?: Date | string | number; max?: Date | string | number } | undefined;
        if (!val || val.min == null || val.max == null) {return null;}
        const minStr = normalizeToIsoString(val.min);
        const maxStr = normalizeToIsoString(val.max);
        if (!minStr || !maxStr) {return null;}
        return [
            { field, op: "gte" as const, type: "string" as const, value: minStr, ...extra },
            { field, op: "lte" as const, type: "string" as const, value: maxStr, ...extra },
        ];
    }
    return null;
}

function formatPredicateKey(predicate: PrefilterPredicate): string {
    const value = predicate.value === undefined
        ? ""
        : typeof predicate.value === "string"
            ? JSON.stringify(predicate.value)
            : String(predicate.value);
    return `${predicate.field}:${predicate.op}:${predicate.type}:${value}`;
}

function normalizeToIsoString(value: Date | string | number): string | null {
    if (typeof value === "string") {return value;}
    if (typeof value === "number") {
        if (!Number.isFinite(value)) {return null;}
        return new Date(value).toISOString();
    }
    if (value instanceof Date) {
        const iso = value.toISOString();
        if (iso === "Invalid Date") {return null;}
        return iso;
    }
    return null;
}
