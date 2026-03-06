import type { Predicate, ResolveObject, ResolveValue } from "@/types";
import type { CacheState } from "@/core/shared/cache";
import type { PredicateSpec, PredicateOp, PlannerDiagnostics, PredicateDiagnostic, QueryPlan, PlanningMeta } from "./plan";
import type { PushdownOrder, PushdownPredicate } from "@/io/ingress/adapters/pushdown";
import type { SearchFilterState } from "@/types/search";
import type { CompiledFuzzyConfig, CompiledTaggerConfig } from "@/core/search/runtime";
import type { IngressHints } from "@/io/ingress/types";
import { createComparePredicate } from "@/core/engine/predicates/compare";
import { createBetweenPredicate } from "@/core/engine/predicates/range";
import { getPlannerDiagnosticsEnabled, logPlanner } from "./planner-logger";

type PlanInput<T extends Record<string, unknown>> = {
    cache: CacheState;
    fuzzyConfig: CompiledFuzzyConfig<T> | null;
    hints?: IngressHints<T>;
    id: string;
    predicates: Array<Predicate<T>>;
    predicateSpecs?: Array<PredicateSpec<T>>;
    pushdownOrders: Array<PushdownOrder>;
    pushdownPredicates: Array<PushdownPredicate>;
    pushdownSafe: boolean;
    searchFilters: Array<SearchFilterState>;
    strictSearch: boolean;
    taggerConfig: CompiledTaggerConfig<T, string> | null;
};

type AnalyzedPredicate<T> = PredicateSpec<T> & { cost: number; selectivity: number };

type RangeBound = { inclusive: boolean; value: number | string | bigint };

type FieldRangeState = {
    max?: RangeBound;
    min?: RangeBound;
    type?: "number" | "string" | "bigint";
};

type FieldMergeState<T> = {
    accessors: PredicateSpec<T>["accessors"];
    eq?: ResolveValue;
    inSet?: Set<ResolveValue>;
    notInSet?: Set<ResolveValue>;
    range?: FieldRangeState;
    sourceIds: Array<string>;
    template: PredicateSpec<T>;
};

type MergeResult<T> = {
    alwaysFalse: boolean;
    merges: Array<{ reason: string; removed: Array<string>; result: Array<string> }>;
    specs: Array<PredicateSpec<T>>;
};

const maxSelectivity = 0.999;
const minSelectivity = 0.001;
const alwaysFalsePredicate = () => false;

export function optimizePlan<T extends Record<string, unknown>>(
    input: PlanInput<T>
): QueryPlan<T> {
    const specs = input.predicateSpecs ?? [];
    if (specs.length === 0) {
        const predicateFn = compilePredicateFn(input.predicates);
        return {
            cache: input.cache,
            fuzzyConfig: input.fuzzyConfig,
            id: input.id,
            predicateFn,
            predicates: input.predicates,
            pushdownOrders: input.pushdownOrders,
            pushdownPredicates: input.pushdownPredicates,
            pushdownSafe: input.pushdownSafe,
            residualPredicateFn: predicateFn,
            residualPredicates: input.predicates,
            searchFilters: input.searchFilters,
            strictSearch: input.strictSearch,
            taggerConfig: input.taggerConfig,
        };
    }

    const diagnosticsEnabled = getPlannerDiagnosticsEnabled();
    const analyzed = specs.map((spec) => {
        const estimate = estimatePredicate(spec, input.hints);
        return { ...spec, cost: estimate.cost, selectivity: estimate.selectivity };
    });

    let hasConstFalse = false;
    for (let i = 0; i < analyzed.length; i++) {
        if (analyzed[i]!.constantValue === false) {
            hasConstFalse = true;
            break;
        }
    }

    if (diagnosticsEnabled) {
        logPlanner({
            source: "execution",
            type: "input",
            planId: input.id,
            data: {
                predicates: analyzed.map((spec) => toDiagnostic(spec, spec.cost, spec.selectivity)),
            },
        });
    }

    const merges: Array<{ reason: string; removed: Array<string>; result: Array<string> }> = [];
    const orderChanges: Array<{ after: Array<string>; before: Array<string>; reason: string }> = [];

    const optimized: Array<AnalyzedPredicate<T>> = [];
    let alwaysFalse = false;
    let segment: Array<AnalyzedPredicate<T>> = [];
    const flushSegment = () => {
        if (segment.length === 0) {return;}
        const merged = mergeSegment(segment, merges);
        if (merged.alwaysFalse) {
            alwaysFalse = true;
            segment = [];
            return;
        }
        const mergedAnalyzed = merged.specs.map((spec) => {
            const estimate = estimatePredicate(spec, input.hints);
            return { ...spec, cost: estimate.cost, selectivity: estimate.selectivity } as AnalyzedPredicate<T>;
        });

        const before = mergedAnalyzed.map((spec) => spec.id);
        const ordered = mergedAnalyzed.slice().sort((a, b) => {
            if (a.cost !== b.cost) {return a.cost - b.cost;}
            if (a.selectivity !== b.selectivity) {return a.selectivity - b.selectivity;}
            return a.id < b.id ? -1 : (a.id > b.id ? 1 : 0);
        });
        const after = ordered.map((spec) => spec.id);
        if (diagnosticsEnabled && before.join(",") !== after.join(",")) {
            orderChanges.push({ before, after, reason: "cost/selectivity" });
        }
        optimized.push(...ordered);
        segment = [];
    };

    for (let i = 0; i < analyzed.length; i++) {
        const spec = analyzed[i]!;
        if (!spec.reorderable) {
            flushSegment();
            optimized.push(spec);
            continue;
        }
        segment.push(spec);
    }
    flushSegment();

    if (hasConstFalse) {
        alwaysFalse = true;
    }

    if (alwaysFalse) {
        const predicateFn = alwaysFalsePredicate;
        const plan: QueryPlan<T> = {
            alwaysFalse: true,
            cache: input.cache,
            diagnostics: diagnosticsEnabled
                ? {
                    final: { alwaysFalse: true, predicates: [], pushdownPredicates: [], residualPredicates: [] },
                    input: analyzed.map((spec) => toDiagnostic(spec, spec.cost, spec.selectivity)),
                    merges,
                    order: orderChanges,
                    pushdown: { applied: [], candidates: [], full: false, residual: [] },
                }
                : undefined,
            fuzzyConfig: input.fuzzyConfig,
            id: input.id,
            planning: {
                alwaysFalse: true,
                estimatedCost: 0,
                estimatedSelectivity: 0,
                predicateCount: 0,
                pushdownCount: 0,
                reorderable: true,
                residualCount: 0,
            },
            predicateFn,
            predicates: [],
            pushdownOrders: input.pushdownOrders,
            pushdownPredicates: [],
            pushdownSafe: false,
            residualPredicateFn: alwaysFalsePredicate,
            residualPredicates: [],
            searchFilters: input.searchFilters,
            strictSearch: input.strictSearch,
            taggerConfig: input.taggerConfig,
        };
        if (diagnosticsEnabled) {
            logPlanner({ source: "execution", type: "merge", planId: input.id, data: { merges } });
            logPlanner({ source: "execution", type: "order", planId: input.id, data: { order: orderChanges } });
            logPlanner({ source: "execution", type: "pushdown", planId: input.id, data: plan.diagnostics?.pushdown });
            logPlanner({ source: "execution", type: "final", planId: input.id, data: plan.diagnostics?.final });
        }
        return plan;
    }

    if (diagnosticsEnabled) {
        logPlanner({ source: "execution", type: "merge", planId: input.id, data: { merges } });
        logPlanner({ source: "execution", type: "order", planId: input.id, data: { order: orderChanges } });
    }

    const predicateSpecs = optimized.map(stripAnalysis);
    const skipPushdown = input.searchFilters.length > 0;
    const shouldAllowPushdown = input.pushdownSafe;
    const predicates = predicateSpecs.map((spec) => spec.predicate);
    const predicateFn = compilePredicateFn(predicates);

    const pushdownSpecs = predicateSpecs.filter((spec) => !!spec.pushdown && spec.kind === "builtin");
    const residualSpecs = predicateSpecs.filter((spec) => !spec.pushdown || spec.kind !== "builtin");
    const pushdownPredicates = pushdownSpecs.map((spec) => spec.pushdown!) as Array<PushdownPredicate>;
    const residualPredicates = residualSpecs.map((spec) => spec.predicate);
    const residualPredicateFn = compilePredicateFn(residualPredicates);
    const pushdownSafe = residualPredicates.length === 0;
    const finalPushdownPredicates = skipPushdown || !shouldAllowPushdown ? [] : pushdownPredicates;

    const planning = summarizePlanning(
        predicateSpecs,
        skipPushdown || !shouldAllowPushdown ? 0 : pushdownSpecs.length,
        residualPredicates.length,
        false
    );
    const diagnostics = diagnosticsEnabled
        ? buildDiagnostics(
            analyzed,
            predicateSpecs,
            skipPushdown || !shouldAllowPushdown ? [] : pushdownSpecs,
            skipPushdown || !shouldAllowPushdown ? predicateSpecs : residualSpecs,
            merges,
            orderChanges,
            false
        )
        : undefined;

    if (diagnosticsEnabled && diagnostics) {
        logPlanner({ source: "execution", type: "pushdown", planId: input.id, data: diagnostics.pushdown });
        logPlanner({ source: "execution", type: "final", planId: input.id, data: diagnostics.final });
    }

    return {
        cache: input.cache,
        diagnostics,
        fuzzyConfig: input.fuzzyConfig,
        id: input.id,
        planning,
        predicateFn,
        predicates,
        predicateSpecs,
        pushdownOrders: input.pushdownOrders,
        pushdownPredicates: finalPushdownPredicates,
        pushdownSafe: pushdownSafe && !skipPushdown && shouldAllowPushdown,
        residualPredicateFn,
        residualPredicates,
        searchFilters: input.searchFilters,
        strictSearch: input.strictSearch,
        taggerConfig: input.taggerConfig,
    };
}

function summarizePlanning<T extends Record<string, unknown>>(
    specs: Array<PredicateSpec<T>>,
    pushdownCount: number,
    residualCount: number,
    alwaysFalse: boolean
): PlanningMeta {
    if (alwaysFalse) {
        return {
            alwaysFalse: true,
            estimatedCost: 0,
            estimatedSelectivity: 0,
            predicateCount: 0,
            pushdownCount: 0,
            reorderable: true,
            residualCount: 0,
        };
    }
    let estimatedCost = 0;
    let estimatedSelectivity = 1;
    let reorderable = true;
    for (let i = 0; i < specs.length; i++) {
        const spec = specs[i]!;
        const estimate = estimatePredicate(spec);
        estimatedCost += estimate.cost;
        estimatedSelectivity *= estimate.selectivity;
        if (!spec.reorderable) {reorderable = false;}
    }
    return {
        alwaysFalse,
        estimatedCost,
        estimatedSelectivity: clamp(estimatedSelectivity, 0, 1),
        predicateCount: specs.length,
        pushdownCount,
        reorderable,
        residualCount,
    };
}

function buildDiagnostics<T extends Record<string, unknown>>(
    inputSpecs: Array<AnalyzedPredicate<T>>,
    finalSpecs: Array<PredicateSpec<T>>,
    pushdownSpecs: Array<PredicateSpec<T>>,
    residualSpecs: Array<PredicateSpec<T>>,
    merges: Array<{ reason: string; removed: Array<string>; result: Array<string> }>,
    order: Array<{ after: Array<string>; before: Array<string>; reason: string }>,
    alwaysFalse: boolean
): PlannerDiagnostics {
    const input = inputSpecs.map((spec) => toDiagnostic(spec, spec.cost, spec.selectivity));
    const final = finalSpecs.map((spec) => {
        const estimate = estimatePredicate(spec);
        return toDiagnostic(spec, estimate.cost, estimate.selectivity);
    });
    const pushdownPredicates = pushdownSpecs.map((spec) => {
        const estimate = estimatePredicate(spec);
        return toDiagnostic(spec, estimate.cost, estimate.selectivity);
    });
    const residualPredicates = residualSpecs.map((spec) => {
        const estimate = estimatePredicate(spec);
        return toDiagnostic(spec, estimate.cost, estimate.selectivity);
    });
    return {
        final: {
            alwaysFalse,
            predicates: final,
            pushdownPredicates,
            residualPredicates,
        },
        input,
        merges,
        order,
        pushdown: {
            applied: pushdownSpecs.map((spec) => spec.id),
            candidates: finalSpecs.filter((spec) => spec.pushdown).map((spec) => spec.id),
            full: residualSpecs.length === 0,
            residual: residualSpecs.map((spec) => spec.id),
        },
    };
}

function stripAnalysis<T>(spec: AnalyzedPredicate<T>): PredicateSpec<T> {
    const { cost, selectivity, ...rest } = spec;
    return rest;
}

function mergeSegment<T extends Record<string, unknown>>(
    specs: Array<AnalyzedPredicate<T>>,
    merges: Array<{ reason: string; removed: Array<string>; result: Array<string> }>
): MergeResult<T> {
    const mergeable: Array<AnalyzedPredicate<T>> = [];
    const other: Array<PredicateSpec<T>> = [];

    for (let i = 0; i < specs.length; i++) {
        const spec = specs[i]!;
        if (spec.constantValue === false) {
            return { alwaysFalse: true, merges, specs: [] };
        }
        if (spec.constantValue === true) {
            merges.push({ reason: "constant:true", removed: [spec.id], result: [] });
            continue;
        }
        if (isMergeableOp(spec.op) && spec.field && spec.accessors) {
            mergeable.push(spec);
            continue;
        }
        other.push(spec);
    }

    const fieldStates = new Map<string, FieldMergeState<T>>();
    const unmerged: Array<PredicateSpec<T>> = [];

    for (let i = 0; i < mergeable.length; i++) {
        const spec = mergeable[i]!;
        const field = spec.field!;
        const state = fieldStates.get(field) ?? {
            accessors: spec.accessors,
            sourceIds: [],
            template: spec,
        };
        state.sourceIds.push(spec.id);

        if (spec.op === "eq") {
            const value = spec.value as ResolveValue;
            if (state.eq !== undefined) {
                if (state.eq !== value) {
                    merges.push({ reason: "eq:conflict", removed: [...state.sourceIds], result: [] });
                    return { alwaysFalse: true, merges, specs: [] };
                }
                merges.push({ reason: "eq:duplicate", removed: [spec.id], result: [state.template.id] });
                continue;
            }
            state.eq = value;
            fieldStates.set(field, state);
            continue;
        }

        if (spec.op === "ne") {
            const value = spec.value as ResolveValue;
            const next = state.notInSet ?? new Set<ResolveValue>();
            next.add(value);
            state.notInSet = next;
            fieldStates.set(field, state);
            continue;
        }

        if (spec.op === "in") {
            const incoming = spec.valueSet ? new Set(spec.valueSet) : new Set(Array.isArray(spec.value) ? spec.value as Array<ResolveValue> : []);
            if (incoming.size === 0) {
                merges.push({ reason: "in:empty", removed: [...state.sourceIds], result: [] });
                return { alwaysFalse: true, merges, specs: [] };
            }
            if (state.eq !== undefined) {
                if (!incoming.has(state.eq)) {
                    merges.push({ reason: "in:eq:conflict", removed: [...state.sourceIds], result: [] });
                    return { alwaysFalse: true, merges, specs: [] };
                }
                merges.push({ reason: "in:eq:redundant", removed: [spec.id], result: [state.template.id] });
                continue;
            }
            if (state.inSet) {
                const intersection = new Set<ResolveValue>();
                for (const value of state.inSet) {
                    if (incoming.has(value)) {intersection.add(value);}
                }
                if (intersection.size === 0) {
                    merges.push({ reason: "in:intersection-empty", removed: [...state.sourceIds], result: [] });
                    return { alwaysFalse: true, merges, specs: [] };
                }
                state.inSet = intersection;
            } else {
                state.inSet = incoming;
            }
            fieldStates.set(field, state);
            continue;
        }

        if (spec.op === "notIn") {
            const incoming = spec.valueSet ? new Set(spec.valueSet) : new Set(Array.isArray(spec.value) ? spec.value as Array<ResolveValue> : []);
            const merged = state.notInSet ?? new Set<ResolveValue>();
            for (const value of incoming) {merged.add(value);}
            state.notInSet = merged;
            fieldStates.set(field, state);
            continue;
        }

        if (isRangeOp(spec.op)) {
            const rangeResult = mergeRangeState(state.range, spec.op, spec.value);
            if (!rangeResult) {
                unmerged.push(spec);
                continue;
            }
            if (rangeResult.conflict) {
                merges.push({ reason: "range:conflict", removed: [...state.sourceIds], result: [] });
                return { alwaysFalse: true, merges, specs: [] };
            }
            state.range = rangeResult.range;
            fieldStates.set(field, state);
            continue;
        }

        unmerged.push(spec);
    }

    const mergedSpecs: Array<PredicateSpec<T>> = [];
    for (const [field, state] of fieldStates.entries()) {
        const { accessors, eq, inSet, notInSet, range, template } = state;

        if (!accessors) {
            if (template) {mergedSpecs.push(template);}
            continue;
        }

        if (eq !== undefined) {
            if (notInSet && notInSet.has(eq)) {
                merges.push({ reason: "eq:notIn:conflict", removed: state.sourceIds, result: [] });
                return { alwaysFalse: true, merges, specs: [] };
            }
            if (range && !rangeAllows(range, eq)) {
                merges.push({ reason: "eq:range:conflict", removed: state.sourceIds, result: [] });
                return { alwaysFalse: true, merges, specs: [] };
            }
            const predicate = (item: T) => accessors.some(item as ResolveObject, (c) => c === eq);
            const spec = createMergedSpec(template, {
                field,
                op: "eq",
                predicate,
                pushdown: { field, op: "eq", value: eq },
                value: eq,
            });
            mergedSpecs.push(spec);
            continue;
        }

        let nextInSet = inSet;
        if (nextInSet && notInSet) {
            for (const value of notInSet) {nextInSet.delete(value);}
            if (nextInSet.size === 0) {
                merges.push({ reason: "in:notIn:conflict", removed: state.sourceIds, result: [] });
                return { alwaysFalse: true, merges, specs: [] };
            }
        }

        if (range && nextInSet) {
            const filtered = new Set<ResolveValue>();
            for (const value of nextInSet) {
                if (rangeAllows(range, value)) {filtered.add(value);}
            }
            nextInSet = filtered;
            if (nextInSet.size === 0) {
                merges.push({ reason: "in:range:conflict", removed: state.sourceIds, result: [] });
                return { alwaysFalse: true, merges, specs: [] };
            }
            if (nextInSet.size === 1) {
                const [value] = nextInSet;
                const predicate = (item: T) => accessors.some(item as ResolveObject, (c) => c === value);
                const spec = createMergedSpec(template, {
                    field,
                    op: "eq",
                    predicate,
                    pushdown: { field, op: "eq", value },
                    value,
                });
                mergedSpecs.push(spec);
                continue;
            }
            const valueSet = nextInSet;
            const predicate = (item: T) => accessors.some(item as ResolveObject, (c) => valueSet.has(c));
            const spec = createMergedSpec(template, {
                field,
                op: "in",
                predicate,
                pushdown: { field, op: "in", value: [...valueSet] },
                value: [...valueSet],
                valueSet,
            });
            mergedSpecs.push(spec);
            if (notInSet) {
                const specNotIn = createNotInSpec(template, field, accessors, notInSet);
                mergedSpecs.push(specNotIn);
            }
            continue;
        }

        if (nextInSet) {
            if (nextInSet.size === 1) {
                const [value] = nextInSet;
                const predicate = (item: T) => accessors.some(item as ResolveObject, (c) => c === value);
                const spec = createMergedSpec(template, {
                    field,
                    op: "eq",
                    predicate,
                    pushdown: { field, op: "eq", value },
                    value,
                });
                mergedSpecs.push(spec);
            } else {
                const valueSet = nextInSet;
                const predicate = (item: T) => accessors.some(item as ResolveObject, (c) => valueSet.has(c));
                const spec = createMergedSpec(template, {
                    field,
                    op: "in",
                    predicate,
                    pushdown: { field, op: "in", value: [...valueSet] },
                    value: [...valueSet],
                    valueSet,
                });
                mergedSpecs.push(spec);
            }
            if (notInSet) {
                mergedSpecs.push(createNotInSpec(template, field, accessors, notInSet));
            }
            continue;
        }

        if (range) {
            const rangeSpecs = createRangeSpecs(template, field, accessors, range);
            mergedSpecs.push(...rangeSpecs);
            if (notInSet) {
                mergedSpecs.push(createNotInSpec(template, field, accessors, notInSet));
            }
            continue;
        }

        if (notInSet) {
            mergedSpecs.push(createNotInSpec(template, field, accessors, notInSet));
        } else if (template) {
            mergedSpecs.push(template);
        }
    }

    for (let i = 0; i < unmerged.length; i++) {
        mergedSpecs.push(unmerged[i]!);
    }

    for (let i = 0; i < other.length; i++) {
        mergedSpecs.push(other[i]!);
    }

    return { alwaysFalse: false, merges, specs: mergedSpecs };
}

function createNotInSpec<T extends Record<string, unknown>>(
    template: PredicateSpec<T>,
    field: string,
    accessors: PredicateSpec<T>["accessors"],
    values: Set<ResolveValue>
): PredicateSpec<T> {
    const valueSet = new Set(values);
    if (!accessors) {return template;}
    if (valueSet.size === 1) {
        const [value] = valueSet;
        const predicate = (item: T) => !accessors.some(item as ResolveObject, (c) => c === value);
        return createMergedSpec(template, {
            field,
            op: "ne",
            predicate,
            pushdown: { field, op: "ne", value },
            value,
        });
    }
    const predicate = (item: T) => !accessors.some(item as ResolveObject, (c) => valueSet.has(c));
    return createMergedSpec(template, {
        field,
        op: "notIn",
        predicate,
        pushdown: { field, op: "notIn", value: [...valueSet] },
        value: [...valueSet],
        valueSet,
    });
}

function createRangeSpecs<T extends Record<string, unknown>>(
    template: PredicateSpec<T>,
    field: string,
    accessors: PredicateSpec<T>["accessors"],
    range: FieldRangeState
): Array<PredicateSpec<T>> {
    if (!accessors) {return [template];}
    if (range.min && range.max) {
        const min = range.min.value as ResolveValue;
        const max = range.max.value as ResolveValue;
        const predicate = createBetweenPredicate(min, max);
        return [createMergedSpec(template, {
            field,
            op: "between",
            predicate: (item: T) => accessors.some(item as ResolveObject, predicate),
            pushdown: { field, op: "between", value: { max, min } },
            value: { max, min },
        })];
    }
    if (range.min) {
        const value = range.min.value as ResolveValue;
        const op: PredicateOp = range.min.inclusive ? "gte" : "gt";
        const predicate = createComparePredicate(value, op === "gt" ? "gt" : "gte");
        return [createMergedSpec(template, {
            field,
            op,
            predicate: (item: T) => accessors.some(item as ResolveObject, predicate),
            pushdown: { field, op, value },
            value,
        })];
    }
    if (range.max) {
        const value = range.max.value as ResolveValue;
        const op: PredicateOp = range.max.inclusive ? "lte" : "lt";
        const predicate = createComparePredicate(value, op === "lt" ? "lt" : "lte");
        return [createMergedSpec(template, {
            field,
            op,
            predicate: (item: T) => accessors.some(item as ResolveObject, predicate),
            pushdown: { field, op, value },
            value,
        })];
    }
    return [template];
}

function createMergedSpec<T extends Record<string, unknown>>(
    template: PredicateSpec<T>,
    updates: Partial<PredicateSpec<T>> & { predicate: Predicate<T>; op: PredicateOp }
): PredicateSpec<T> {
    return {
        ...template,
        ...updates,
        kind: "builtin",
        reorderable: true,
    };
}

function mergeRangeState(
    existing: FieldRangeState | undefined,
    op: PredicateOp,
    value: unknown
): { conflict: boolean; range: FieldRangeState } | null {
    const range = existing ? { ...existing } : {};
    if (op === "between") {
        if (!value || typeof value !== "object") {return null;}
        const min = (value as { min?: unknown }).min;
        const max = (value as { max?: unknown }).max;
        const minType = comparableType(min);
        const maxType = comparableType(max);
        if (!minType || !maxType || minType !== maxType) {return null;}
        range.type = minType;
        range.min = reduceMin(range.min, { inclusive: true, value: min as number | string | bigint });
        range.max = reduceMax(range.max, { inclusive: true, value: max as number | string | bigint });
        if (hasRangeConflict(range)) {return { conflict: true, range };}
        return { conflict: false, range };
    }

    if (op === "gt" || op === "gte") {
        const type = comparableType(value);
        if (!type) {return null;}
        if (range.type && range.type !== type) {return null;}
        range.type = type;
        const bound: RangeBound = { inclusive: op === "gte", value: value as number | string | bigint };
        range.min = reduceMin(range.min, bound);
        if (hasRangeConflict(range)) {return { conflict: true, range };}
        return { conflict: false, range };
    }

    if (op === "lt" || op === "lte") {
        const type = comparableType(value);
        if (!type) {return null;}
        if (range.type && range.type !== type) {return null;}
        range.type = type;
        const bound: RangeBound = { inclusive: op === "lte", value: value as number | string | bigint };
        range.max = reduceMax(range.max, bound);
        if (hasRangeConflict(range)) {return { conflict: true, range };}
        return { conflict: false, range };
    }

    return null;
}

function reduceMin(current: RangeBound | undefined, incoming: RangeBound): RangeBound {
    if (!current) {return incoming;}
    const diff = compareValues(current.value, incoming.value);
    if (diff < 0) {return incoming;}
    if (diff > 0) {return current;}
    return { inclusive: current.inclusive && incoming.inclusive, value: current.value };
}

function reduceMax(current: RangeBound | undefined, incoming: RangeBound): RangeBound {
    if (!current) {return incoming;}
    const diff = compareValues(current.value, incoming.value);
    if (diff > 0) {return incoming;}
    if (diff < 0) {return current;}
    return { inclusive: current.inclusive && incoming.inclusive, value: current.value };
}

function hasRangeConflict(range: FieldRangeState): boolean {
    if (!range.min || !range.max) {return false;}
    const diff = compareValues(range.min.value, range.max.value);
    if (diff < 0) {return false;}
    if (diff > 0) {return true;}
    return !(range.min.inclusive && range.max.inclusive);
}

function rangeAllows(range: FieldRangeState, value: ResolveValue): boolean {
    const type = comparableType(value);
    if (!type || (range.type && type !== range.type)) {return false;}
    if (range.min) {
        const diff = compareValues(value as number | string | bigint, range.min.value);
        if (diff < 0) {return false;}
        if (diff === 0 && !range.min.inclusive) {return false;}
    }
    if (range.max) {
        const diff = compareValues(value as number | string | bigint, range.max.value);
        if (diff > 0) {return false;}
        if (diff === 0 && !range.max.inclusive) {return false;}
    }
    return true;
}

function compareValues(a: number | string | bigint, b: number | string | bigint): number {
    if (typeof a === "number" && typeof b === "number") {return a - b;}
    if (typeof a === "string" && typeof b === "string") {return a < b ? -1 : (a > b ? 1 : 0);}
    if (typeof a === "bigint" && typeof b === "bigint") {return a < b ? -1 : (a > b ? 1 : 0);}
    return 0;
}

function comparableType(value: unknown): "number" | "string" | "bigint" | null {
    if (typeof value === "number" && Number.isFinite(value)) {return "number";}
    if (typeof value === "string") {return "string";}
    if (typeof value === "bigint") {return "bigint";}
    return null;
}

function isMergeableOp(op: PredicateOp): boolean {
    return op === "eq" || op === "ne" || op === "in" || op === "notIn" || isRangeOp(op);
}

function isRangeOp(op: PredicateOp): boolean {
    return op === "gt" || op === "gte" || op === "lt" || op === "lte" || op === "between";
}

function estimatePredicate<T extends Record<string, unknown>>(
    spec: PredicateSpec<T>,
    hints?: IngressHints<T>
): { cost: number; selectivity: number } {
    if (spec.constantValue === false) {return { cost: 0, selectivity: 0 };}
    if (spec.constantValue === true) {return { cost: 0.1, selectivity: 1 };}

    let cost = 1;
    let selectivity = 0.5;
    const segments = spec.accessors?.segments.length ?? 1;
    const depthCost = segments > 1 ? Math.min(segments, 6) * 0.15 : 0;
    const uniqueKey = hints?.uniqueKey ? String(hints.uniqueKey) : null;
    const estimatedCount = hints?.estimatedCount ?? 0;

    switch (spec.op) {
        case "eq":
            cost = 0.8;
            selectivity = 0.1;
            if (uniqueKey && spec.field === uniqueKey) {
                const count = estimatedCount > 0 ? estimatedCount : 10_000;
                selectivity = clamp(1 / count, minSelectivity, 0.2);
            }
            break;
        case "ne":
            cost = 0.9;
            selectivity = 0.9;
            break;
        case "in":
            cost = 1.1;
            selectivity = 0.2;
            if (spec.valueSet && spec.valueSet.size > 0) {
                cost += Math.min(spec.valueSet.size, 64) * 0.02;
                if (estimatedCount > 0) {
                    selectivity = clamp(spec.valueSet.size / estimatedCount, minSelectivity, maxSelectivity);
                } else {
                    selectivity = clamp(spec.valueSet.size * 0.05, minSelectivity, 0.9);
                }
            }
            break;
        case "notIn":
            cost = 1.2;
            selectivity = 0.85;
            if (spec.valueSet && spec.valueSet.size > 0 && estimatedCount > 0) {
                const inSelect = clamp(spec.valueSet.size / estimatedCount, minSelectivity, maxSelectivity);
                selectivity = clamp(1 - inSelect, minSelectivity, maxSelectivity);
            }
            break;
        case "gt":
        case "gte":
        case "lt":
        case "lte":
            cost = 1.1;
            selectivity = 0.4;
            break;
        case "between":
            cost = 1.3;
            selectivity = 0.3;
            break;
        case "contains":
            cost = 1.8;
            selectivity = 0.35;
            break;
        case "startsWith":
            cost = 1.6;
            selectivity = 0.25;
            break;
        case "endsWith":
            cost = 1.7;
            selectivity = 0.35;
            break;
        case "matches":
            cost = 2.6;
            selectivity = 0.25;
            break;
        case "isNull":
            cost = 0.9;
            selectivity = 0.08;
            break;
        case "notNull":
            cost = 0.9;
            selectivity = 0.9;
            break;
        case "pathExists":
        case "pathExistsNullable":
            cost = 1.1;
            selectivity = 0.8;
            break;
        case "arraySome":
            cost = 3.2;
            selectivity = 0.55;
            break;
        case "arrayEvery":
            cost = 3.6;
            selectivity = 0.3;
            break;
        case "arrayNone":
            cost = 3.0;
            selectivity = 0.7;
            break;
        case "dateEquals":
            cost = 2.2;
            selectivity = 0.15;
            break;
        case "dateAfter":
        case "dateAfterOrEqual":
        case "dateBefore":
        case "dateBeforeOrEqual":
            cost = 2.4;
            selectivity = 0.35;
            break;
        case "dateBetween":
            cost = 2.6;
            selectivity = 0.25;
            break;
        case "groupAnd":
        case "groupOr":
        case "groupNot":
            cost = spec.groupInfo?.estimatedCost ?? 4.5;
            selectivity = spec.groupInfo?.estimatedSelectivity ?? 0.5;
            break;
        case "nested":
            cost = spec.groupInfo?.estimatedCost ?? 5.5;
            selectivity = spec.groupInfo?.estimatedSelectivity ?? 0.4;
            break;
        case "custom":
            cost = 6.5;
            selectivity = 0.5;
            break;
        case "const":
            cost = 0.1;
            selectivity = spec.constantValue ? 1 : 0;
            break;
        default:
            cost = 2;
            selectivity = 0.5;
            break;
    }

    if (spec.kind !== "builtin") {
        cost += 0.5;
    }
    cost += depthCost;
    return {
        cost: clamp(cost, 0, 50),
        selectivity: clamp(selectivity, minSelectivity, maxSelectivity),
    };
}

function toDiagnostic<T extends Record<string, unknown>>(
    spec: PredicateSpec<T>,
    cost: number,
    selectivity: number
): PredicateDiagnostic {
    return {
        cost,
        field: spec.field,
        id: spec.id,
        ignoreCase: spec.ignoreCase,
        op: spec.op,
        pushdown: !!spec.pushdown,
        reorderable: spec.reorderable,
        selectivity,
        value: spec.value,
    };
}

function compilePredicateFn<T>(predicates: Array<Predicate<T>>): (item: T) => boolean {
    const count = predicates.length;
    if (count === 0) {return () => true;}
    if (count === 1) {
        const p0 = predicates[0]!;
        return (item) => p0(item);
    }
    if (count === 2) {
        const p0 = predicates[0]!;
        const p1 = predicates[1]!;
        return (item) => p0(item) && p1(item);
    }
    if (count === 3) {
        const p0 = predicates[0]!;
        const p1 = predicates[1]!;
        const p2 = predicates[2]!;
        return (item) => p0(item) && p1(item) && p2(item);
    }
    return (item) => {
        for (let i = 0; i < count; i++) {
            if (!predicates[i]!(item)) {return false;}
        }
        return true;
    };
}

function clamp(value: number, min: number, max: number): number {
    if (value < min) {return min;}
    if (value > max) {return max;}
    return value;
}
