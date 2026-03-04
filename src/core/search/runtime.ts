import type { CacheState } from "@/core/shared/cache";
import { getPathAccessors } from "@/core/shared/cache";
import type { PathAccessors } from "@/core/shared/path";
import type { ResolveObject, ResolveValue, Predicate } from "@/types";
import type {
    AvailableTags,
    FuzzyConfig,
    FuzzyQueryInput,
    SearchInput,
    SearchQuery,
    SearchCapabilityState,
    TagFilter,
    TaggerConfig,
    SearchFilterState,
} from "@/types/search";

export type CompiledFuzzyConfig<T> = {
    customNormalize: boolean;
    fields: ReadonlyArray<{ accessors: PathAccessors; weight: number }>;
    minScore: number;
    normalize: (value: string) => string;
    order: "score" | "existing" | "scoreThenOrder";
    requireAll: boolean;
    strict: boolean;
} & { __type?: T };

export type CompiledTaggerConfig<T, Tags extends string> = {
    customNormalize: boolean;
    equalsRules: Array<TagRule<ResolveValue>>;
    inRules: Array<TagRule<Set<ResolveValue>>>;
    normalize: (value: string) => string;
    regexRules: Array<TagRule<RegExp>>;
    strict: boolean;
    stringRules: Array<TagStringRule>;
    indexOfTag: Map<Tags, number>;
    tags: ReadonlyArray<Tags>;
    grouped: CompiledTaggerGroups;
} & { __type?: T };

type TagRule<TValue> = {
    accessors: PathAccessors;
    tagIndex: number;
    value: TValue;
};

type TagStringRule = TagRule<string> & {
    op: "contains" | "startsWith" | "endsWith";
};

type CompiledTaggerGroup = {
    accessors: PathAccessors;
    equals: Array<{ tagIndex: number; value: ResolveValue }>;
    in: Array<{ tagIndex: number; set: Set<ResolveValue> }>;
    string: Array<{ tagIndex: number; op: "contains" | "startsWith" | "endsWith"; value: string }>;
    regex: Array<{ tagIndex: number; regex: RegExp }>;
};

type CompiledTaggerGroups = {
    order: Array<CompiledTaggerGroup>;
};

export type SearchResult<T> = {
    items: Array<T>;
    scores?: Array<number>;
    tagMasks?: Array<number>;
};

const defaultNormalize = (value: string) => value.toLowerCase();

function getNormalizeCache(cache: CacheState, normalize: (value: string) => string): Map<string, string> {
    const existing = cache.searchCache.normalizeCache.get(normalize);
    if (existing) {return existing;}
    const created = new Map<string, string>();
    cache.searchCache.normalizeCache.set(normalize, created);
    return created;
}

function normalizeValueWithStore(store: Map<string, string>, normalize: (value: string) => string, value: string): string {
    const cached = store.get(value);
    if (cached !== undefined) {return cached;}
    const normalized = normalize(value);
    store.set(value, normalized);
    return normalized;
}

function maskFromStringWithCache(maskCache: Map<string, number>, value: string): number {
    const cached = maskCache.get(value);
    if (cached !== undefined) {return cached;}
    let mask = 0;
    for (let i = 0; i < value.length; i++) {
        const code = value.charCodeAt(i);
        if (code !== undefined && code >= 97 && code <= 122) {
            mask |= 1 << (code - 97);
        }
    }
    maskCache.set(value, mask);
    return mask;
}

function queryMask(query: string): number {
    let mask = 0;
    for (let i = 0; i < query.length; i++) {
        const code = query.charCodeAt(i);
        if (code !== undefined && code >= 97 && code <= 122) {
            mask |= 1 << (code - 97);
        }
    }
    return mask;
}

function scoreSubsequence(candidate: string, query: string): number {
    let score = 0;
    let lastMatch = -1;
    let qi = 0;

    for (let i = 0; i < candidate.length && qi < query.length; i++) {
        const c = candidate.charCodeAt(i);
        const q = query.charCodeAt(qi);
        if (c === undefined || q === undefined || c !== q) {continue;}
        const isStart = i === 0;
        const prev = i > 0 ? candidate.charCodeAt(i - 1) : 0;
        const boundary = isStart || prev === 45 || prev === 95 || prev === 32 || (prev >= 48 && prev <= 57);
        const contiguous = lastMatch + 1 === i;
        score += 10;
        if (boundary) {score += 6;}
        if (contiguous) {score += 4;}
        lastMatch = i;
        qi++;
    }

    if (qi < query.length) {return 0;}
    return score;
}

function parseFuzzyInput(input?: FuzzyQueryInput): { minScore?: number; query: string; } | null {
    if (!input) {return null;}
    if (typeof input === "string") {return { query: input };}
    return { minScore: input.minScore, query: input.query };
}

export function compileFuzzyConfig<T>(cache: CacheState, config: FuzzyConfig<T>): CompiledFuzzyConfig<T> {
    const fields = config.fields.map((field) => ({
        accessors: getPathAccessors(cache, String(field.path)),
        weight: field.weight ?? 1,
    }));
    const normalize = config.normalize ?? defaultNormalize;
    return {
        customNormalize: config.normalize !== undefined,
        fields,
        minScore: config.minScore ?? 0,
        normalize,
        order: config.order ?? "score",
        requireAll: config.requireAll ?? true,
        strict: config.strict ?? true,
    };
}

export function compileTaggerConfig<T, Tags extends string>(
    cache: CacheState,
    config: TaggerConfig<T, Tags>
): CompiledTaggerConfig<T, Tags> {
    const normalize = config.normalize ?? defaultNormalize;
    const tags = [...config.tags];
    const indexOfTag = new Map<Tags, number>();
    for (let i = 0; i < tags.length; i++) {indexOfTag.set(tags[i]!, i);}

    const stringRules: CompiledTaggerConfig<T, Tags>["stringRules"] = [];
    const regexRules: CompiledTaggerConfig<T, Tags>["regexRules"] = [];
    const equalsRules: CompiledTaggerConfig<T, Tags>["equalsRules"] = [];
    const inRules: CompiledTaggerConfig<T, Tags>["inRules"] = [];

    for (const rule of config.rules) {
        const tagIndex = indexOfTag.get(rule.tag);
        if (tagIndex === undefined) {continue;}
        if ("equals" in rule) {
            equalsRules.push({ accessors: getPathAccessors(cache, String(rule.field)), tagIndex, value: rule.equals as ResolveValue });
            continue;
        }
        if ("in" in rule) {
            inRules.push({ accessors: getPathAccessors(cache, String(rule.field)), value: new Set(rule.in as Array<ResolveValue>), tagIndex });
            continue;
        }
        if ("contains" in rule) {
            stringRules.push({
                op: "contains",
                accessors: getPathAccessors(cache, String(rule.field)),
                tagIndex,
                value: normalize(rule.contains),
            });
            continue;
        }
        if ("startsWith" in rule) {
            stringRules.push({
                op: "startsWith",
                accessors: getPathAccessors(cache, String(rule.field)),
                tagIndex,
                value: normalize(rule.startsWith),
            });
            continue;
        }
        if ("endsWith" in rule) {
            stringRules.push({
                op: "endsWith",
                accessors: getPathAccessors(cache, String(rule.field)),
                tagIndex,
                value: normalize(rule.endsWith),
            });
            continue;
        }
        if ("matches" in rule) {
            regexRules.push({
                accessors: getPathAccessors(cache, String(rule.field)),
                tagIndex,
                value: new RegExp(rule.matches.source, rule.matches.flags.replaceAll(/[gy]/g, "")),
            });
        }
    }

    const grouped = new Map<PathAccessors, CompiledTaggerGroup>();
    const ensureGroup = (accessors: PathAccessors) => {
        const existing = grouped.get(accessors);
        if (existing) {return existing;}
        const created: CompiledTaggerGroup = {
            accessors,
            equals: [],
            in: [],
            string: [],
            regex: [],
        };
        grouped.set(accessors, created);
        return created;
    };

    for (let i = 0; i < equalsRules.length; i++) {
        const rule = equalsRules[i]!;
        ensureGroup(rule.accessors).equals.push({ tagIndex: rule.tagIndex, value: rule.value });
    }
    for (let i = 0; i < inRules.length; i++) {
        const rule = inRules[i]!;
        ensureGroup(rule.accessors).in.push({ tagIndex: rule.tagIndex, set: rule.value });
    }
    for (let i = 0; i < stringRules.length; i++) {
        const rule = stringRules[i]!;
        ensureGroup(rule.accessors).string.push({ tagIndex: rule.tagIndex, op: rule.op, value: rule.value });
    }
    for (let i = 0; i < regexRules.length; i++) {
        const rule = regexRules[i]!;
        ensureGroup(rule.accessors).regex.push({ tagIndex: rule.tagIndex, regex: rule.value });
    }

    return {
        customNormalize: config.normalize !== undefined,
        equalsRules,
        inRules,
        normalize,
        regexRules,
        strict: config.strict ?? true,
        stringRules,
        tags,
        indexOfTag,
        grouped: { order: [...grouped.values()] },
    };
}

function matchesTagFilter(mask: number, required: number, forbidden: number, any: number, notAny: number): boolean {
    if (forbidden && (mask & forbidden) !== 0) {return false;}
    if (notAny && (mask & notAny) !== 0) {return false;}
    if (required && (mask & required) !== required) {return false;}
    if (any && (mask & any) === 0) {return false;}
    return true;
}

function buildTagMask<Tags extends string>(
    indexOfTag: Map<Tags, number>,
    filter: TagFilter<Tags> | undefined
): { any: number; forbidden: number; notAny: number; required: number; } {
    if (!filter) {return { any: 0, forbidden: 0, notAny: 0, required: 0 };}
    const toMask = (input?: ReadonlyArray<Tags>) => {
        if (!input || input.length === 0) {return 0;}
        let mask = 0;
        for (let i = 0; i < input.length; i++) {
            const idx = indexOfTag.get(input[i]!);
            if (idx === undefined) {continue;}
            mask |= 1 << idx;
        }
        return mask;
    };
    return {
        any: toMask(filter.hasAny),
        forbidden: toMask(filter.not),
        notAny: toMask(filter.notAny),
        required: toMask(filter.has),
    };
}

function runTagger<T, Tags extends string>(
    cache: CacheState,
    config: CompiledTaggerConfig<T, Tags>,
    item: T
): number {
    let mask = 0;
    const object = item as ResolveObject;
    const normalizeCache = getNormalizeCache(cache, config.normalize);

    for (let i = 0; i < config.grouped.order.length; i++) {
        const group = config.grouped.order[i]!;
        group.accessors.forEach(object, (value: ResolveValue) => {
            for (let j = 0; j < group.equals.length; j++) {
                const rule = group.equals[j]!;
                if (value === rule.value) {mask |= 1 << rule.tagIndex;}
            }
            for (let j = 0; j < group.in.length; j++) {
                const rule = group.in[j]!;
                if (rule.set.has(value)) {mask |= 1 << rule.tagIndex;}
            }
            for (let j = 0; j < group.string.length; j++) {
                const rule = group.string[j]!;
                if (typeof value !== "string") {continue;}
                const normalized = normalizeValueWithStore(normalizeCache, config.normalize, value);
                if (rule.op === "contains" && normalized.includes(rule.value)) {
                    mask |= 1 << rule.tagIndex;
                } else if (rule.op === "startsWith" && normalized.startsWith(rule.value)) {
                    mask |= 1 << rule.tagIndex;
                } else if (rule.op === "endsWith" && normalized.endsWith(rule.value)) {
                    mask |= 1 << rule.tagIndex;
                }
            }
            for (let j = 0; j < group.regex.length; j++) {
                const rule = group.regex[j]!;
                if (typeof value !== "string") {continue;}
                if (rule.regex.test(value)) {mask |= 1 << rule.tagIndex;}
            }
        });
    }
    return mask;
}

function runFuzzy<T>(
    config: CompiledFuzzyConfig<T>,
    item: T,
    query: string,
    queryMaskValue: number,
    normalizeCache: Map<string, string>,
    maskCache: Map<string, number>
): number {
    let best = 0;
    const object = item as ResolveObject;
    for (let i = 0; i < config.fields.length; i++) {
        const field = config.fields[i]!;
        field.accessors.forEach(object, (value) => {
            if (typeof value !== "string") {return;}
            const normalized = normalizeValueWithStore(normalizeCache, config.normalize, value);
            if ((maskFromStringWithCache(maskCache, normalized) & queryMaskValue) !== queryMaskValue) {return;}
            const score = scoreSubsequence(normalized, query) * field.weight;
            if (score > best) {best = score;}
        });
    }
    return best;
}

export function resolveSearchQuery<C extends SearchCapabilityState>(input: SearchInput<C>): SearchFilterState {
    if (typeof input === "string") {return { fuzzy: input };}
    if (Array.isArray(input)) {return { tags: { hasAny: input as Array<string> } };}
    return input as SearchFilterState;
}

export function executeSearchPipeline<T, C extends SearchCapabilityState>(
    data: ReadonlyArray<T>,
    predicates: ReadonlyArray<Predicate<T>>,
    predicateFn: ((item: T) => boolean) | null,
    cache: CacheState,
    fuzzyConfig: CompiledFuzzyConfig<T> | null,
    taggerConfig: CompiledTaggerConfig<T, AvailableTags<C>> | null,
    filters: ReadonlyArray<SearchFilterState>,
    includeMetadata: boolean
): SearchResult<T> {
    const query: SearchQuery<string> = {};
    for (let i = 0; i < filters.length; i++) {
        const filter = filters[i]!;
        if (filter.fuzzy !== undefined) {query.fuzzy = filter.fuzzy;}
        if (filter.tags !== undefined) {query.tags = filter.tags;}
    }

    const fuzzyInput = parseFuzzyInput(query.fuzzy);
    const activeNormalize = fuzzyConfig?.normalize ?? defaultNormalize;
    const fuzzyQuery = fuzzyInput ? activeNormalize(fuzzyInput.query) : "";
    const fuzzyMask = fuzzyInput ? queryMask(fuzzyQuery) : 0;
    const fuzzyMinScore = fuzzyInput?.minScore ?? fuzzyConfig?.minScore ?? (fuzzyConfig?.requireAll ? 1 : 0);

    const tagMask = taggerConfig
        ? buildTagMask(taggerConfig.indexOfTag, query.tags)
        : { any: 0, forbidden: 0, notAny: 0, required: 0 };

    const results: Array<T> = [];
    const scores: Array<number> = [];
    const tagMasks: Array<number> = [];

    const hasPredicateFn = predicateFn !== null;
    const normalizeCache = fuzzyConfig ? getNormalizeCache(cache, fuzzyConfig.normalize) : null;
    const maskCache = cache.searchCache.maskCache;
    outer: for (let i = 0; i < data.length; i++) {
        const item = data[i]!;
        if (hasPredicateFn) {
            if (!predicateFn!(item)) {continue;}
        } else {
            for (let p = 0; p < predicates.length; p++) {
                if (!predicates[p]!(item)) {continue outer;}
            }
        }

        let score = 0;
        if (fuzzyInput && fuzzyConfig) {
            score = runFuzzy(fuzzyConfig, item, fuzzyQuery, fuzzyMask, normalizeCache!, maskCache);
            if (fuzzyConfig.requireAll && score <= 0) {continue;}
            if (score < fuzzyMinScore) {continue;}
        }

        let tagsMaskValue = 0;
        if (taggerConfig) {
            tagsMaskValue = runTagger(cache, taggerConfig, item);
            if (!matchesTagFilter(tagsMaskValue, tagMask.required, tagMask.forbidden, tagMask.any, tagMask.notAny)) {
                continue;
            }
        }

        results.push(item);
        if (includeMetadata) {
            scores.push(score);
            tagMasks.push(tagsMaskValue);
        }
    }

    if (fuzzyInput && fuzzyConfig && (fuzzyConfig.order === "score" || fuzzyConfig.order === "scoreThenOrder")) {
        const indices = new Array(results.length);
        for (let i = 0; i < indices.length; i++) {indices[i] = i;}
        indices.sort((a, b) => {
            const diff = (scores[b] ?? 0) - (scores[a] ?? 0);
            if (diff !== 0) {return diff;}
            return a - b;
        });
        const orderedItems: Array<T> = [];
        const orderedScores: Array<number> = [];
        const orderedMasks: Array<number> = [];
        for (let i = 0; i < indices.length; i++) {
            const idx = indices[i]!;
            orderedItems.push(results[idx]!);
            if (includeMetadata) {
                orderedScores.push(scores[idx]!);
                orderedMasks.push(tagMasks[idx]!);
            }
        }
        return includeMetadata
            ? { items: orderedItems, scores: orderedScores, tagMasks: orderedMasks }
            : { items: orderedItems };
    }

    return includeMetadata
        ? { items: results, scores, tagMasks }
        : { items: results };
}
