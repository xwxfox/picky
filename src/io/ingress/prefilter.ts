export type PrefilterMode = "auto" | "off";

export type PrefilterOp =
    | "eq"
    | "ne"
    | "in"
    | "notIn"
    | "gt"
    | "gte"
    | "lt"
    | "lte"
    | "isNull"
    | "notNull";

export type PrefilterType = "string" | "number" | "boolean" | "null";

export type PrefilterValue = string | number | boolean | null;

export type PrefilterPredicate = {
    field: string;
    op: PrefilterOp;
    segments?: ReadonlyArray<string>;
    type: PrefilterType;
    value?: PrefilterValue | ReadonlyArray<PrefilterValue>;
};

export type PrefilterPlan = {
    fields: ReadonlyArray<string>;
    key: string;
    predicates: ReadonlyArray<PrefilterPredicate>;
};

export type PrefilterStats = {
    checked: number;
    matched: number;
    parsed: number;
    skipped: number;
    unknown: number;
};

export type PrefilterStreamOptions = {
    prefilterProgram?: import("@/io/ingress/prefilter-c").PrefilterProgram | null;
    stats?: PrefilterStats;
    planId?: string;
    prefilter?: PrefilterPlan | null;
    prefilterMode?: PrefilterMode;
    timingParent?: number | null;
};
