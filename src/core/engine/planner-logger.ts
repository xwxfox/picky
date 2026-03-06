export type ExecutionEventType = "input" | "merge" | "order" | "pushdown" | "final";
export type IngressEventType = "input" | "final";
export type EgressEventType = "input" | "pushdown" | "final";

export type ExecutionLogEvent = {
    source: "execution";
    type: ExecutionEventType;
    planId: string;
    data: unknown;
};

export type IngressLogEvent = {
    source: "ingress";
    type: IngressEventType;
    planId: string;
    data: unknown;
};

export type EgressLogEvent = {
    source: "egress";
    type: EgressEventType;
    planId: string;
    data: unknown;
};

export type MiscLogEvent = {
    source: "misc";
    type: string;
    planId?: string;
    data: unknown;
};

export type PlannerLogEvent =
    | ExecutionLogEvent
    | IngressLogEvent
    | EgressLogEvent
    | MiscLogEvent;

export type PlannerLogger = (event: PlannerLogEvent) => void;

let plannerLogger: PlannerLogger | null = null;
let plannerDiagnosticsEnabled = false;

export function setPlannerLogger(
    logger: PlannerLogger | null,
    options?: { includeDiagnostics?: boolean }
): void {
    plannerLogger = logger ?? null;
    if (options?.includeDiagnostics !== undefined) {
        plannerDiagnosticsEnabled = options.includeDiagnostics;
    }
}

export function setPlannerDiagnostics(enabled: boolean): void {
    plannerDiagnosticsEnabled = enabled;
}

export function getPlannerLogger(): PlannerLogger | null {
    return plannerLogger;
}

export function getPlannerDiagnosticsEnabled(): boolean {
    return plannerDiagnosticsEnabled || plannerLogger !== null;
}

export function logPlanner(event: PlannerLogEvent): void {
    if (!plannerLogger) {return;}
    plannerLogger(event);
}
