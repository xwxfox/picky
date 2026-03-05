import { Engine, IngressEngine } from "../src";

type Event = {
    id: number;
    message: string;
    severity: "INFO" | "WARN" | "ERROR";
    type: string;
    user: {
        email: string;
    };
};

const data: Array<Event> = [
    {
        id: 1,
        message: "payment failed for invoice 933",
        severity: "ERROR",
        type: "PAYMENT_FAILED",
        user: { email: "ops@acme.io" },
    },
    {
        id: 2,
        message: "timeout while calling billing service",
        severity: "WARN",
        type: "SERVICE_TIMEOUT",
        user: { email: "eng@acme.io" },
    },
    {
        id: 3,
        message: "login ok",
        severity: "INFO",
        type: "AUTH_SUCCESS",
        user: { email: "bot@example.internal" },
    },
];

const ingress = IngressEngine.from(data);

const tagged = Engine.from(ingress)
    .configureTagger({
        rules: [
            { equals: "PAYMENT_FAILED", field: "type", tag: "billing" },
            { contains: "timeout", field: "message", tag: "infra" },
            { field: "severity", in: ["WARN", "ERROR"], tag: "needs_followup" },
            { field: "user.email", matches: /@example\.internal$/, tag: "internal" },
        ],
        tags: ["billing", "infra", "needs_followup", "internal"],
    })
    .out()
    .tags({ hasAny: ["billing", "infra"], notAny: ["internal"] })
    .resultWithMetadata();

console.dir(tagged, { depth: null });
