
import {
  Engine,
  IngressEngine,
  Schema,
  setPlannerDiagnostics,
  setPlannerLogger,
  setPlannerTiming,
  setPlannerDeepMetrics,
  setTimingSource,
  fileSource,
  formatRunReport
} from "@/index.ts";
import { resolve } from "path";

type LargeItem = {
  active: boolean;
  created: Date | string;
  flags: Array<string>;
  id: number;
  Logs: Array<{ tags: Array<string>; type: string; when: Date | string }>;
  meta: {
    owner: {
      name: string;
      nickname?: string | null;
    };
  };
  name: string | null;
  score: number;
};
const randomInt = (min: number, max: number) => Math.floor(Math.random() * (max - min)) + min;
const ownerNames = ["Alice", "Bob", "Cara", "Dee", "Eli"];
const getRandomNamesArray = (): Array<string> => {
  const upper = randomInt(1, ownerNames.length + 1);
  const out: Array<string> = [];
  for (let i = 0; i < upper; i++) {
    const random = randomInt(0, ownerNames.length)
    out.push(ownerNames[random]!);
  }
  return out;
};

export const run = async () => {
  const events: Array<{ source: string; type: string; planId?: string; data?: unknown; metrics?: { counts?: Record<string, number>; durationsMs?: Record<string, number>; extras?: Record<string, unknown> }; timing?: { durationMs: number; label: string; startMs: number; endMs: number; spanId: number; parentId?: number } }> = [];
  setPlannerLogger((evt) => {
    events.push(evt);
    if (evt.type === "timing") { return; }
    if (evt.type === "metrics") { return; }
    console.log(`[${evt.source}.${evt.type}]<ID: ${evt.planId ?? "N/A"}>:`);
    console.dir(evt.data, { depth: null });

    /*
            if (evt.event === "planner:final") {
        console.log("plan", evt.planId);
        console.dir(evt.data, { depth: null });
    }*/
  }, { includeDiagnostics: true, includeTiming: true, includeDeepMetrics: true });

  setPlannerDiagnostics(true);
  setPlannerTiming(true);
  setPlannerDeepMetrics(true);
  setTimingSource("performance");

  const start = performance.now();
  const source = fileSource<LargeItem>(resolve(import.meta.dir, "../perf/bench-data/large-items/150000.json"),
    {
      format: "json",
      hints: { estimatedCount: 150000 },
      schema: Schema.inline<LargeItem>(),
      prefilterMode: "auto"
    });
  const input = IngressEngine.fromSource(source)
  /*
 
   const data = await Bun.file(
       resolve(import.meta.dir, "../perf/bench-data/large-items/150000.json")
   ).json() as Array<LargeItem>;
 const dataLoadTime = performance.now() - start;
   const input = IngressEngine.from(data) // lsp: const input: IngressEngine<LargeItem>
   */
  const filter = Engine.from(input);
  const res = await filter.in("meta.owner.name", getRandomNamesArray()) /* lsp: const res: any */

    .greaterThan("score", 9)
    .valueNotNull("name")
    .dateBetween("created", "2026-01-01", "2026-02-31")
    .configureTagger({
      tags: ["falgs:green"],
      rules: [
        {
          field: "flags",
          in: ["green"],
          tag: "falgs:green"
        }
      ]
    })
    .out()
    .orderByDate("created", { direction: "desc" })
    .result();
  const end = performance.now();
  console.log(`Query took ${(end - start)}ms - sizes: input=${input.length}, res=${res.length}`);
  console.log("\n");
  console.log(formatRunReport(events));
};


if (import.meta.main) {
  await run();
}


/*
$ bun run examples/debugging.ts 
[execution.input]<ID: plan_4__t0>:
{
  predicates: [
    {
      cost: 1.57,
      field: "meta.owner.name",
      id: "p1",
      ignoreCase: undefined,
      op: "in",
      pushdown: true,
      reorderable: true,
      selectivity: 0.001,
      value: [ "Alice" ],
    }, {
      cost: 1.1,
      field: "score",
      id: "p2",
      ignoreCase: undefined,
      op: "gt",
      pushdown: true,
      reorderable: true,
      selectivity: 0.4,
      value: 9,
    }, {
      cost: 0.9,
      field: "name",
      id: "p3",
      ignoreCase: undefined,
      op: "notNull",
      pushdown: true,
      reorderable: true,
      selectivity: 0.9,
      value: undefined,
    }, {
      cost: 2.6,
      field: "created",
      id: "p4",
      ignoreCase: undefined,
      op: "dateBetween",
      pushdown: false,
      reorderable: true,
      selectivity: 0.25,
      value: {
        max: "2026-02-31",
        min: "2026-01-01",
      },
    }
  ],
}
[execution.merge]<ID: plan_4__t0>:
{
  merges: [],
}
[execution.order]<ID: plan_4__t0>:
{
  order: [
    {
      before: [ "p1", "p2", "p3", "p4" ],
      after: [ "p3", "p2", "p1", "p4" ],
      reason: "cost/selectivity",
    }
  ],
}
[execution.pushdown]<ID: plan_4__t0>:
{
  applied: [],
  candidates: [ "p3", "p2", "p1" ],
  full: false,
  residual: [ "p3", "p2", "p1", "p4" ],
}
[execution.final]<ID: plan_4__t0>:
{
  alwaysFalse: false,
  predicates: [
    {
      cost: 0.9,
      field: "name",
      id: "p3",
      ignoreCase: undefined,
      op: "notNull",
      pushdown: true,
      reorderable: true,
      selectivity: 0.9,
      value: undefined,
    }, {
      cost: 1.1,
      field: "score",
      id: "p2",
      ignoreCase: undefined,
      op: "gt",
      pushdown: true,
      reorderable: true,
      selectivity: 0.4,
      value: 9,
    }, {
      cost: 1.25,
      field: "meta.owner.name",
      id: "p1",
      ignoreCase: undefined,
      op: "eq",
      pushdown: true,
      reorderable: true,
      selectivity: 0.1,
      value: "Alice",
    }, {
      cost: 2.6,
      field: "created",
      id: "p4",
      ignoreCase: undefined,
      op: "dateBetween",
      pushdown: false,
      reorderable: true,
      selectivity: 0.25,
      value: {
        max: "2026-02-31",
        min: "2026-01-01",
      },
    }
  ],
  pushdownPredicates: [],
  residualPredicates: [
    {
      cost: 0.9,
      field: "name",
      id: "p3",
      ignoreCase: undefined,
      op: "notNull",
      pushdown: true,
      reorderable: true,
      selectivity: 0.9,
      value: undefined,
    }, {
      cost: 1.1,
      field: "score",
      id: "p2",
      ignoreCase: undefined,
      op: "gt",
      pushdown: true,
      reorderable: true,
      selectivity: 0.4,
      value: 9,
    }, {
      cost: 1.25,
      field: "meta.owner.name",
      id: "p1",
      ignoreCase: undefined,
      op: "eq",
      pushdown: true,
      reorderable: true,
      selectivity: 0.1,
      value: "Alice",
    }, {
      cost: 2.6,
      field: "created",
      id: "p4",
      ignoreCase: undefined,
      op: "dateBetween",
      pushdown: false,
      reorderable: true,
      selectivity: 0.25,
      value: {
        max: "2026-02-31",
        min: "2026-01-01",
      },
    }
  ],
}
[egress.input]<ID: plan_4__t0>:
{
  hasSearch: false,
  orders: 1,
  limit: null,
  offset: 0,
}
[egress.pushdown]<ID: plan_4__t0>:
{
  applied: false,
  eligible: false,
  reason: {
    hasResidual: true,
    hasSearch: false,
    hasSearchFilters: false,
    ingressSupportsPushdown: false,
    offsetWithoutLimit: false,
  },
}
[ingress.input]<ID: ingress>:
{
  capabilities: {
    count: false,
    filter: false,
    group: false,
    order: false,
    paginate: false,
    search: false,
  },
  hints: {
    estimatedCount: 150000,
  },
  requiresGrouping: false,
  requiresOrdering: true,
  requiresSearch: false,
}
[ingress.final]<ID: ingress>:
{
  plan: {
    strategy: "eager",
  },
  reason: "ordering/grouping",
  supported: {
    group: false,
    order: false,
  },
}
[egress.final]<ID: plan_4__t0>:
{
  mode: "async",
  path: "local",
  hasOrders: true,
  hasSearch: false,
  residualPredicates: 1,
  resultCount: 22164,
}
Query took 398.016342ms - sizes: input=150000, res=22164


Run Report [plan_4__t0]
Summary
- duration: 392.53ms
- output: 22164
- mode: async
- path: local
- streaming: false
- search: false
- orders: true

Timing Hotspots (ms)
- run.execute: total=392.53 avg=392.53 count=1
- egress.executeAsync: total=391.99 avg=391.99 count=1
- execution.executeAsync: total=366.19 avg=366.19 count=1
- execution.load.materialize: total=296.28 avg=296.28 count=1
- egress.finalizeOrder: total=24.46 avg=24.46 count=1
- execution.compilePlan: total=3.13 avg=3.13 count=1
- execution.optimizePlan: total=2.69 avg=2.69 count=1
- execution.mergePass: total=0.92 avg=0.92 count=1
- execution.orderPass: total=0.09 avg=0.09 count=1
- execution.pushdownSplit: total=0.09 avg=0.09 count=1

Predicate Hotspots (ms)
- notNull: total=13.53 avg=0.00009 count=150000
- eq: total=11.35 avg=0.00010 count=114961
- gt: total=7.08 avg=0.00005 count=145161
- dateBetween: total=4.77 avg=0.00021 count=23006

Trace
- source.create: 0.00ms [ingress]
- builder.create: 0.00ms [engine]
- egress.start: 0.00ms [egress]
- execution.compilePlan: 3.13ms [plan_4__t0]
  - execution.optimizePlan: 2.69ms [plan_4__t0]
    - execution.mergePass: 0.92ms [plan_4__t0]
    - execution.orderPass: 0.09ms [plan_4__t0]
    - execution.pushdownSplit: 0.09ms [plan_4__t0]
- run.execute: 392.53ms [plan_4__t0]
  - egress.executeResult: 0.05ms [plan_4__t0]
    - egress.execute.start: 0.00ms [plan_4__t0]
    - egress.mergeFilters: 0.00ms [plan_4__t0]
  - egress.executeAsync: 391.99ms [plan_4__t0]
    - egress.execute.start: 0.00ms [plan_4__t0]
    - execution.executeAsync: 366.19ms [plan_4__t0]
      - execution.execute.start: 0.00ms [plan_4__t0]
      - ingress.planIngress: 0.08ms [ingress]
      - ingress.plan.start: 0.00ms [ingress]
      - execution.load.materialize: 296.28ms [plan_4__t0]
      - execution.execute.end: 0.00ms [plan_4__t0]
    - egress.finalizeOrder: 24.46ms [plan_4__t0]
    - egress.result.final: 0.00ms [plan_4__t0]

Predicate Metrics
- notNull: count=150000 total=13.53ms avg=0.00009ms
- gt: count=145161 total=7.08ms avg=0.00005ms
- eq: count=114961 total=11.35ms avg=0.00010ms
- dateBetween: count=23006 total=4.77ms avg=0.00021ms

Plan Predicate Counts
- notNull: 1
- gt: 1
- eq: 1
- dateBetween: 1

Decisions
- execution [plan_4__t0]: {"costByOp":{"in":1.57,"gt":1.1,"notNull":0.9,"dateBetween":2.6},"mergeCount":0,"predicateCountsByKind":{"builtin":4},"pushdownCount":3,"residualCount":1}
- ingress [ingress]: {"reason":"ordering/grouping","strategy":"eager","supported":{"group":false,"order":false}}
- execution [plan_4__t0]: {"phase":"executeAsync"}
- egress [plan_4__t0]: {"path":"local","hasOrders":true,"hasSearch":false,"limit":null,"offset":0}

*/