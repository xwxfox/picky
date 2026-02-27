# picky

smol lil filter engine for arrays of *structured* objects.

note: this is built for bun + typescript, zero runtime deps.

> it is actually quite neat but i hate writing docs so go dig in the code u fkn twink

## what it does
- build chainable predicates for nested data
- understands dates (Date, ISO strings, timestamps)
- supports arrays once per path (by design)
- has a small cache for hot paths + date parsing

## owo
```ts
import { FilterEngine } from ".";

const result = FilterEngine.from(data)
  .equals("meta.owner.name", "Alice")
  .dateBetween("created", "2024-01-01", "2024-02-01")
  .nested("Logs", q => q.equals("type", "CREDIT_MAX_EXCEEDED"))
  .result();
```

## run tests
```sh
bun test
```

## files
- `index.ts` main engine + types
- `engine.test.ts` coverage for the weird edges
- `example.ts` small usage sandbox

kthxwuvubaii;3