# picky

smol lil filter engine focussing on perf optimizations and nice sugary DX for arrays of *structured* objects.
> WIP, but functional. Still trying to find more fast-paths & micro optimizations uwu
note: this is built for bun + typescript, zero runtime deps.

> it is actually quite neat but i hate writing docs so go dig in the code u fkn twink

## what it does
- build chainable predicates for nested data
- understands dates (Date, ISO strings, timestamps)
- supports arrays once per path (by design)
- has a small cache for hot paths + date parsing
- supports nested objects, Array<object>s, with pretty nice paths n full type safety
- methods for `or, and & not` grouping
- `nested` method for "stepping into" an areay of objects
- equals, lessThan, greaterThan, lessThanOrEquals, in, notIn, contains, startsWith, endsWith, matches (regex), areaySome, arrayAny, arrayNone, between, dateBefore, dateAfter, dateBetween & more predicates 

## owo
```ts
import { FilterEngine } from ".";

const result = FilterEngine.from(data)
  .equals("meta.owner.name", "Alice")
  .dateBetween("created", "2024-01-01", "2024-02-01")
  .nested("Logs", q => q.equals("type", "CREDIT_MAX_EXCEEDED"))
  .result();
```

All of the fields and values n everything are fully typesafe automatically (editor autocomplete with available keys / paths, comptime & lsp validity checking etc)

## run tests
```sh
bun test
```

## files
- `index.ts` main engine + types
- `engine.test.ts` coverage for the weird edges
- `example.ts` small usage sandbox

kthxwuvubaii;3
