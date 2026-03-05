# pickie

smol lil filter engine focussing on perf optimizations and nice sugary DX for arrays of *structured* objects.  
> WIP, but functional. Still trying to find more fast-paths & micro optimizations uwu  
note: this is built for bun + typescript, zero runtime deps.

> it is actually quite neat but i hate writing docs so go dig in the code u fkn twink

## what it does

- build chainable predicates for nested data
- fully typesafe dot-paths (autocomplete + comptime validation)
- understands dates (`Date`, ISO strings, timestamps)
- supports arrays once per path (by design)
- small bounded cache for hot paths + date parsing
- nested objects + `Array<object>` support
- immutable builder (each chain returns a new engine)
- methods for `or, and & not` grouping
- `nested` method for "stepping into" an array of objects
- `pathExists` vs `pathExistsNullable` distinction
- equals, notEquals
- lessThan, lessThanOrEqual, greaterThan, greaterThanOrEqual
- in, notIn
- contains, startsWith, endsWith, matches (regex)
- arraySome, arrayEvery, arrayNone
- between, dateBefore, dateAfter, dateBetween, dateEquals
- custom predicates if u wanna do cursed stuff manually
- ordering, limiting, pagination, grouping
- fuzzy search + tagger pipeline (optional + typed)
- async ingress + streaming execution (stream or eager based on hints)
- pushdown adapters (sqlite, redis, redis sorted set)
- http ingress adapter + openapi schema inference

> im still thinking of other stuff to add/improve - check NOTES.md (though its not really formatted for "others" and more for my ideas n stuff)


## owo

```ts
import { Engine, IngressEngine } from ".";

const result = Engine.from(IngressEngine.from(data))
  .equals("meta.owner.name", "Alice")
  .dateBetween("created", "2024-01-01", "2024-02-01")
  .nested("logs", q => q.equals("type", "CREDIT_MAX_EXCEEDED"))
  .out()
  .orderBy("score", { direction: "desc" })
  .limit(10)
  .result();
```

```ts
const grouped = Engine.from(IngressEngine.from(data))
  .equals("active", true)
  .out()
  .orderBy("name")
  .groupBy("meta.owner.name");

const cursor = Engine.from(IngressEngine.from(data))
  .out()
  .orderByDate("created")
  .paginate({ pageSize: 50, total: "lazy" });

cursor.next();
```

All fields + values are fully typesafe automatically  
(editor autocomplete with available keys / paths, comptime & LSP validity checking etc)

- Invalid paths fail at compile time
- Wrong value types fail at compile time
- Dates are gated behind date methods
- You can’t accidentally deep-chain arrays into oblivion


## examples

- `examples/basic-filtering.ts` predicate chaining + nested array filter
- `examples/search-fuzzy.ts` fuzzy search + score metadata
- `examples/search-tagger.ts` tagger rules + tag filters
- `examples/grouping-pagination.ts` ordering, paging, grouping
- `examples/chains.ts` reusable chains + schema inference
- `examples/async-ingress-search.ts` async ingress + fuzzy search
- `examples/sqlite-pushdown.ts` sqlite ingress + pushdown order/filter
- `examples/http-ingress.ts` http ingress adapter


## perf

tested with:

- ~3200 elements
- ~22MB dataset in memory
- deep nested object paths
- nested array filtering

-> ~3.4–3.6ms on bun runtime

perf scales roughly linearly with dataset size.


## perf choices & fast paths

we do a "bunch" of nonsense internally to try to squeeze perf owo

### single-array-per-path rule

you can traverse arrays once per path.

why?

- prevents combinatorial explosions
- keeps traversal complexity predictable
- keeps type recursion sane
- keeps runtime fast
- avoids accidental `users.posts.comments.author.name` doom chains

this is a constraint for performance (& my) sanity.


### depth-capped path typing

type recursion is capped (default depth 5).

why?

- prevents TS compiler meltdown
- keeps LSP responsive
- still deep enough for 99% of real schemas



### iterative traversal (no recursion)

path resolution uses iterative buffers (`current` / `next`) instead of recursion.

why?

- no call stack growth
- better JIT optimization
- stable hot loop shape
- avoids deopts



### double-buffer reuse

`current` and `next` arrays are swapped instead of reallocated each depth step.

why?

- avoids GC churn
- reduces allocations in hot paths
- keeps filtering stable under load



### compile fast paths (0–3 predicates)

the engine special-cases 0, 1, 2 and 3 predicates before falling back to a loop.

why?

- avoids loop overhead for common cases
- helps inlining
- small but measurable gain in tight filtering loops


### no-predicate shortcut

when no predicates exist, we skip predicate calls and slice directly.

why?

- avoids unnecessary branching
- keeps pagination + grouping fast in the zero-filter case


### limit/offset fast path

limit/offset without ordering uses a streaming filter with early exit.

why?

- avoids allocating full filtered arrays
- breaks early once `offset + limit` is satisfied
- keeps hot loop tight



### top-k selection for ordered limit/offset

when ordering + limit/offset is used, we keep a bounded heap for the smallest `offset + limit` items.

why?

- avoids full sort when you only need a small window
- keeps stable ordering via the same comparator + index tiebreaks


### order key specialization (1–3 keys)

ordering with 1–3 keys stores keys as fields instead of an array.

why?

- fewer allocations per item
- faster comparator access in hot sorts


### short-path resolver fast paths

paths with 1–3 segments use dedicated loops instead of the generic buffer walk.

why?

- fewer allocations in hot resolves
- fewer branches for the most common path shapes


### pre-specialized comparison predicates

operators (`gt`, `lt`, etc.) generate specialized closures.

why?

- no operator switching in hot loop
- no per-candidate branching
- keeps predicate monomorphic



### ISO date pre-check + cache

date parsing:

- checks regex before calling `Date.parse`
- caches both valid and invalid parses
- bounded FIFO cache

why?

- `Date.parse` is slow
- ISO pre-check avoids garbage parsing
- caching avoids repeated parsing of hot values
- FIFO is cheaper than full LRU and good enough



### path segment cache

dot paths are split once and cached.

why?

- `.split(".")` in hot loops bad
- path strings are usually reused
- bounded cache keeps memory stable for long-lived services



### regex sanitization

global (`g`) + sticky (`y`) flags are stripped internally.

why?

- `.test()` mutates `lastIndex`
- causes nondeterministic behavior
- makes repeated filtering unsafe

i fucking hate regex



### OR collapsing

`or()` collapses previous predicates into one group.

why?

- prevents predicate tree explosion
- keeps compiled predicate flat
- easier for JIT to optimize


### ordering & grouping are optional

ordering only materializes sorted buffers when needed.

why?

- keeps default filtering zero-cost for ordering
- avoids extra allocations when you don't request it
- allows fast-path limit/offset



### runtime shape intentionally loose

runtime accepts generic structured objects.

types enforce correctness at compile time.

why?

- no runtime schema walking
- no deep validation overhead
- yunny
- we assume structured data, and we already validate @ comp



## configure

```ts
IngressEngine.configure({
  sharedCache: true,
  maxDateCache: 2048,
  maxPathCache: 2048,
});
```

- `sharedCache` -> reuse caches across engines (good for long-lived servers)
- `maxDateCache` -> cap for date parse cache
- `maxPathCache` -> cap for dot-path cache

`IngressEngine.clearCaches()` clears shared caches manually if needed.



## run tests

```sh
bun test
```



## run tests

```sh
bun test
```



## run benchmrks (linux/mac)

```sh
bun run bench
```



## what this is not

- not a database
- not a graphql engine
- not meant for untrusted freeform JSON chaos
- not meant for multi-million row analytics

it’s a very fast, type-safe in-memory picky boi.



kthxwuvubaii;3
