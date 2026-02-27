# picky

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

> im still thinking of other stuff to add/improve - check NOTES.md (though its not really formatted for "others" and more for my ideas n stuff)


## owo

```ts
import { FilterEngine } from ".";

const result = FilterEngine.from(data)
  .equals("meta.owner.name", "Alice")
  .dateBetween("created", "2024-01-01", "2024-02-01")
  .nested("logs", q => q.equals("type", "CREDIT_MAX_EXCEEDED"))
  .result();
```

All fields + values are fully typesafe automatically  
(editor autocomplete with available keys / paths, comptime & LSP validity checking etc)

- Invalid paths fail at compile time
- Wrong value types fail at compile time
- Dates are gated behind date methods
- You can’t accidentally deep-chain arrays into oblivion


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
FilterEngine.configure({
  sharedCache: true,
  maxDateCache: 2048,
  maxPathCache: 2048,
});
```

- `sharedCache` -> reuse caches across engines (good for long-lived servers)
- `maxDateCache` -> cap for date parse cache
- `maxPathCache` -> cap for dot-path cache

`FilterEngine.clearCaches()` clears shared caches manually if needed.



## run tests

```sh
bun test
```



## files

- `index.ts` main engine + types + cursed type gymnastics
- `engine.test.ts` coverage for weird edges + date behavior
- `example.ts` small usage sandbox



## what this is not

- not a database
- not a graphql engine
- not meant for untrusted freeform JSON chaos
- not meant for multi-million row analytics

it’s a very fast, type-safe in-memory picky boi.



kthxwuvubaii;3