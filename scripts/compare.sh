#!/usr/bin/env bash
# Run and profile all benchmarks in scripts/smol-bench/tasks/*.bench.ts
# - First run: timed (time -p)
# - Second run: bun with --cpu-prof-md and --heap-prof-md, saving to ./perf
# - Pre-run: generate benchmark data in ./perf/bench-data
#
# Usage (from repo root): bun ru bench

set -u
# intentionally not `set -e` so a failing benchmark doesn't stop the rest

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Use a relative perf dir when passing to bun (prevents duplicated cwd in bun output)
PERF_DIR_REL="perf"                 # path passed to bun (relative)
PERF_DIR_ABS="$PROJECT_ROOT/$PERF_DIR_REL"  # actual directory to create

BENCH_DIR="$SCRIPT_DIR/smol-bench/tasks"

mkdir -p "$PERF_DIR_ABS"

if [ ! -d "$BENCH_DIR" ]; then
  echo "Benchmark directory not found: $BENCH_DIR"
  echo "Expected: scripts/smol-bench/tasks"
  exit 1
fi

# enable nullglob so the for-loop runs zero times if no matches
shopt -s nullglob
bench_files=("$BENCH_DIR"/*.bench.ts)
shopt -u nullglob

if [ "${#bench_files[@]}" -eq 0 ]; then
  echo "No .bench.ts files found in $BENCH_DIR"
  exit 0
fi

echo
echo "Generating benchmark data..."
bun "$SCRIPT_DIR/smol-bench/generate_data.ts"

for bench_path in "${bench_files[@]}"; do
  bench_file="$(basename "$bench_path")"          # e.g. 12k.bench.ts
  bench_name_noext="${bench_file%.bench.ts}"     # e.g. 12k
  suite_name="$(basename "$(dirname "$BENCH_DIR")")" # smol-bench (parent of tasks)

  header="Benchmark: ${suite_name} / ${bench_name_noext}"
  separator="$(printf '%*s' "${#header}" '' | tr ' ' '-')"

  echo
  echo "$header"
  echo "$separator"

  printf "Timing benchmark %s %s...\n" "$suite_name" "$bench_name_noext"
  # run timed; time outputs to stderr; use -p for portable output
  time -p bun "$bench_path"
  echo

  printf "Profiling benchmark %s %s...\n" "$suite_name" "$bench_name_noext"
  cpu_prof_name="${suite_name}-${bench_name_noext}-cpu-prof.md"
  heap_prof_name="${suite_name}-${bench_name_noext}-heap-prof.md"

  # Pass perf dir as relative path to bun to avoid bun printing duplicated cwd.
  # We still create the absolute directory above.
  bun --cpu-prof-md --heap-prof-md \
    --cpu-prof-name "$cpu_prof_name" --cpu-prof-dir "$PERF_DIR_REL" \
    --heap-prof-name "$heap_prof_name" --heap-prof-dir "$PERF_DIR_REL" \
    "$bench_path"

  echo "Saved profiles:"
  echo " - $PERF_DIR_ABS/$cpu_prof_name"
  echo " - $PERF_DIR_ABS/$heap_prof_name"
done

echo
echo "All benchmarks processed. Profiles are in: $PERF_DIR_ABS"
