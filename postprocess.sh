#!/usr/bin/env bash
# Post-process raw JMH benchmark outputs into cleaned CSVs suitable for the
# import step described in the README (Excel "Get Data" pipeline).
#
# Usage: bash postprocess.sh <benchsrc> [output_dir]
#
#   <benchsrc>     Directory containing the raw benchmark outputs produced by
#                  run_all_bench.sh, namely:
#                       <benchsrc>/benchmark_out_s.csv   <benchsrc>/bench_s.out
#                       <benchsrc>/benchmark_out_m.csv   <benchsrc>/bench_m.out
#                       <benchsrc>/benchmark_out_l.csv   <benchsrc>/bench_l.out
#                  Any size whose CSV is missing is silently skipped.
#   [output_dir]   Where to write the cleaned CSVs. Defaults to ./results.
#
# For each size X in {s,m,l} (mapped to small/medium/large) that exists under
# <benchsrc>, this script writes:
#     <output_dir>/<size>_clean.csv         # CSV suitable for Excel import
#     <output_dir>/<size>_iterations.csv    # per-query iteration counts,
#                                           # extracted from the sbt log (only
#                                           # present if the benchmarks were
#                                           # built with the IT printlns enabled)
#
# The sed invocations below avoid the -i (in-place) flag and any GNU-only
# extensions, so this script is portable between GNU sed (Linux) and BSD sed
# (macOS). Tested on Ubuntu 22.04 and macOS (Darwin).

set -u

usage() {
    cat <<'EOF'
Usage: bash postprocess.sh <benchsrc> [output_dir]

  <benchsrc>     Directory containing benchmark_out_{s,m,l}.csv
                 and bench_{s,m,l}.out produced by run_all_bench.sh.
  [output_dir]   Output directory (default: ./results).

Writes <output_dir>/<size>_clean.csv and <output_dir>/<size>_iterations.csv
for each size present (small, medium, large).
EOF
    exit "${1:-1}"
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
    usage 1
fi
case "$1" in
    -h|--help) usage 0 ;;
esac

BENCHSRC="$1"
OUTDIR="${2:-results}"

if [[ ! -d "$BENCHSRC" ]]; then
    echo "Error: benchsrc directory not found: $BENCHSRC" >&2
    exit 1
fi

mkdir -p "$OUTDIR"

find_first() {
    # echo the first path that exists, else empty
    local p
    for p in "$@"; do
        [[ -f "$p" ]] && { echo "$p"; return 0; }
    done
    return 1
}

clean_one() {
    local size="$1"     # s|m|l
    local long="$2"     # small|medium|large

    # Accept either layout:
    #   <benchsrc>/benchmark_out_<size>.csv   (collected into one dir), OR
    #   <benchsrc>/bench/benchmark_out_<size>.csv  (straight out of run_all_bench.sh at repo root)
    local csv
    csv=$(find_first "$BENCHSRC/benchmark_out_${size}.csv" \
                     "$BENCHSRC/bench/benchmark_out_${size}.csv") || csv=""
    local log
    log=$(find_first "$BENCHSRC/bench_${size}.out") || log=""

    local out_clean="$OUTDIR/${long}_clean.csv"
    local out_iters="$OUTDIR/${long}_iterations.csv"

    if [[ -n "$csv" ]]; then
        # Strip the "tyql.bench.TO" prefix and split the "<Class>Benchmark.<query>"
        # cell into two CSV columns, then strip the benchmark-group suffixes used
        # in the JMH method names.
        sed -e 's/tyql\.bench\.TO//' \
            -e 's/Benchmark\./","/' \
            -e 's/_graph//' \
            -e 's/_misc//' \
            -e 's/_programanalysis//' \
            "$csv" > "$out_clean"
        echo "wrote $out_clean (from $csv)"
    else
        echo "(skipped ${long}: no benchmark_out_${size}.csv under $BENCHSRC or $BENCHSRC/bench)"
    fi

    if [[ -n "$log" ]]; then
        # Extract per-iteration counts printed by the optional IT printlns in
        # bench/src/main/scala/TimeoutQueryBenchmark/<query>.scala. If the
        # printlns are disabled the output is simply empty.
        sed -n 's/^\[info\] IT, *//p' "$log" > "$out_iters"
        echo "wrote $out_iters (from $log)"

        # Warn about any Exception in the log that is NOT a TimeoutException.
        # Timeouts are expected on the queries that blow past the per-iteration
        # cutoff; anything else indicates a real problem (missing data dir,
        # JVM crash, query failure, ...).
        local bad_count
        bad_count=$(grep -c -i 'Exception' "$log" | head -1)
        local to_count
        to_count=$(grep -c -i 'TimeoutException' "$log" | head -1)
        local non_to=$(( bad_count - to_count ))
        if [[ $non_to -gt 0 ]]; then
            echo ""
            echo "WARNING: $log contains $non_to non-timeout Exception line(s):" >&2
            grep -n -i 'Exception' "$log" \
                | grep -v -i 'TimeoutException' \
                | head -20 >&2
            echo "(Above: first 20 matches. Inspect $log for the full context.)" >&2
            echo ""
        fi
    fi
}

clean_one s small
clean_one m medium
clean_one l large
