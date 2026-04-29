#!/usr/bin/env bash
# ECOOP 2026 TyQL artifact entrypoint.
#
# Runs run_all_bench.sh for the requested size(s) and then postprocess.sh,
# writing cleaned CSVs (plus the raw JMH outputs) into /out.
#
# All arguments are forwarded to run_all_bench.sh except when the first
# argument is -h/--help (we print this help instead).

set -eu

OUTDIR="${OUTDIR:-/out}"

usage() {
    cat <<'EOF'
TyQL ECOOP 2026 artifact runner.

Usage: docker run --rm -v $PWD/out:/out tyql-artifact [-S] [-M] [-L]

Flags are forwarded to run_all_bench.sh. With no flags, -S (small, ~3 min
on the authors' machine) is used.

Output layout (inside the mounted /out):
  <size>_clean.csv          cleaned CSV suitable for Excel import
  raw/bench_<size>.out      full sbt/JMH log
  raw/benchmark_out_<size>.csv  raw JMH CSV

Medium (-M) and large (-L) suites require the Zenodo data mounted at
/tyql/bench/data so that each query directory has m_data/ and l_data/
subdirectories. See the README for the exact mount command.
EOF
}

case "${1:-}" in
    -h|--help) usage; exit 0 ;;
esac

mkdir -p "$OUTDIR"
cd /tyql

echo ">>> Running benchmarks (args: $*)"
bash run_all_bench.sh -java-home "$GRAALVM_HOME" "$@"

echo ">>> Post-processing into $OUTDIR"
bash postprocess.sh -perf /tyql "$OUTDIR"

# Ship the raw outputs too, for diagnostics.
mkdir -p "$OUTDIR/raw"
for sz in s m l; do
    if [ -f "/tyql/bench_${sz}.out" ]; then
        cp "/tyql/bench_${sz}.out" "$OUTDIR/raw/"
    fi
    if [ -f "/tyql/bench/benchmark_out_${sz}.csv" ]; then
        cp "/tyql/bench/benchmark_out_${sz}.csv" "$OUTDIR/raw/"
    fi
done

echo ">>> Done. Results in $OUTDIR:"
ls -la "$OUTDIR"
