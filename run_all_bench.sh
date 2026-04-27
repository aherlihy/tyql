#!/usr/bin/env bash
# Run the JMH benchmarks for one or more input dataset sizes.
#
# Usage: bash run_all_bench.sh -java-home <path-to-jdk> [-S] [-M] [-L]
#
#   -java-home <path>   Path to the JDK to use (required). Example:
#                       -java-home /opt/graalvm-community-openjdk-17.0.9+9.1
#   -S                  Run the small  (xs/data) benchmark suite.
#   -M                  Run the medium (m_data)  benchmark suite.
#   -L                  Run the large  (l_data)  benchmark suite.
#
# If none of -S, -M, -L are supplied, all three sizes are run consecutively.
#
# For size <X> (one of s, m, l) the results are written to:
#     bench/benchmark_out_<X>.csv     # JMH CSV output
#     bench_<X>.out                   # full sbt/JMH log (follow with `tail -f`)
#
# The script clears any previous output files for the sizes it is about to run.

set -u

usage() {
    cat <<'EOF'
Run the JMH benchmarks for one or more input dataset sizes.

Usage: bash run_all_bench.sh -java-home <path-to-jdk> [-S] [-M] [-L] [-skipTimeout]

  -java-home <path>   Path to the JDK to use (required). Example:
                      -java-home /opt/graalvm-community-openjdk-17.0.9+9.1
  -S                  Run the small  (data)   benchmark suite.
  -M                  Run the medium (m_data) benchmark suite.
  -L                  Run the large  (l_data) benchmark suite.
  -skipTimeout        Skip the benchmark configurations that are known to hit
                      the 10-minute per-iteration timeout at -M and -L (faster
                      run, fewer rows in the output CSV). Without this flag,
                      every configuration is run — the complete paper-matching
                      run but slower because each timed-out configuration
                      still costs ~10 min.

If none of -S, -M, -L are supplied, all three sizes are run consecutively.

For size <X> (one of s, m, l) the results are written to:
    bench/benchmark_out_<X>.csv   # JMH CSV output
    bench_<X>.out                 # full sbt/JMH log (follow with `tail -f`)

The script removes any previous output files for the sizes it is about to run.
EOF
    exit "${1:-1}"
}

JAVA_HOME_PATH=""
RUN_S=0
RUN_M=0
RUN_L=0
SKIP_TIMEOUT=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        -java-home|--java-home)
            if [[ $# -lt 2 ]]; then
                echo "Error: -java-home requires a path argument." >&2
                usage 1
            fi
            JAVA_HOME_PATH="$2"
            shift 2
            ;;
        -S) RUN_S=1; shift ;;
        -M) RUN_M=1; shift ;;
        -L) RUN_L=1; shift ;;
        -skipTimeout|--skipTimeout) SKIP_TIMEOUT=1; shift ;;
        -h|--help) usage 0 ;;
        *)
            echo "Error: unknown argument: $1" >&2
            usage 1
            ;;
    esac
done

if [[ -z "$JAVA_HOME_PATH" ]]; then
    echo "Error: -java-home <path> is required." >&2
    usage 1
fi

if [[ ! -d "$JAVA_HOME_PATH" ]]; then
    echo "Error: java-home path does not exist: $JAVA_HOME_PATH" >&2
    exit 1
fi

# Default to all sizes if none were specified.
if [[ $RUN_S -eq 0 && $RUN_M -eq 0 && $RUN_L -eq 0 ]]; then
    RUN_S=1
    RUN_M=1
    RUN_L=1
fi

s_data_dir="data"
m_data_dir="m_data"
l_data_dir="l_data"

only=".*(TOScalaSQLBenchmark|TOCollectionsBenchmark).*"
# Exclusions that apply to every size. UnrestrictedTyQL is a debug/exploration
# variant that is not part of the paper's results and should never be run.
exceptBase='-e Unrestricted'

# Additional exclusions applied only when -skipTimeout is passed. These skip
# configurations that are known to hit the 10-minute per-iteration timeout at
# the given size, so the overall run is faster at the cost of producing fewer
# rows in the output CSV. Without -skipTimeout every configuration is run
# (complete paper-matching run).
exceptM_skipTimeout="${exceptBase} "'-e TOCollectionsBenchmark\.((cba)|(cspa)|(evenodd)|(pointsto)|(javapointsto)|(orbits)|(party)|(trust)) -e TOScalaSQLBenchmark\.((evenodd))'
exceptL_skipTimeout="${exceptBase} "'-e TOCollectionsBenchmark\.((ancestry)|(asps)|(bom)|(cba)|(cspa)|(evenodd)|(pointsto)|(orbits)|(party)|(trust)) -e TOScalaSQLBenchmark\.((ancestry)|(evenodd)|(pointsto)|(javapointsto))'

if [[ $SKIP_TIMEOUT -eq 1 ]]; then
    exceptS="$exceptBase"
    exceptM="$exceptM_skipTimeout"
    exceptL="$exceptL_skipTimeout"
else
    exceptS="$exceptBase"
    exceptM="$exceptBase"
    exceptL="$exceptBase"
fi

jtest="-wi 0 -i 1"

run_size() {
    local size="$1"
    local data_dir="$2"
    local except="$3"

    echo ">>> Running size=${size} (TYQL_DATA_DIR=${data_dir})"
    rm -f "bench_${size}.out" "bench/benchmark_out_${size}.csv"

    export TYQL_DATA_DIR="$data_dir"
    # Redirect stdin from /dev/null so sbt never tries to read from the
    # controlling TTY. Without this, running the script with `&` gets
    # suspended by SIGTTIN as soon as sbt's interactive console probes stdin.
    sbt -java-home "$JAVA_HOME_PATH" "clean; bench/Jmh/clean" < /dev/null
    # tee so JMH progress is visible on stdout while still being captured in
    # bench_<size>.out for postprocess.sh.
    sbt -java-home "$JAVA_HOME_PATH" \
        "bench/Jmh/run $only $except $jtest -rff benchmark_out_${size}.csv -jvmArgs \"-Xmx8G\"" \
        < /dev/null 2>&1 | tee "bench_${size}.out"
    echo ">>> Finished size=${size}. Log: bench_${size}.out  CSV: bench/benchmark_out_${size}.csv"
}

if [[ $RUN_S -eq 1 ]]; then run_size s "$s_data_dir" "$exceptS"; fi
if [[ $RUN_M -eq 1 ]]; then run_size m "$m_data_dir" "$exceptM"; fi
if [[ $RUN_L -eq 1 ]]; then run_size l "$l_data_dir" "$exceptL"; fi

exit 0
