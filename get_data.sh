#!/usr/bin/env bash
# Download and unpack the full input dataset (small / medium / large for all
# 16 Recursive Query Benchmark queries) from Zenodo into bench/data/.
#
# The small dataset is already checked into the repository, so the -S suite
# can run without this script. You only need to run this before the -M or -L
# suites.
#
# Usage:
#   bash get_data.sh [--force]
#
#   --force    Re-download and re-unpack even if a medium-size directory
#              already exists.
#
# After this script finishes, bench/data/<query>/{data,m_data,l_data}/ are
# populated for every query. run_all_bench.sh -M / -L can then be invoked.
#
# The script is idempotent: if the target directories already exist it exits
# without touching the network.

set -eu

ZENODO_URL="https://zenodo.org/records/19237643/files/rr-data.zip"
ZIP="rr-data.zip"
# If this directory already exists, we assume the dataset is unpacked.
PROBE_DIR="bench/data/ancestry/m_data"

FORCE=0
case "${1:-}" in
    --force) FORCE=1 ;;
    -h|--help)
        sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
        exit 0
        ;;
    "") ;;
    *)
        echo "Unknown argument: $1" >&2
        exit 1
        ;;
esac

print_next_step() {
    cat <<EOF

Next step: run the benchmarks in Docker, with bench/data bind-mounted so
the container sees the medium / large input datasets:

    mkdir -p out
    docker run --rm \\
        -v "\$(pwd)/out:/out" \\
        -v "\$(pwd)/bench/data:/tyql/bench/data" \\
        tyql-artifact -M

Replace -M with -L for the large suite, or pass both (-S -M -L) to run
everything. Add -skipTimeout to skip the configurations that are known to
hit the 10-minute per-iteration timeout.
EOF
}

if [[ $FORCE -eq 0 && -d "$PROBE_DIR" ]]; then
    echo "Dataset already present at $PROBE_DIR — nothing to do."
    echo "Pass --force to re-download and re-unpack."
    print_next_step
    exit 0
fi

if [[ ! -f "$ZIP" ]]; then
    echo ">>> Downloading $ZENODO_URL (~147 MB)"
    curl -fL --retry 3 --retry-delay 10 --connect-timeout 30 --max-time 1800 \
         --progress-bar -o "$ZIP" "$ZENODO_URL"
else
    echo ">>> Using existing $ZIP (pass --force to re-download)"
fi

echo ">>> Unpacking $ZIP into bench/"
# The zip's top-level entry is data/, so extracting into bench/ lands the
# content at bench/data/<query>/{data,m_data,l_data}/ — the layout the
# benchmark harness expects.
unzip -o -q "$ZIP" -d bench/

echo ">>> Done. Representative layout under bench/data/:"
ls -d bench/data/*/ | head -5
echo "    ..."

print_next_step
