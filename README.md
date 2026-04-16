# TyQL: Type-Safe Recursive Queries Reproducibility 

# Experimental Section 
Experiments in this section are run on Intel(R) Xeon(R) Gold 5118 CPU @ 2.30GHz (2 x 12-core) with 376GB RAM, 
on Ubuntu 22.04 LTS with Linux kernel Linux 5.15.0-27-generic (s48) and Scala 3.8.2 The JVM used is
GraalVM Community Java 17.0.9 with sbt 1.9.9 with heap size 8GB

Each experiment is run with the Java Benchmarking Harness (JMH) with 5 iterations, 5 warm-up iterations.
The library versions are specified in the root `build.sbt` file.

# Input datasets
The experiments are run on 3 differently sized datasets that are stored as CSV files in the `bench/data` directory.
The smallest (small, `-S`) is checked into the repository at `bench/data/<benchmark>/data/` and is used by default.
The medium (`-M`) and large (`-L`) datasets are hosted on Zenodo
(https://zenodo.org/records/19237643) as a single archive `rr-data.zip` (~147 MB).

`get_data.sh` downloads and unpacks them into the correct locations
(`bench/data/<benchmark>/m_data/` and `.../l_data/`). It is idempotent — re-running it is a no-op once the data is in place:
```shell
$ bash get_data.sh
```
After it finishes, the message at the end of the script gives the exact
`docker run` command to use for the medium / large suites (it bind-mounts `bench/data` into the container).

# Push-button run (Docker)
A `Dockerfile` is provided that matches the paper environment exactly
(Ubuntu 22.04 + GraalVM Community 17.0.9 + sbt 1.9.9, pinned to `linux/amd64`).
All dependencies are pre-resolved at build time, so running the benchmarks
needs no internet.

The full reviewer workflow:

```shell
# 1. Build the image (one-time)
$ docker build -t tyql-artifact .

# 2. Kick-the-tires: small suite only, cleaned CSVs in ./out
$ mkdir -p out
$ docker run --rm -v "$(pwd)/out:/out" tyql-artifact

# 3. (Only for -M / -L) fetch the Zenodo-hosted medium/large datasets
$ bash get_data.sh

# 4. Medium run (or -L, or -S -M -L) with bench/data bind-mounted
$ docker run --rm \
      -v "$(pwd)/out:/out" \
      -v "$(pwd)/bench/data:/tyql/bench/data" \
      tyql-artifact -M
```

Flags accepted by the image are the same as `run_all_bench.sh`:
`-S` / `-M` / `-L` select sizes, and `-skipTimeout` skips the configurations
that are known to hit the 10-minute per-iteration timeout at `-M` / `-L`
(faster run, fewer rows in the output CSV; without it every configuration is
run — the complete paper-matching run but slower because each timed-out
configuration still costs ~10 min).

## Running detached (remote machines, long `-M` / `-L` runs)

`-M` and `-L` take hours, so it's useful to start the container and log out.
Use `docker run -d` (detached), save the container ID, and follow the live log
with `docker logs -f`:

```shell
# Start detached; the command prints and returns a container ID
$ CID=$(docker run -d \
         -v "$(pwd)/out:/out" \
         -v "$(pwd)/bench/data:/tyql/bench/data" \
         tyql-artifact -S -M)

# (safe to log out here — the container keeps running under the Docker daemon)

# Later: see whether it's still running
$ docker ps --filter "id=$CID"

# Follow the live JMH output (equivalent to `tail -f bench_<size>.out`)
$ docker logs -f $CID

# Block until it finishes (exit code mirrors the entrypoint)
$ docker wait $CID
```

After the container exits, `./out/` contains the cleaned CSVs and `./out/raw/`
contains the full sbt/JMH logs.

## Run sizes and expected wall-clock time

On the machine used for the paper's experiments (see the Experimental Section
above), the three sizes took approximately:

| Size | Flag | Wall-clock on tested machine |
|------|------|------------------------------|
| small  | `-S` | a few minutes |
| medium | `-M` | ~4 hours  |
| large  | `-L` | ~10 hours |

The paper's performance table reports all three sizes. The ECOOP artifact
guidelines recommend supplying a scaled-down version of any experiment whose
full run is prohibitively long; running `-S` and `-M` alone reproduces the
small and medium rows of the paper's table and avoids the ~10-hour `-L` run,
while `-S` on its own is the short-run / kick-the-tires option.

Reviewers running under Docker Desktop (macOS/Windows) for the `-M` or `-L`
suites may need to increase the Docker Desktop VM memory in
Settings → Resources to accommodate the `-Xmx8G` heap plus JVM overhead.
`-S` runs in the default Docker Desktop configuration.

# Build TyQL (without Docker)
```shell
# Always clean + recompile both source and benchmarking code between runs, otherwise JMH can cause SBT to exit if something is out of sync 
$ sbt -java-home <path to jdk> "clean;compile;bench/Jmh/clean;bench/Jmh/compile"
```
# Run Benchmarks
Run benchmarks on a dedicated server without other processes running at the same time.
The script `run_all_bench.sh` launches one or more of the three sizes consecutively.

```
Usage: bash run_all_bench.sh -java-home <path-to-jdk> [-S] [-M] [-L] [-skipTimeout]

  -java-home <path>   Path to the JDK to use (required).
  -S                  Run the small  (data)   benchmark suite.
  -M                  Run the medium (m_data) benchmark suite.
  -L                  Run the large  (l_data) benchmark suite.
  -skipTimeout        Skip configurations that are known to hit the 10-minute
                      per-iteration timeout at -M/-L. Faster run, fewer rows
                      in the output CSV. Without it every configuration is
                      run — complete paper-matching run but slower because
                      each timed-out configuration still costs ~10 min.
```
If none of `-S`, `-M`, `-L` are supplied, all three sizes are run consecutively.

The log of each size is written to `bench_<size>.out` and the results to
`bench/benchmark_out_<size>.csv`. You can follow the progress with
`tail -f bench_<size>.out`.

Examples:
```shell
# Run all three sizes
$ bash run_all_bench.sh -java-home /opt/graalvm-community-openjdk-17.0.9+9.1

# Run just the small suite (the short-run option for "kick-the-tires")
$ bash run_all_bench.sh -java-home /opt/graalvm-community-openjdk-17.0.9+9.1 -S
```

# Post-processing result data
This section describes how to generate the tables/charts used in the paper.
`postprocess.sh` automatically scans each `bench_<size>.out` for `Exception`
lines that are not `TimeoutException` and prints a warning if any are found —
benchmarks that hit the 10-minute cutoff surface as `TimeoutException`s and are
expected, but any other exception indicates a problem (missing data dir, JVM
crash, query failure, ...).

## Cleaning the benchmark files
Run `postprocess.sh` on the directory containing the raw outputs of
`run_all_bench.sh`. The script strips the JMH class prefix from each row and
splits the method name into a separate column so the CSV can be imported into
Excel as-is. It uses only POSIX-compatible `sed` features and works on both
macOS (BSD sed) and Ubuntu (GNU sed).

```
Usage: bash postprocess.sh <benchsrc> [output_dir]

  <benchsrc>     Directory with benchmark_out_{s,m,l}.csv and bench_{s,m,l}.out
                 (both the repo root after a local run, or a collected dir,
                 are accepted).
  [output_dir]   Output directory (default: ./results).
```

For each size present it writes:
- `<output_dir>/<size>_clean.csv` — cleaned CSV suitable for Excel import
- `<output_dir>/<size>_iterations.csv` — per-query iteration counts extracted
  from the sbt log (empty unless the optional `IT` printlns in
  `bench/src/main/scala/TimeoutQueryBenchmark/<query>.scala` are enabled)

Example (assuming you ran the benchmarks from the repo root):
```shell
$ bash postprocess.sh . results
```

## Generating aggregate data 
The excel file used to generate results is `tyql-repro.xlsx`; import the cleaned
data and the charts auto-generate. Steps:
1) Open `tyql-repro.xlsx` with Excel (tested with Version 16.76 / 23081101).
2) Add an empty sheet to import the data into.
3) In that new sheet, navigate to `Data > Get Data (Power Query) > From Text (Legacy)` and select `results/small_clean.csv` > "Get Data".
4) In the text import wizard click "Delimited" > next > select "Comma".
5) Do **not** treat consecutive delimiters as one. > Finish and put the data, ignoring the header, in the "small" sheet starting at row 3 column A. It should go to line 50.
6) Repeat steps 3-5 for `results/medium_clean.csv` and `results/large_clean.csv` in their respective sheets.

Due to the JIT there is some variability in the calculated speedups 
but the numbers should be within the same order of magnitude and consistent, relative to each other, with the ones in the paper. The Orbits query is unique among the benchmarks in that it references the recursive relation multiple times in the final query via a correlated subquery, which limits the speedup over non-recursive SQL on
  larger inputs as DuckDB re-evaluates the recursive CTE for each reference. On some machines WITH RECURSIVE is slightly faster, on some machines non-recursive SQL is slightly faster, and on some machines they are equivalent.
If you want to additionally count iterations, then you can uncomment the printlines in `bench/src/main/scala/TimeoutQueryBenchmark/<query>.scala` 
and rerun the benchmarks, then post-processes the output to count the number of iterations.

If there are any questions or problems running the benchmark you can always contact me at herlihyap at gmail and I will do my best to clarify.

To keep the recursive query source code focused and readable, backend specialization (Dialect support, multi-database SQL generation) is located on the "backend-specialization" branch.

# Additional Notes
- The code shown in Fig. 14 is located in `restrictedFix`: `scala/tyql/query/Query.scala:357`.
- The example queries in Fig. 11 are located in `scala/test/query/PaperExampleTests.scala`.