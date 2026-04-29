# TyQL: Type-Safe Recursive Queries Reproducibility

## Experimental Setup
Experiments are run on Intel(R) Xeon(R) Gold 5118 CPU @ 2.30GHz (2 x 12-core) with 395GB RAM,
on Ubuntu 22.04 LTS with Linux kernel 6.5.0-17-generic and Scala 3.8.2. The JVM used is
GraalVM Community Java 17.0.9 with sbt 1.9.9 with heap size 8GB.

Each experiment is run with the Java Benchmarking Harness (JMH) with 5 iterations, 5 warm-up iterations.
The library versions are specified in the root `build.sbt` file.

## Input datasets
The experiments are run on 3 differently sized datasets stored as CSV files in the `bench/data` directory.
All three sizes (small, medium, large) are included in the pre-built Docker image.

The datasets are also available separately on Zenodo
(https://zenodo.org/records/19237643) as `rr-data.zip` (~147 MB) for
building from source. The script `get_data.sh` downloads and unpacks them.

# Push-button run (pre-built Docker image)

The artifact is distributed as a pre-built Docker image containing the
complete environment (Ubuntu 22.04, GraalVM Community 17.0.9, sbt 1.9.9,
Scala 3.8.2, JMH 1.37, pinned to `linux/amd64`) with all datasets and
dependencies pre-installed. No internet access is needed to run benchmarks.

## Quick start

```shell
# 1. Load the pre-built image
$ docker load < tyql-artifact-image.tar.gz

# 2. Kick-the-tires: small suite only (~3 min), cleaned CSVs in ./out
$ mkdir -p out
$ docker run --rm -v "$(pwd)/out:/out" tyql-artifact

# 3. Medium suite (~4 hours, ~2 hours with -skipTimeout)
$ docker run --rm -v "$(pwd)/out:/out" tyql-artifact -M

# 4. Large suite (~10 hours, ~5 hours with -skipTimeout)
$ docker run --rm -v "$(pwd)/out:/out" tyql-artifact -L
```

Flags: `-S` / `-M` / `-L` select sizes; `-skipTimeout` skips configurations
known to hit the JMH timeout.

## Running detached (remote machines, long runs)

`-M` and `-L` take hours, so it's useful to start the container and log out:

```shell
$ CID=$(docker run -d -v "$(pwd)/out:/out" tyql-artifact -M)

# Safe to log out — the container keeps running under the Docker daemon

# Later: check if still running
$ docker ps --filter "id=$CID"

# Follow live JMH output
$ docker logs -f $CID

# Block until finished
$ docker wait $CID
```

After the container exits, `./out/` contains cleaned CSVs and `./out/raw/`
contains the full sbt/JMH logs.

## Run sizes and expected wall-clock time

| Size   | Flag | Wall-clock time              |
|--------|------|------------------------------|
| small  | `-S` | a few minutes                |
| medium | `-M` | ~4 hours, ~2hr -skipTimeout  |
| large  | `-L` | ~10 hours, ~5hr -skipTimeout |

The paper's performance table reports all three sizes. Running `-S` and `-M`
alone reproduces the small and medium rows. `-S` on its own is the
kick-the-tires option.

Docker Desktop users (macOS/Windows) may need to increase the VM memory in
Settings > Resources to accommodate the `-Xmx8G` heap plus JVM overhead for
`-M` / `-L`. `-S` runs in the default Docker Desktop configuration.

# Generating tables with Excel
For convenience, an Excel file (`artifact-tables.xlsx`) reproduces the exact
tables and provides a summary of the % difference in reported speedup between
the reviewer's data, the bare-metal run reported in the paper, and the results
the authors got from following these steps on the dockerized version.

1) Open `artifact-tables.xlsx` with Excel (tested with Version 16.76 / 23081101).
2) Add an empty sheet to import the data into.
3) In that new sheet, navigate to `Data > Get Data (Power Query) > From Text (Legacy)` and select `out/small_clean.csv` > "Get Data".
4) In the text import wizard click "Delimited" > next > select "Comma". Do **not** treat consecutive delimiters as one.
5) Finish and paste the data in the "small" sheet where it is indicated in yellow.
6) Repeat steps 3-5 for `medium_clean.csv` and `large_clean.csv` in their respective sheets.

The "EXACT TABLE" sheet generates the paper's performance tables, with the
exception of the number of iterations (reproducing iteration counts requires
running benchmarks a second time with print-lines uncommented; the number of
iterations is a property of the query, data, and backend database, not of TyQL).

The "compare-with-submitted" sheet summarizes the difference in speedup between
the bare-metal submitted benchmarks and the reviewer's data, as well as the
difference between the authors' dockerized run and the reviewer's data. Cells
are highlighted in red if the difference exceeds 1.5x/0.5x.

Due to the JIT there is some variability in the calculated speedups but the
numbers should be within the same order of magnitude and consistent, relative
to each other, with the ones in the paper.

# Building from source (without the pre-built image)

The instructions below are for building the Docker image or running benchmarks
from source. This is not required if using the pre-built image above.

## Building the Docker image from scratch

```shell
# Fetch medium/large datasets from Zenodo
$ bash get_data.sh

# Build the image (includes all data)
$ docker build -t tyql-artifact .

# Save the image
$ docker save tyql-artifact | gzip > tyql-artifact-image.tar.gz
```

## Building TyQL natively (without Docker)
```shell
$ sbt -java-home <path to jdk> "clean;compile;bench/Jmh/clean;bench/Jmh/compile"
```

## Running benchmarks natively

Run benchmarks on a dedicated server without other processes running.
The script `run_all_bench.sh` launches one or more sizes consecutively.

```
Usage: bash run_all_bench.sh -java-home <path-to-jdk> [-S] [-M] [-L] [-skipTimeout]

  -java-home <path>   Path to the JDK to use (required).
  -S                  Run the small  (data)   benchmark suite.
  -M                  Run the medium (m_data) benchmark suite.
  -L                  Run the large  (l_data) benchmark suite.
  -skipTimeout        Skip configurations that are known to hit timeout at -M/-L.
```

If none of `-S`, `-M`, `-L` are supplied, all three sizes are run consecutively.

Examples:
```shell
$ bash run_all_bench.sh -java-home /opt/graalvm-community-openjdk-17.0.9+9.1 -S
```

## Post-processing

`postprocess.sh` cleans the raw JMH CSV and scans for non-`TimeoutException` errors:

```
Usage: bash postprocess.sh <benchsrc> [output_dir]
```

Example:
```shell
$ bash postprocess.sh . results
```

# Additional Notes
- The `restrictedFix` combinator (Fig. 14) is in `src/main/scala/tyql/query/Query.scala`.
- The example queries (Fig. 11) are in `src/test/scala/test/query/PaperExampleTests.scala`.
- Backend specialization (dialects, multi-database SQL) is on the `backend-specialization` branch.
- Contact: herlihyap at gmail
