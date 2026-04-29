# TyQL: Type-Safe Recursive Queries Reproducibility (Iterations)

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
$ docker load < tyql-artifact-iterations-all-image.tar.gz

# 2. Kick-the-tires: small suite only (~1 min), iterations CSVs in ./out
$ mkdir -p out
$ docker run --rm -v "$(pwd)/out:/out" tyql-artifact-iterations-all

# 3. Medium suite (~1hr30)
$ docker run --rm -v "$(pwd)/out:/out" tyql-artifact-iterations-all -M

# 4. Large suite (~3hr)
$ docker run --rm -v "$(pwd)/out:/out" tyql-artifact-iterations-all -L
```

Flags: `-S` / `-M` / `-L` select sizes; `-skipTimeout` skips configurations
known to hit the JMH timeout.

## Running detached (remote machines, long runs)

`-M` and `-L` take hours, so it's useful to start the container and log out:

```shell
$ CID=$(docker run -d -v "$(pwd)/out:/out" tyql-artifact-iterations-all -M)

# Safe to log out — the container keeps running under the Docker daemon

# Later: check if still running
$ docker ps --filter "id=$CID"

# Follow live JMH output
$ docker logs -f $CID

# Block until finished
$ docker wait $CID
```

After the container exits, `./out/` contains cleaned iterations CSVs and `./out/raw/`
contains the full sbt/JMH logs.