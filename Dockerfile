# syntax=docker/dockerfile:1
#
# TyQL reproducibility artifact for ECOOP 2026.
# Matches the paper environment: Ubuntu 22.04, GraalVM Community 17.0.9, sbt 1.9.9.
# Pinned to linux/amd64 (the architecture the paper's numbers were measured on).
#
# Build:
#   docker build -t tyql-artifact .
#
# Push-button run (small size only):
#   mkdir -p out
#   docker run --rm -v "$(pwd)/out:/out" tyql-artifact
#
# Run additional sizes (requires the Zenodo-hosted medium/large data
# bind-mounted under /tyql/bench/data — see README):
#   docker run --rm \
#       -v "$(pwd)/out:/out" \
#       -v "$(pwd)/bench/data:/tyql/bench/data" \
#       tyql-artifact -S -M -L

FROM --platform=linux/amd64 ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

# Base tools required by run_all_bench.sh / postprocess.sh plus what we need
# to fetch GraalVM and sbt at build time. Everything pinned to apt so there
# are no unpinned network reads at runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        coreutils \
        curl \
        gzip \
        sed \
        tar \
    && rm -rf /var/lib/apt/lists/*

# --- GraalVM Community JDK 17.0.9 (exact version used for the paper) ---
ENV GRAALVM_HOME=/opt/graalvm-community-openjdk-17.0.9+9.1
ENV PATH=${GRAALVM_HOME}/bin:${PATH}
RUN set -eux; \
    curl -fL --retry 3 --retry-delay 10 --connect-timeout 30 --max-time 900 \
        -o /tmp/graalvm.tgz \
        "https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-17.0.9/graalvm-community-jdk-17.0.9_linux-x64_bin.tar.gz"; \
    tar -xzf /tmp/graalvm.tgz -C /opt; \
    rm /tmp/graalvm.tgz; \
    java -version

# --- sbt 1.9.9 (arch-independent JVM application) ---
ENV SBT_HOME=/opt/sbt
ENV PATH=${SBT_HOME}/bin:${PATH}
RUN set -eux; \
    curl -fL --retry 3 --retry-delay 10 --connect-timeout 30 --max-time 600 \
        -o /tmp/sbt.tgz \
        https://github.com/sbt/sbt/releases/download/v1.9.9/sbt-1.9.9.tgz; \
    tar -xzf /tmp/sbt.tgz -C /opt; \
    rm /tmp/sbt.tgz

WORKDIR /tyql

# Copy the repo (small dataset lives in bench/data/<q>/data/). The
# .dockerignore elides per-run outputs, target/ dirs, and large zips that are
# regenerated from Zenodo.
COPY . /tyql/

# Pre-resolve all sbt/Ivy dependencies and compile once, so the runtime
# invocation does not need internet. We then clear target/ so that the
# `clean + Jmh/clean` step inside run_all_bench.sh starts from a known state.
RUN set -eux; \
    sbt -java-home "${GRAALVM_HOME}" \
        "update; bench/update; compile; bench/Jmh/compile"; \
    sbt -java-home "${GRAALVM_HOME}" "clean; bench/Jmh/clean"

# Cleaned CSVs are written here; bind-mount from the host to retrieve them.
VOLUME ["/out"]

COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
# Default to the small size.
CMD ["-S"]
