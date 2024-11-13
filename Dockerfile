FROM debian:12

# Install all packages in one layer, including sbt repository setup
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    unzip \
    netcat-openbsd \
    sqlite3 \
    ca-certificates \
    gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    curl -L https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.deb -o jdk.deb && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    ./jdk.deb \
    sbt && \
    rm jdk.deb && \
    rm -rf /var/lib/apt/lists/*

# Install DuckDB
RUN curl -L https://github.com/duckdb/duckdb/releases/download/v0.8.1/duckdb_cli-linux-amd64.zip -o duckdb.zip \
    && unzip duckdb.zip \
    && mv duckdb /usr/local/bin/ \
    && chmod +x /usr/local/bin/duckdb \
    && rm duckdb.zip

# Run as a non-root user
RUN useradd -m -s /bin/bash appuser \
    && mkdir -p /app /test-results \
    && chown -R appuser:appuser /app /test-results
COPY --chown=appuser:appuser start.sh /start.sh
RUN chmod +x /start.sh
WORKDIR /app
USER appuser

CMD ["/start.sh"]

