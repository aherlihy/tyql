FROM debian:12

# Install general dependencies and the sqlite3 shell
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    unzip \
    openjdk-17-jdk \
    sqlite3 && \
    rm -rf /var/lib/apt/lists/*

# Install Scala and sbt
RUN curl -fL 'https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz' | gzip -d > cs && chmod +x cs && ./cs setup -y
ENV PATH="/root/.local/share/coursier/bin/:${PATH}"

# Install the DuckDB shell
RUN curl -fL 'https://github.com/duckdb/duckdb/releases/download/v1.1.1/duckdb_cli-linux-amd64.zip' --output 'duckdb.zip' && unzip duckdb.zip -d /usr/bin/ && rm duckdb.zip

# `sbt update` is dependent on the build.sbt
WORKDIR /app
COPY build.sbt build.sbt
RUN sbt update

COPY src/ src/
COPY project/build.properties properties/build.properties

CMD ["sbt", "test"]
