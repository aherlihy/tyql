FROM debian:12

# Install general dependencies
RUN apt-get update
RUN apt-get install -y curl unzip

# Install Scala and sbt
RUN apt-get install -y openjdk-17-jdk
RUN curl -fL 'https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz' | gzip -d > cs && chmod +x cs && ./cs setup -y
ENV PATH="/root/.local/share/coursier/bin/:${PATH}"

# Install sqlite3 and duckdb command line tools (for interactive debugging)
RUN apt-get install -y sqlite3
RUN curl -fL 'https://github.com/duckdb/duckdb/releases/download/v1.1.1/duckdb_cli-linux-amd64.zip' --output 'duckdb.zip'
RUN unzip duckdb.zip -d /usr/bin/
RUN rm duckdb.zip

# `sbt update` is dependent on the build.sbt
WORKDIR /app
COPY build.sbt build.sbt
RUN sbt update

COPY src/ src/
COPY project/build.properties properties/build.properties

CMD ["sbt", "test"]
