# RelAlg MLIR Generation from TyQL

This directory contains tests and reference files for generating LingoDB-compatible `relalg` MLIR dialect from TyQL queries.

## 1. Approach

TyQL compiles Scala query expressions into a `QueryIR` tree (filter, project, join, aggregate, sort, etc.). The `RelAlgGenerator` (in `ToRelAlg.scala`) walks this IR and programmatically constructs an MLIR module using [scair](https://github.com/edin-dal/scair) — a Scala library for building and printing MLIR programs.

The pipeline is:

```
Scala DSL query → TyQL QueryIR → RelAlgGenerator → scair Operation tree → MLIR text (relalg dialect)
```

For each QueryIR node, the generator creates the corresponding `relalg` operation (e.g. `Selection`, `Aggregation`, `MapOp`) with nested regions containing `tuples.getcol`, `db.compare`, `db.add`, etc. The scair `Printer` then serializes the operation tree to MLIR text using custom printers that match LingoDB's expected assembly format.

The generated MLIR can be:
- **Validated** by LingoDB's `mlir-db-opt` parser/verifier
- **Executed** by LingoDB's `run-mlir` against a TPC-H database (requires the execution wrapper — see the reference files in `mlir/`)

## 2. Scair Dialect Additions

Three new dialects were added to scair to support LingoDB's relational algebra IR.

The published scair release did not yet include `DerivedOperation` `customPrint` support, which is needed for the LingoDB dialect operations (LingoDB uses custom assembly format, not generic MLIR format). This functionality was already available on the main branch, so the dialects were built on top of main and locally published.

The changes are on the [`relalg-tyql`](https://github.com/aherlihy/scair/tree/relalg-tyql) branch and must be locally published before use with TyQL:

```bash
git clone https://github.com/aherlihy/scair.git
cd scair
git checkout relalg-tyql
mill __.publishLocal
```

TyQL's `build.sbt` depends on the locally published snapshot:

```scala
"io.github.edin-dal" %% "scair-dialects" % "0.0.0-339-f04bd1-DIRTY12dc25b-SNAPSHOT"
```

### `db` dialect (`dialects/src/db/DB.scala`)

Types:
- `DecimalType(p, s)` — `!db.decimal<p,s>` (e.g. `!db.decimal<12,2>`)
- `DateType(unit)` — `!db.date<unit>` (e.g. `!db.date<day>`)
- `CharType(len)` — `!db.char<len>` (e.g. `!db.char<1>`)
- `DBStringType()` — `!db.string`
- `NullableType(inner)` — `!db.nullable<inner>`

Operations:
- `DBConstant` — `db.constant("value") : type`
- `DBCompare` — `db.compare <pred> %lhs : type, %rhs : type` (predicates: eq, neq, lt, lte, gt, gte)
- `DBAnd` / `DBOr` / `DBNot` — logical connectives
- `DBAdd` / `DBSub` / `DBMul` / `DBDiv` — arithmetic (result types follow LingoDB's decimal promotion rules)
- `DBCast` — `db.cast %val : srcType -> dstType`

### `tuples` dialect (`dialects/src/tuples/Tuples.scala`)

Types:
- `TupleStreamType()` — `!tuples.tuplestream`
- `TupleType()` — `!tuples.tuple`

Attributes:
- `ColumnRefAttr(scope, name)` — prints as `@scope::@name`
- `ColumnDefAttr(scope, name, type)` — prints as `@scope::@name({type = ...})`

Operations:
- `GetCol` — `tuples.getcol %tuple @scope::@col : type`
- `TuplesReturn` — `tuples.return %vals : types` (region terminator)

### `relalg` dialect (`dialects/src/relalg/RelAlg.scala`)

Operations:
- `BaseTable` — declares a table with column definitions and datasource
- `Selection` — filter with a predicate region
- `MapOp` — compute new columns via a mapping region
- `InnerJoin` / `CrossProduct` — join operators
- `Aggregation` — group-by with aggregate function computations
- `AggrFn` — individual aggregate functions (sum, avg, min, max)
- `CountRows` — `relalg.count`
- `Sort` — order by column specifications
- `Limit` — row limit
- `Projection` — column projection (distinct/all)
- `Materialize` — materialize columns with output names

All operations use **custom printers** because LingoDB uses a custom assembly format, not the generic MLIR format.

### Registration

All three dialects are registered in `dialects/src/AllDialects.scala`.

## 3. Getting Started

### Step 1: Build and publish scair locally

```bash
git clone https://github.com/aherlihy/scair.git
cd scair
git checkout relalg-tyql
mill __.publishLocal
```

### Step 2: Clone TyQL and run the tests

```bash
git clone https://github.com/aherlihy/tyql.git
cd tyql
git checkout scair
sbt 'testOnly test.relalg.*'
```

This runs the TPC-H RelAlg tests (Q1, Q3, Q6, Q10) and writes MLIR to `src/test/scala/test/relalg/out/`:
- `q*.mlir` — bare relalg MLIR (for validation with `mlir-db-opt`)
- `exec-q*.mlir` — executable MLIR wrapped in `func.func @main()` (for execution with `run-mlir`)

### Step 3: Build LingoDB (for validation/execution)

Clone and build LingoDB inside Docker. The LingoDB repo provides a dev Dockerfile:

```bash
git clone https://github.com/lingo-db/lingo-db.git
cd lingo-db

# Build the dev Docker image
docker build -t lingodb-dev -f tools/docker/Dockerfile --target devimg .

# Start the container, mounting the project directories and a location for the TPC-H database
# Replace <WORKSPACE> with the parent directory containing lingo-db and tyql
docker run -d --name lingodb-dev \
  -v <WORKSPACE>:/workspace \
  -v /tmp/tpch-db:/tmp/tpch-db \
  lingodb-dev:latest tail -f /dev/null

# Build LingoDB inside the container
docker exec lingodb-dev bash -c "cd /workspace/lingo-db && make build/lingodb-debug/.buildstamp"
```

### Step 4: Create the TPC-H database

```bash
# Generate TPC-H CSV data (scale 0.01) into a temp directory
docker exec lingodb-dev bash -c "cd /workspace/lingo-db && tools/generate/tpch.sh /tmp/tpch-csv 0.01"

# Create the database — must run from the CSV directory since initialize.sql uses relative COPY paths
docker exec lingodb-dev bash -c "cd /tmp/tpch-csv && /workspace/lingo-db/build/lingodb-debug/sql /tmp/tpch-db < /workspace/lingo-db/resources/sql/tpch/initialize.sql"
```

### Step 5: Validate and execute MLIR

**Validate** (parse/verify, no database needed):

```bash
docker exec lingodb-dev /workspace/lingo-db/build/lingodb-debug/mlir-db-opt \
  /workspace/tyql/src/test/scala/test/relalg/out/q1.mlir
```

**Execute against TPC-H database:**

```bash
docker exec lingodb-dev /workspace/lingo-db/build/lingodb-debug/run-mlir \
  /workspace/tyql/src/test/scala/test/relalg/out/exec-q6.mlir /tmp/tpch-db
```

The `exec-q*.mlir` files are wrapped in `func.func @main()` with `relalg.query`, `relalg.materialize`, `relalg.query_return`, and `subop.set_result`, which is required by `run-mlir`. The bare `q*.mlir` files can only be validated with `mlir-db-opt`.

### Generating reference MLIR from SQL

The `sql-to-mlir` tool converts SQL queries to MLIR, but it segfaults after printing output. Use this workaround to capture the output:

```bash
docker exec lingodb-dev bash -c \
  "script -qc '/workspace/lingo-db/build/lingodb-debug/sql-to-mlir \
    /workspace/tyql/src/test/scala/test/query/sql/q1.sql /tmp/tpch-db' \
    /dev/null 2>/dev/null" \
  | sed 's/\r//g' | grep -v "Segmentation fault" > q1_ref.mlir
```

## 4. Directory Structure

```
src/test/scala/test/relalg/
├── README.md                  # This file
├── TPCHRelAlgTests.scala      # TPC-H Q1, Q3, Q6, Q10 test definitions
├── mlir/                      # Reference MLIR from sql-to-mlir (with execution wrapper)
│   ├── q1.mlir
│   ├── q3.mlir
│   ├── q6.mlir
│   └── q10.mlir
└── out/                       # TyQL-generated MLIR (written by tests)
    ├── q1.mlir                # bare relalg (for mlir-db-opt validation)
    ├── q3.mlir
    ├── q6.mlir
    ├── q10.mlir
    ├── exec-q1.mlir           # executable (for run-mlir execution)
    ├── exec-q3.mlir
    ├── exec-q6.mlir
    └── exec-q10.mlir
```

