# Running CQuirrel

CQuirrel implements the AJU (Acyclic Join under Updates) algorithm for
incrementally maintaining TPC-H Query 10 results using Apache Flink.

## Prerequisites

| Requirement | Version | Notes                               |
| ----------- | ------- | ----------------------------------- |
| Java        | 21      | Zulu JDK recommended                |
| Maven       | 3.9.12  | Build system                        |
| TPC-H dbgen | 2.17.3  | Data generator (bundled via devenv) |
| DuckDB      | —       | Optional, for result verification   |

All prerequisites are automatically provided by the Nix development
environment. If you prefer manual setup, install Java 21 and Maven 3.9.12
yourself and build dbgen from the TPC-H tools source.

---

## Environment Setup

### Option A: devenv (recommended)

```sh
# Install devenv if you haven't already
# See https://devenv.sh/getting-started/

# Enter the development shell — sets up Java, Maven, dbgen, DuckDB,
# and all required environment variables automatically.
devenv shell
```

On entry, the shell prints the dbgen version to confirm the environment
is configured correctly.

### Option B: Manual setup

```sh
export TPCH_DATA_DIR=/path/to/your/tpch_data
export DUCKDB_RC=/path/to/project/.duckdbrc   # optional, for DuckDB verification

# Ensure java -version reports 21 and mvn --version reports 3.9.x
```

---

## Generating Test Data

CQuirrel reads pipe-delimited `.tbl` files produced by TPC-H dbgen.
The devenv wrapper writes all files to `$TPCH_DATA_DIR` (`./tpch_data/`
by default).

```sh
# Generate SF=0.1 (~100 MB) — good for quick testing
dbgen -vf -s 0.1

# Generate SF=1 (1 GB) — standard benchmark size
dbgen -vf -s 1

# Generate only the tables CQuirrel uses
dbgen -vf -s 0.1 -T l   # nation + region (nation.tbl needed)
dbgen -vf -s 0.1 -T c   # customer
dbgen -vf -s 0.1 -T o   # orders + lineitem

# Generate incremental update sets (for testing INSERT/DELETE processing)
dbgen -v -U 4 -s 0.1
```

CQuirrel requires these files in `$TPCH_DATA_DIR`:

- `nation.tbl`
- `customer.tbl`
- `orders.tbl`
- `lineitem.tbl`

See [docs/dbgen.md](dbgen.md) for full dbgen usage reference.

---

## Building

```sh
# Compile only
mvn compile

# Full verification: compile + Error Prone static analysis + format check
mvn verify

# Auto-format source code (google-java-format via Spotless)
mvn spotless:apply

# Check formatting without modifying files
mvn spotless:check
```

The build produces `target/cquirrel-0.1.0.jar`.

---

## Running

CQuirrel provides two entry points: a Flink streaming job and a
standalone single-threaded runner.

Both entry points share the same core: [`Q10BatchEngine`](src/main/java/hk/ust/engine/Q10BatchEngine.java)
(AJU + aggregation + top-K materialization). Input batching and output timing are configured
independently:

| Variable / property | Default | Description |
| ------------------- | ------- | ----------- |
| `CQUIRREL_OUTPUT_POLICY` / `q10.output.policy` | `ON_JOB_END` | When to emit top-K: `ON_EACH_DELTA`, `ON_BATCH_END`, `ON_JOB_END` |
| `CQUIRREL_INPUT_BATCH_SIZE` / `q10.input.batch.size` | unbounded (`0`) | Tuple updates per input batch; `1` = one update per batch |

**Output policies**

- `ON_JOB_END` — bulk snapshot at end (default; matches paper-style eval)
- `ON_BATCH_END` — emit current top-20 after each input batch (Standalone also ends a batch after each `.tbl`)
- `ON_EACH_DELTA` — emit only when the top-20 set changes (finest incremental output)

Incremental policies write snapshot CSVs next to the main output file, e.g.
`flink-q10.csv.batch_end.3.csv`.

### Standalone Runner (recommended for quick verification)

Runs the AJU algorithm in a single thread without Flink overhead. Useful
for correctness verification and profiling.

```sh
mvn -q exec:java -Dexec.mainClass=hk.ust.StandaloneRunner
```

Or with an explicit classpath:

```sh
java -cp target/cquirrel-0.1.0.jar:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout) \
  hk.ust.StandaloneRunner
```

**Output:** A formatted table showing the top 20 customers by revenue
(matching TPC-H Q10 semantics).

### Flink Streaming Job

Runs the full AJU pipeline as a Flink streaming application:
TpchUpdateSource -> Q10UnifiedBatchFunction (AJU + batch aggregate + top-K) -> discarding sink

```sh
mvn -q exec:java -Dexec.mainClass=hk.ust.AjuStreamJob
```

Or with an explicit classpath:

```sh
java -cp target/cquirrel-0.1.0.jar:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout) \
  hk.ust.AjuStreamJob
```

The streaming job also processes incremental update files (`*.tbl.u1`,
`*.tbl.u2`, ...) if present in `$TPCH_DATA_DIR`, simulating a stream
of inserts and deletes.

---

## Verifying Correctness

Compare CQuirrel output against DuckDB running the reference Q10 query:

```sh
duckdb
```

(The devenv wrapper auto-loads `.duckdbrc` which installs the TPC-H
extension.)

```sql
-- Load data
CREATE TABLE nation AS SELECT * FROM read_csv('tpch_data/nation.tbl', delim='|', header=false);
CREATE TABLE customer AS SELECT * FROM read_csv('tpch_data/customer.tbl', delim='|', header=false);
CREATE TABLE orders AS SELECT * FROM read_csv('tpch_data/orders.tbl', delim='|', header=false);
CREATE TABLE lineitem AS SELECT * FROM read_csv('tpch_data/lineitem.tbl', delim='|', header=false);

-- Or use the TPC-H extension directly
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=0.1);

-- Run Q10
SELECT c_custkey, c_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= date '1993-10-01'
  AND o_orderdate < date '1993-10-01' + interval '3' month
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20;
```

The top-20 customer keys and revenue values should match CQuirrel's
output exactly.

---

## CI

The GitHub Actions workflow (`.github/workflows/test.yml`) runs on push
to `main` and on pull requests, across three platforms:

- Ubuntu latest (x86_64)
- macOS latest (ARM)
- Ubuntu 24.04 (ARM)

It invokes `devenv test`, which currently verifies that the development
environment bootstraps correctly (dbgen is available).

---

## Quick Start (TL;DR)

```sh
devenv shell              # enter dev environment
dbgen -vf -s 0.1          # generate small test data
mvn verify                # build + lint + format check
mvn -q exec:java -Dexec.mainClass=hk.ust.StandaloneRunner   # run
```
