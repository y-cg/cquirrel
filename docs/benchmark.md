# CQuirrel Benchmark Guide

This guide covers how to run the four common benchmark cases for CQuirrel's AJU
implementation of TPC-H Q10, and how to verify correctness against DuckDB.

**Contents**

- [Environment Setup](#environment-setup)
- [Generating Test Data](#generating-test-data)
- [Building](#building)
- [Case 1 — Standalone, bulk snapshot (`ON_JOB_END`)](#case-1--standalone-bulk-snapshot-on_job_end)
- [Case 2 — Standalone, incremental batch output (`ON_BATCH_END`)](#case-2--standalone-incremental-batch-output-on_batch_end)
- [Case 3 — Flink streaming, bulk snapshot (`ON_JOB_END`)](#case-3--flink-streaming-bulk-snapshot-on_job_end)
- [Case 4 — Flink streaming with incremental updates](#case-4--flink-streaming-with-incremental-updates)
- [Correctness Verification with DuckDB](#correctness-verification-with-duckdb)
- [Reading the Output](#reading-the-output)
- [Understanding the Metrics](#understanding-the-metrics)

---

## Environment Setup

### devenv (recommended)

```sh
devenv shell
```

This provides Java 21, Maven 3.9, dbgen, and DuckDB automatically. The shell
prints the dbgen version on entry to confirm the environment is ready.

### Manual

Ensure `java -version` reports 21 and `mvn --version` reports 3.9.x. Set:

```sh
export TPCH_DATA_DIR=/path/to/tpch_data   # directory containing .tbl files
export DUCKDB_RC=.duckdbrc                 # optional, loads TPC-H extension
```

---

## Generating Test Data

CQuirrel reads pipe-delimited `.tbl` files produced by TPC-H dbgen. All files
are written to `$TPCH_DATA_DIR` (defaults to `./tpch_data/`).

```sh
# SF=0.1 (~100 MB) — good for quick testing and iteration
dbgen -vf -s 0.1

# SF=1 (1 GB) — standard benchmark size for throughput measurement
dbgen -vf -s 1
```

CQuirrel uses exactly four tables. To generate only those tables:

```sh
dbgen -vf -s 0.1 -T l   # nation + region
dbgen -vf -s 0.1 -T c   # customer
dbgen -vf -s 0.1 -T o   # orders + lineitem
```

### Incremental update sets (Case 4 only)

To test INSERT/DELETE processing, generate update files with the `-U` flag:

```sh
dbgen -v -U 4 -s 0.1
```

This produces `*.tbl.u1`, `*.tbl.u2`, ... update files. Each file contains
alternating DELETE/INSERT pairs. CQuirrel's source processes these after the
initial bulk load.

---

## Building

```sh
# Compile only
mvn compile

# Full verification: compile + Error Prone + format check
mvn verify
```

The build produces `target/cquirrel-0.1.0.jar`.

---

## Case 1 — Standalone, bulk snapshot (`ON_JOB_END`)

The standalone runner executes AJU in a single thread without Flink overhead.
`ON_JOB_END` (default) emits the top-20 only at the very end — matching the
paper-style bulk evaluation.

```sh
# Default: ON_JOB_END
mvn -q exec:java -Dexec.mainClass=hk.ust.StandaloneRunner

# Explicit
CQUIRREL_OUTPUT_POLICY=ON_JOB_END mvn -q exec:java -Dexec.mainClass=hk.ust.StandaloneRunner
```

**Output:** `result/standalone-q10.csv` — one CSV with the final top-20 customers.

**Use when:** You want a baseline throughput number (no Flink overhead), or a
reference result for correctness checking. This is the fastest single-run mode.

**Expected metrics:** `processing_time_ms` = `aju` + `aggregate` + `topk` +
`sink` combined. No incremental outputs are written during the run.

---

## Case 2 — Standalone, incremental batch output (`ON_BATCH_END`)

`ON_BATCH_END` emits the current top-20 after each input batch boundary.
For the standalone runner, a "batch" ends when a table file finishes loading.

```sh
CQUIRREL_OUTPUT_POLICY=ON_BATCH_END mvn -q exec:java -Dexec.mainClass=hk.ust.StandaloneRunner
```

**Output:** `result/standalone-q10.csv` (final snapshot) plus intermediate
snapshots named `result/standalone-q10.csv.batch_end.N.csv` after each of the
four tables (nation, customer, orders, lineitem).

**Use when:** Verifying that top-K results are consistent across batch
boundaries, or measuring how quickly the top-20 stabilizes as more data arrives.

**Batch boundary timing:** The aggregator's top-K changes only when join results
from the new table affect the ranking. Typically the largest change occurs after
`lineitem` (the root table) finishes loading.

---

## Case 3 — Flink streaming, bulk snapshot (`ON_JOB_END`)

The Flink job runs the full AJU pipeline as a streaming application:
`TpchUpdateSource` → `Q10UnifiedBatchFunction` (AJU + aggregate + top-K) →
discarding sink.

```sh
# Must use exec:exec (not exec:java) to avoid class-loader serialization issues
mvn -q compile exec:exec \
  -Dexec.executable=java \
  -Dexec.classpathScope=runtime \
  -Dexec.args="-classpath %classpath hk.ust.AjuStreamJob"
```

Or via the justfile shortcut:

```sh
just streamjob
```

**Output:** Console only (discarding sink). The `FlinkBreakdownReporter` prints
a standalone-aligned phase breakdown to stdout after the job completes:

```
Phase breakdown (standalone-aligned):
  load:     1234 ms
  aju:       567 ms
  aggregate:  45 ms
  topk:       12 ms
  sink:        3 ms
  runtime:   234 ms   # Flink per-record dispatch
  cluster:   890 ms   # MiniCluster startup/teardown
```

**Use when:** Measuring true streaming performance with Flink's operator
overhead, or evaluating the AJU algorithm in a streaming runtime context.

**Metrics file:** If `CQUIRREL_METRICS_OUT` is set, one JSON line per run is
appended to that file (e.g. `result/metrics.jsonl`).

---

## Case 4 — Flink streaming with incremental updates

This case extends Case 3 by processing update files (`*.tbl.u1`, etc.) generated
by `dbgen -U <n>`. The source first bulk-loads all four tables, then processes
each update set as a DELETE/INSERT pair stream.

```sh
# 1. Generate base data + update sets
dbgen -vf -s 0.1          # base .tbl files
dbgen -v -U 4 -s 0.1      # 4 update sets (DELETE/INSERT pairs)

# 2. Run with ON_EACH_DELTA to observe every change
CQUIRREL_OUTPUT_POLICY=ON_EACH_DELTA mvn -q compile exec:exec \
  -Dexec.executable=java \
  -Dexec.classpathScope=runtime \
  -Dexec.args="-classpath %classpath hk.ust.AjuStreamJob"
```

**Output policies for incremental updates:**

| Policy | When output is emitted |
|--------|-----------------------|
| `ON_JOB_END` | Only a single snapshot after all updates are processed |
| `ON_BATCH_END` | After each input batch boundary (each update file) |
| `ON_EACH_DELTA` | Whenever the top-20 set changes after a join delta |

**Use when:** Measuring AJU's incremental maintenance property — the algorithm
updates only affected groups in O(1) per tuple, so even with many update files
the processing cost remains proportional to the number of affected join results.

**Output:** Intermediate snapshots written as `result/flink-q10.csv.delta.N.csv`
whenever the top-20 set changes. The final result still appears in the console
breakdown.

---

## Correctness Verification with DuckDB

Compare CQuirrel's output against DuckDB running the reference TPC-H Q10 query.

```sh
# 1. Generate test data
dbgen -vf -s 0.1

# 2. Run CQuirrel standalone (or Flink)
mvn -q exec:java -Dexec.mainClass=hk.ust.StandaloneRunner

# 3. Run the reference query in DuckDB
duckdb -init /dev/null -cmd ".mode csv" < query/q10-from-tbl.sql > result/duckdb-q10.csv
```

The top-20 customer keys (`c_custkey`) and revenue values should match exactly.
Compare with your favourite diff tool:

```sh
diff <(cut -d, -f1,3 result/standalone-q10.csv | sort) \
      <(cut -d, -f1,3 result/duckdb-q10.csv | sort)
```

> The `.duckdbrc` at the project root installs the TPC-H extension, but
> `query/q10-from-tbl.sql` loads data from the `.tbl` files directly (no TPC-H
> extension needed).

---

## Reading the Output

### Standalone CSV (`result/standalone-q10.csv`)

```csv
c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment
12345,Customer#000012345,9876543.21,5000.00,GERMANY,...
...
```

The columns match TPC-H Q10's SELECT output, ordered by `revenue DESC`.
The file has no header row when emitted by the batch engine (DuckDB's `.mode csv`
produces a boxed format with a header — the two are not directly comparable
without processing).

### Flink output (console)

The Flink job uses a discarding sink, so all output is printed to the console
breakdown table. The phase breakdown table shows wall time per phase, not the
query result itself. For result verification, use the standalone runner.

### Incremental snapshots

When `ON_BATCH_END` or `ON_EACH_DELTA` is active, additional CSV files are
written next to the main output:

```
result/
├── standalone-q10.csv                 # final snapshot
├── standalone-q10.csv.batch_end.1.csv  # after nation
├── standalone-q10.csv.batch_end.2.csv  # after customer
├── standalone-q10.csv.batch_end.3.csv  # after orders
├── standalone-q10.csv.batch_end.4.csv  # after lineitem
└── duckdb-q10.csv                     # reference from DuckDB
```

---

## Understanding the Metrics

Each run produces a JSON line in `result/metrics.jsonl` (when
`CQUIRREL_METRICS_OUT` is set):

```json
{
  "runner": "standalone",
  "tpch_data_dir": "/path/to/tpch_data",
  "updates_total": 481661,
  "join_deltas_total": 13420,
  "phases_ms": {
    "load": 312,
    "aju": 890,
    "aggregate": 45,
    "topk": 12,
    "sink": 3
  },
  "wall_time_ms": 1215,
  "processing_time_ms": 950,
  "throughput": {
    "updates_per_sec": 507011,
    "join_deltas_per_sec": 14126
  },
  "memory": {
    "heap_used_mb_peak": 512,
    "heap_max_mb": 2048
  }
}
```

**Key fields:**

| Field | Meaning |
|-------|---------|
| `updates_total` | Tuple updates processed (all 4 tables combined) |
| `join_deltas_total` | Join results emitted from AJU (only non-filtered tuples become alive) |
| `processing_time_ms` | `aju` + `aggregate` + `topk` + `sink` (excludes `load`) |
| `throughput.updates_per_sec` | Input throughput = `updates_total / processing_time_ms` |
| `throughput.join_deltas_per_sec` | AJU throughput = `join_deltas_total / processing_time_ms` |

**Flink-specific fields:**

| Field | Meaning |
|-------|---------|
| `memory.preload_updates_count` | 0 for Flink (fromCollection has no pre-load count) |
| `phases_ms.runtime` | Per-record dispatch overhead inside Flink operators |
| `phases_ms.cluster` | MiniCluster startup/teardown not attributed to operators |

> For Flink, the cluster and runtime phases are excluded from
> `processing_time_ms`, making standalone and Flink processing costs directly
> comparable.