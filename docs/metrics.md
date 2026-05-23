# L0 Run Metrics

CQuirrel collects **run-level (L0)** metrics at the end of each execution: phase timings, throughput, and peak JVM heap usage.

## Metrics

| Field | Description |
|-------|-------------|
| `updates_total` | Tuple updates processed (input count) |
| `join_deltas_total` | Join deltas emitted (standalone only; Flink reports `0`) |
| `phases_ms` | `load`, `aju`, `aggregate`, `topk`, `sink` |
| `wall_time_ms` | Total wall time including load |
| `processing_time_ms` | Sum of `aju` + `aggregate` + `topk` + `sink` (excludes `load`) |
| `throughput.updates_per_sec` | `updates_total / processing_time` |
| `throughput.join_deltas_per_sec` | `join_deltas_total / processing_time` |
| `memory.heap_used_mb_peak` | Peak heap used during the run |
| `memory.heap_max_mb` | Configured max heap (`-Xmx`) |
| `memory.preload_updates_count` | Pre-loaded updates in Flink (`fromCollection`) |

### Flink vs standalone

- **Standalone** splits `load` (parse) vs `aju` (`processElement`), and times `aggregate`, `topk`, `sink` separately.
- **Flink** records the same phase names inside `env.execute()` via operator instrumentation. Additional Flink-only phases:
  - `runtime` — `Source.collect` and per-record operator dispatch
  - `cluster` — MiniCluster startup/teardown not attributed to operators
- After each Flink run, a **standalone-aligned breakdown** is printed to stdout.
- **`processing_time_ms`** (both runners) = `aju` + `aggregate` + `topk` + `sink` (excludes `load`, `runtime`, `cluster`).

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CQUIRREL_METRICS` | `on` | Set to `off` or `false` to disable |
| `CQUIRREL_METRICS_OUT` | `result/metrics.jsonl` in devenv shell | Append one JSON line per run to this file |
| `CQUIRREL_OUTPUT_POLICY` | `ON_JOB_END` | See [docs/running.md](running.md) |
| `CQUIRREL_INPUT_BATCH_SIZE` | unbounded | Input batch size for `Q10BatchEngine` |

## Example

```bash
export TPCH_DATA_DIR=/path/to/tpch
export CQUIRREL_METRICS_OUT=result/metrics.jsonl
mvn -q exec:java -Dexec.mainClass=hk.ust.StandaloneRunner
```

Console output ends with a summary table; optional JSON lines are appended to the output file.
