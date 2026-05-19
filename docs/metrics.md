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
- **Flink** times file preload as `load` and the entire `env.execute()` pipeline as `aju` (aggregate, top-K, and sink run inside Flink and are not split at L0).

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CQUIRREL_METRICS` | `on` | Set to `off` or `false` to disable |
| `CQUIRREL_METRICS_OUT` | (none) | Append one JSON line per run to this file |

## Example

```bash
export TPCH_DATA_DIR=/path/to/tpch
export CQUIRREL_METRICS_OUT=result/metrics.jsonl
mvn -q exec:java -Dexec.mainClass=hk.ust.StandaloneRunner
```

Console output ends with a summary table; optional JSON lines are appended to the output file.
