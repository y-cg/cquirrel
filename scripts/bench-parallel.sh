#!/usr/bin/env bash
set -euo pipefail

# Parallelism scaling sweep for Flink Case 3 (bulk snapshot, ON_JOB_END).
# Usage: ./scripts/bench-parallel.sh [1 2 4 8]

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -z "${TPCH_DATA_DIR:-}" ]]; then
  export TPCH_DATA_DIR="$ROOT/tpch_data"
fi

if [[ ! -f "$TPCH_DATA_DIR/lineitem.tbl" ]]; then
  echo "Generating SF=0.1 TPC-H data in $TPCH_DATA_DIR ..."
  dbgen -vf -s 0.1
fi

PARALLELISMS=("$@")
if [[ ${#PARALLELISMS[@]} -eq 0 ]]; then
  PARALLELISMS=(1 2 4)
fi

METRICS_OUT="${CQUIRREL_METRICS_OUT:-result/scaling.jsonl}"
mkdir -p result
: > "$METRICS_OUT"

PIPELINE="${CQUIRREL_PIPELINE:-unified}"

echo "Writing scaling metrics to $METRICS_OUT (pipeline=$PIPELINE)"

for p in "${PARALLELISMS[@]}"; do
  echo "--- CQUIRREL_PARALLELISM=$p ---"
  CQUIRREL_PARALLELISM="$p" \
    CQUIRREL_PIPELINE="$PIPELINE" \
    CQUIRREL_METRICS_OUT="$METRICS_OUT" \
    mvn -q compile exec:exec \
      -Dexec.executable=java \
      -Dexec.classpathScope=runtime \
      -Dexec.args="-classpath %classpath hk.ust.AjuStreamJob"
done

echo
echo "=== Scaling summary (last run per parallelism) ==="
python3 - <<'PY'
import json
from collections import OrderedDict
from pathlib import Path
import os

path = Path(os.environ.get("CQUIRREL_METRICS_OUT", "result/scaling.jsonl"))
if not path.exists():
    print("No metrics file:", path)
    raise SystemExit(0)

latest = OrderedDict()
with path.open() as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        latest[row.get("parallelism", "?")] = row

print(f"{'p':>4}  {'pipeline':<12}  {'updates/s':>12}  {'deltas/s':>12}  {'proc_ms':>10}")
for p, row in latest.items():
    tp = row.get("throughput", {})
    print(
        f"{p:>4}  {row.get('pipeline_mode','?'):<12}  "
        f"{tp.get('updates_per_sec', 0):>12.0f}  "
        f"{tp.get('join_deltas_per_sec', 0):>12.0f}  "
        f"{row.get('processing_time_ms', 0):>10}"
    )
PY
