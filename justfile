standalone output="result/standalone-q10.csv":
    mvn -q exec:java -Dexec.mainClass=hk.ust.StandaloneRunner -Dq10.output={{output}}


streamjob output="result/flink-q10.csv":
    mvn -q compile exec:exec -Dexec.executable=java -Dexec.classpathScope=runtime -Dexec.args="-Dq10.output={{output}} -classpath %classpath hk.ust.AjuStreamJob"


# Sweep CQUIRREL_PARALLELISM values and append metrics to result/scaling.jsonl
bench-scaling pipeline="unified" *parallelisms:
    #!/usr/bin/env bash
    set -euo pipefail
    export CQUIRREL_PIPELINE="{{pipeline}}"
    args=()
    for p in {{parallelisms}}; do args+=("$p"); done
    ./scripts/bench-parallel.sh "${args[@]}"


# Run TPC-H Q10 via DuckDB against the generated .tbl files for correctness verification
q10-duckdb:
    mkdir -p result
    duckdb -init /dev/null -cmd ".mode csv" < query/q10-from-tbl.sql > result/duckdb-q10.csv
