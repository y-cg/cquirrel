standalone output="result/standalone-q10.csv" parallelism="1":
    mvn -q exec:java -Dexec.mainClass=hk.ust.StandaloneRunner -Dq10.output={{output}} -Dq10.parallelism={{parallelism}}


streamjob output="result/flink-q10.csv":
    mvn -q compile exec:exec -Dexec.executable=java -Dexec.classpathScope=runtime -Dexec.args="-Dq10.output={{output}} -classpath %classpath hk.ust.AjuStreamJob"


# Run TPC-H Q10 via DuckDB against the generated .tbl files for correctness verification
q10-duckdb:
    mkdir -p result
    duckdb -init /dev/null -cmd ".mode csv" < query/q10-from-tbl.sql > result/duckdb-q10.csv
