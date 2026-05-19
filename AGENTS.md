# AGENTS.md

## Cursor Cloud specific instructions

This is a single-module Java 21 / Maven project (CQuirrel) that implements incremental TPC-H Q10 maintenance using Apache Flink. No external services (databases, queues, containers) are required.

### Prerequisites (installed by update script)

- **Java 21** (pre-installed on Ubuntu 24.04 VMs)
- **Maven 3.9.x** (installed to `/opt/apache-maven-3.9.16`)
- **TPC-H dbgen** (built from source at `/opt/tpch-dbgen`)

### Environment variables

- `TPCH_DATA_DIR=/workspace/tpch_data` — set in `~/.bashrc`; required by both entry points to locate `.tbl` data files.

### Generating test data

```sh
dbgen -vf -s 0.1   # SF=0.1, ~100MB, quick testing
```

The `dbgen` wrapper at `/usr/local/bin/dbgen` auto-creates `$TPCH_DATA_DIR` and runs from there.

### Build & lint

See `README.md` for standard commands. Key points:

- `mvn compile` — compiles and runs Error Prone static analysis (the project's lint step).
- `mvn verify` — compile + Error Prone + Spotless format check. Note: `DataStreamJob.java` has a pre-existing Spotless formatting violation; `mvn verify` will fail on the format check until that file is fixed.
- `mvn spotless:apply` — auto-fix formatting.
- `mvn spotless:check` — check formatting only.

### Running the application

- **Standalone runner** (recommended for quick verification):
  `mvn -q exec:java -Dexec.mainClass=hk.ust.StandaloneRunner`
- **Flink streaming job** — must use `exec:exec` (not `exec:java`) to avoid class-loader serialization issues:
  `mvn -q compile exec:exec -Dexec.executable=java -Dexec.classpathScope=runtime -Dexec.args="-classpath %classpath hk.ust.AjuStreamJob"`
  The `justfile` provides shortcuts: `just standalone` and `just streamjob`.

### Gotchas

- The Flink streaming job fails with `exec:java` due to a serialization error (`Could not deserialize stream node`). Always use `exec:exec` or the `just streamjob` shortcut.
- There are no unit tests (`src/test/` does not exist). CI only runs `devenv test` which verifies the Nix environment bootstraps correctly.
- The `result/` directory is not gitignored; be aware of accidental commits of output CSVs.
