# TPC-H dbgen

`dbgen` is the TPC-H benchmark data generator (version 2.17.3). In this
project it is wrapped so that all generated `.tbl` files are written to
`$TPCH_DATA_DIR` (`tpch_data/` at the project root).

## Basic Options

| Flag     | Description                                        |
| -------- | -------------------------------------------------- |
| `-s <n>` | Set Scale Factor — `1` ≈ 1 GB of data (default: 1) |
| `-f`     | Force overwrite of existing files                  |
| `-v`     | Verbose mode — show generation progress            |
| `-q`     | Quiet mode                                         |
| `-U <n>` | Generate `<n>` update sets                         |

## Per-Table Generation (`-T`)

| Flag   | Table(s)          |
| ------ | ----------------- |
| `-T c` | customer          |
| `-T o` | orders + lineitem |
| `-T O` | orders only       |
| `-T L` | lineitem only     |
| `-T p` | part + partsupp   |
| `-T P` | part only         |
| `-T S` | partsupp only     |
| `-T s` | supplier          |
| `-T l` | nation + region   |
| `-T n` | nation only       |
| `-T r` | region only       |

## Examples

```sh
# Generate the SF=1 (1 GB) validation database — most common
dbgen -vf -s 1

# Small-scale test data (~100 MB)
dbgen -vf -s 0.1

# Large-scale data (10 GB)
dbgen -vf -s 10

# Generate only the lineitem table at SF=1
dbgen -vf -s 1 -T L

# Generate 4 update sets at SF=1
dbgen -v -U 4 -s 1
```
