# Learnings: duckdb_objfs on OneLake

## TPC-H SF10, DuckDB database stored in OneLake (2026-10-18)

Setup: `tpchgen-cli` SF10 (3.7 GiB parquet) loaded into `duckdb_objfs://tpch_sf10.db` on OneLake,
then the 22 queries from a fresh process. GitHub `ubuntu-latest` (4 vCPU, 16 GB). Fork CI run
39122557565. Every query result matched the same query on the local parquet.

| | duckdb_objfs `.db` | duckdb_iceberg (lakehouse_benchmark, same SF) |
|---|---:|---:|
| Load + checkpoint | 96 s | n/a |
| 22 queries, cold | 140 s | 44 s |
| 22 queries, warm | 6.3 s | 23.9 s |
| 22 queries, local parquet | 14 s | n/a |

**Verdict: built for transactional durability on object storage, not for cold OLAP reads.**
Fine once the data sits in DuckDB's buffer pool.

What the I/O counters (`duckdb_objfs_io_stats()`) showed:

- **Cold reads are latency-bound.** 11,947 GETs, 3.75 GB, about 66 ms each, about 300 KB per
  request with roughly 6 in flight (one per DuckDB thread): about 27 MB/s. The file is stored as
  256 KiB chunks (`rust/src/file_metadata.rs`), read-ahead is capped at 4 MiB
  (`rust/src/chunk_manager.rs`). OLAP scans want large ranges with many requests in flight.
- **Read amplification.** The cold pass read 3.75 GB, which looks like more than the columns the
  22 queries touch.
- **Write amplification.** The load wrote 7.7 GB and read back 3.3 GB for 3.7 GiB of input. That
  comes from SlateDB flush and compaction.
- **The extension's own cache is small.** SlateDB's in-memory block cache defaults to 512 MiB, and
  the persistent disk cache (`duckdb_objfs_persistent_cache_path`) is off by default. The fast warm
  run comes from DuckDB's buffer pool, not from the extension. A new process starts cold, and so
  does every query that touches columns no earlier query read (Q04, Q05, Q13 each took 16-18 s cold).
- **Execution is not the problem.** Warm, the 22 queries beat local parquet. The gap is only in
  getting bytes from OneLake.

Possible improvements (not done): larger range reads, prefetch with many requests in flight, and
finding where the extra reads come from. A local disk cache would hide the cold-read cost rather
than fix it.

Other things learnt while writing the test:

- DuckDB's `tpch_answers()` only goes up to SF1. At SF10 the check is the same query on the local
  parquet instead.
- DuckDB's built-in Q11 hard-codes the SF1 fraction (0.0001) and returns 0 rows at SF10. The spec
  value is 0.0001 / SF (8,685 rows at SF10).
- This build has no ICU, so subtracting two `TIMESTAMP WITH TIME ZONE` values fails to bind. Use
  `epoch_us()`.
