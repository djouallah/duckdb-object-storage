#!/usr/bin/env bash
# Issue repro: READ_ONLY attach held while another process writes and checkpoints.
DUCKDB=${DUCKDB:-$PWD/build/release/duckdb}
cd "$(mktemp -d)"
STORE=$(mktemp -d)
setup="LOAD duckdb_object_storage; SET duckdb_objfs_root = '$STORE';"

$DUCKDB -c "$setup ATTACH 'duckdb_objfs://readers.db' AS obj;
  CREATE TABLE obj.filler AS SELECT 'b' || (range // 50000) AS batch, range % 50000 AS i,
    md5(('b' || (range // 50000)) || (range % 50000)) AS payload FROM range(80 * 50000);
  CHECKPOINT obj;"

{ echo "$setup ATTACH 'duckdb_objfs://readers.db' AS obj;"
  for s in $(seq 1 20000); do
    echo "BEGIN; DELETE FROM obj.filler WHERE batch = 'b$((s - 1))';"
    echo "INSERT INTO obj.filler SELECT 'b$((s + 79))', range, md5('b$((s + 79))' || range) FROM range(50000); COMMIT;"
    if (( s % 3 == 0 )); then echo "CHECKPOINT obj;"; fi
  done; } > writer.sql

{ echo "$setup SET memory_limit = '64MB'; SET threads = 2;"
  echo "ATTACH 'duckdb_objfs://readers.db' AS obj (READ_ONLY);"
  for q in $(seq 1 200); do
    echo "SELECT count(*) AS row_count, count(DISTINCT batch) AS batches,
      count(*) FILTER (WHERE payload IS DISTINCT FROM md5(batch || i)) AS bad_payloads FROM obj.filler;"
  done; } > reader.sql

$DUCKDB -bail -f writer.sql > writer.log 2>&1 &
sleep 3
for r in 1 2 3; do $DUCKDB -csv -noheader -f reader.sql > reader_$r.log 2>&1 & done
sleep 120; kill -9 %1 %2 %3 %4; wait
echo "answers by value:"; grep -h -E '^[0-9]+,[0-9]+,[0-9]+$' reader_*.log | sort | uniq -c
echo "errors:"; grep -h -i error reader_*.log | sort | uniq -c
