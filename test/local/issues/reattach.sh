#!/usr/bin/env bash
# Issue repro: a fenced process can never attach the database again.
DUCKDB=${DUCKDB:-$PWD/build/release/duckdb}
cd "$(mktemp -d)"
STORE=$(mktemp -d)
setup="LOAD duckdb_object_storage; SET duckdb_objfs_root = '$STORE';"
mkfifo a.in
$DUCKDB < a.in > a.log 2>&1 &
exec 5> a.in
echo "$setup ATTACH 'duckdb_objfs://reattach.db' AS obj;
      CREATE TABLE obj.t AS SELECT range AS i FROM range(1000);" >&5
sleep 5
# process B: a second read-write attach fences A
$DUCKDB -c "$setup ATTACH 'duckdb_objfs://reattach.db' AS obj; INSERT INTO obj.t VALUES (1000); DETACH obj;"
sleep 3
echo "DETACH obj;
      ATTACH 'duckdb_objfs://reattach.db' AS again (READ_ONLY);
      SELECT count(*) FROM again.t;" >&5
exec 5>&-; wait
echo "process A:"; cat a.log
echo "process C (fresh):"
$DUCKDB -c "$setup ATTACH 'duckdb_objfs://reattach.db' AS obj (READ_ONLY); SELECT count(*) FROM obj.t;"
