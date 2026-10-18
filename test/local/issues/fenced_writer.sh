#!/usr/bin/env bash
# Issue repro: writer A commits in a loop; writer B attaches the same database read-write and
# commits once. Prints how each process ended (exit 134 = abort). KIND=plain or KIND=ducklake.
DUCKDB=${DUCKDB:-$PWD/build/release/duckdb}
KIND=${KIND:-plain}
cd "$(mktemp -d)"
STORE=$(mktemp -d)
setup="LOAD duckdb_object_storage; SET duckdb_objfs_root = '$STORE';"
for n in $(seq 1 10); do
  if [ "$KIND" = ducklake ]; then
    attach="INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:duckdb_objfs://race_$n.ducklake' AS obj (DATA_PATH '$PWD/data_$n/');"
  else
    attach="ATTACH 'duckdb_objfs://race_$n.db' AS obj;"
  fi
  { echo "$setup $attach CREATE TABLE obj.t (i BIGINT);"
    echo "COPY (SELECT 1) TO 'ready_$n.csv';"
    for i in $(seq 0 200000); do echo "INSERT INTO obj.t VALUES ($i);"; done; } > a_$n.sql
  $DUCKDB -bail -f a_$n.sql > a_$n.log 2>&1 &
  until [ -f ready_$n.csv ]; do sleep 0.1; done; sleep 1
  $DUCKDB -bail -c "$setup $attach INSERT INTO obj.t VALUES (-1); DETACH obj;" > b_$n.log 2>&1
  b=$?
  wait %1; a=$?
  echo "trial $n: A exit $a: $(grep -a -m1 -E 'what|Error' a_$n.log | cut -c1-170)"
  echo "         B exit $b: $(grep -a -m1 -E 'what|Error' b_$n.log | cut -c1-170)"
done
