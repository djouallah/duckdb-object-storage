#!/usr/bin/env bash
# TPC-H on a DuckDB database stored on live OneLake (port of the TPC-H part of
# djouallah/lakehouse_benchmark). tpchgen-cli generates parquet locally; the tables are loaded into
# duckdb_objfs://tpch_sf<N>.db, then the 22 queries run against OneLake from a fresh process.
#
# DuckDB's tpch_answers() stops at SF1, so each query also runs on the local parquet and the two
# results must be identical (all money columns are DECIMAL, so the aggregates are exact).
# Same requirements as run_e2e.sh, plus tpchgen-cli on PATH. Knobs: TPCH_SF, TPCH_WORK_DIR.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SF="${TPCH_SF:-10}"
WORK_DIR="${TPCH_WORK_DIR:-$(mktemp -d)}"
mkdir -p "${WORK_DIR}"
# shellcheck source=test/azure/common.sh
source "${ROOT_DIR}/test/azure/common.sh"
DATA="${WORK_DIR}/tpch_sf${SF}"
DB="duckdb_objfs://tpch_sf${SF}.db"
TABLES=(region nation supplier customer part partsupp orders lineitem)

{
	echo "## TPC-H SF${SF} on OneLake: \`${WORKSPACE}/${ROOT}/tpch_sf${SF}.db\`"
	echo
} >>"${SUMMARY}"

# ---------------------------------------------------------------- generate (local)

echo "=== generate SF${SF} with tpchgen-cli"
start=${SECONDS}
rm -rf "${DATA}"
tpchgen-cli --scale-factor "${SF}" --format parquet --output-dir "${DATA}"
echo "tpchgen-cli SF${SF}: $((SECONDS - start))s, $(du -sh "${DATA}" | cut -f1) of parquet" | tee -a "${SUMMARY}"
echo >>"${SUMMARY}"

# parquet_of <table>: the read_parquet path for one table (one file, or a directory of parts).
parquet_of() {
	if [[ -f "${DATA}/$1.parquet" ]]; then
		echo "${DATA}/$1.parquet"
	else
		echo "${DATA}/$1/*.parquet"
	fi
}

# The 22 queries as DuckDB ships them, without the trailing semicolon so they can be wrapped.
QUERY_DIR="${WORK_DIR}/queries"
mkdir -p "${QUERY_DIR}"
for n in $(seq 1 22); do
	file="${QUERY_DIR}/q$(printf %02d "${n}").sql"
	"${DUCKDB_BIN}" -list -noheader -c "INSTALL tpch; LOAD tpch;
		SELECT regexp_replace(query, ';\s*$', '') FROM tpch_queries() WHERE query_nr = ${n};" >"${file}"
	grep -qi select "${file}" || { echo "could not extract TPC-H query ${n}" >&2; exit 1; }
done

# ---------------------------------------------------------------- load into OneLake

# Row counts fixed by the TPC-H spec; region and nation do not scale, lineitem is not linear.
expected_rows() {
	case "$1" in
	region) echo 5 ;;
	nation) echo 25 ;;
	supplier) echo $((SF * 10000)) ;;
	customer) echo $((SF * 150000)) ;;
	part) echo $((SF * 200000)) ;;
	partsupp) echo $((SF * 800000)) ;;
	orders) echo $((SF * 1500000)) ;;
	lineitem)
		case "${SF}" in
		1) echo 6001215 ;;
		10) echo 59986052 ;;
		100) echo 600037902 ;;
		esac
		;;
	esac
}

# run_phase reads from a process substitution, not a pipe, so FAILED is kept in this shell.
run_phase tpch_load < <(
	echo "ATTACH '${DB}' AS obj;"
	for t in "${TABLES[@]}"; do
		echo "CREATE OR REPLACE TABLE obj.${t} AS FROM read_parquet('$(parquet_of "${t}")');"
	done
	echo "CHECKPOINT obj;"
	echo "DETACH obj;"
	echo "ATTACH '${DB}' AS obj (READ_ONLY);"
	for t in "${TABLES[@]}"; do
		echo "INSERT INTO checks SELECT '${t} rows', '$(expected_rows "${t}")', count(*)::VARCHAR FROM obj.${t};"
	done
	echo "DETACH obj;"
)

# ---------------------------------------------------------------- 22 queries: OneLake cold, hot, local

# run_queries <db.schema> <run> <table prefix>: all 22 queries, each into a temp table, timed.
run_queries() {
	echo "USE $1;"
	local f q
	for f in "${QUERY_DIR}"/q*.sql; do
		q=$(basename "${f}" .sql)
		echo "SET VARIABLE t0 = get_current_timestamp();"
		echo "CREATE OR REPLACE TEMP TABLE $3_${q} AS"
		cat "${f}"
		echo ";"
		echo "INSERT INTO timings VALUES ('${q^^}', '$2', epoch(get_current_timestamp() - getvariable('t0')));"
	done
}

TIMINGS="${WORK_DIR}/tpch_timings.md"
run_phase tpch_queries < <(
	echo "ATTACH '${DB}' AS obj (READ_ONLY);"
	echo "CREATE SCHEMA memory.ref;"
	for t in "${TABLES[@]}"; do
		echo "CREATE VIEW memory.ref.${t} AS FROM read_parquet('$(parquet_of "${t}")');"
	done
	echo "CREATE TEMP TABLE timings (query VARCHAR, run VARCHAR, seconds DOUBLE);"
	run_queries obj.main onelake_cold o
	run_queries obj.main onelake_hot h
	run_queries memory.ref local_parquet r
	echo "USE memory;"
	# The cold results (o_) are the ones checked: every read behind them came from OneLake.
	for f in "${QUERY_DIR}"/q*.sql; do
		q=$(basename "${f}" .sql)
		cat <<SQL
INSERT INTO checks SELECT '${q^^} matches local',
    (SELECT count(*) FROM r_${q})::VARCHAR || ' rows, 0 diff',
    (SELECT count(*) FROM o_${q})::VARCHAR || ' rows, ' || (SELECT count(*) FROM (
        (FROM o_${q} EXCEPT ALL FROM r_${q}) UNION ALL (FROM r_${q} EXCEPT ALL FROM o_${q})))::VARCHAR || ' diff';
SQL
	done
	cat <<SQL
.mode markdown
.output ${TIMINGS}
PIVOT (SELECT query, run, round(seconds, 2) AS seconds
       FROM (FROM timings UNION ALL SELECT 'total', run, sum(seconds) FROM timings GROUP BY run))
ON run IN ('onelake_cold', 'onelake_hot', 'local_parquet') USING first(seconds) GROUP BY query ORDER BY query;
.output
.mode duckbox
DETACH obj;
SQL
)

if [[ -s "${TIMINGS}" ]]; then
	{
		echo "#### Query seconds (cold = first run on OneLake, hot = second run in the same process)"
		cat "${TIMINGS}"
		echo
	} >>"${SUMMARY}"
fi

if [[ ${#FAILED[@]} -gt 0 ]]; then
	echo "=== failed phases: ${FAILED[*]}" | tee -a "${SUMMARY}"
	exit 1
fi
echo "=== OneLake TPC-H SF${SF} passed"
