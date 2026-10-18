#!/usr/bin/env bash
# TPC-H benchmark on DuckLake in live OneLake (port of the TPC-H part of djouallah/lakehouse_benchmark).
# The DuckLake catalog is a duckdb_objfs database; the table data is parquet in the lakehouse
# Tables/ section (schema tpch_sf<N>). tpchgen-cli generates parquet locally, the tables are loaded
# into DuckLake, then the 22 queries run twice against OneLake from a fresh process (cold, then hot).
# Timings go to stdout and the job summary. Same requirements as run_e2e.sh, plus tpchgen-cli on
# PATH. Knobs: TPCH_SF, TPCH_WORK_DIR, TPCH_ROW_GROUP_ROWS.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SF="${TPCH_SF:-10}"
WORK_DIR="${TPCH_WORK_DIR:-$(mktemp -d)}"
mkdir -p "${WORK_DIR}"
# shellcheck source=test/azure/common.sh
source "${ROOT_DIR}/test/azure/common.sh"
DATA="${WORK_DIR}/tpch_sf${SF}"
LAKEHOUSE="${ONELAKE_LAKEHOUSE:-${ROOT%%/*}}"
DATA_PATH="abfss://${WORKSPACE}@onelake.dfs.fabric.microsoft.com/${LAKEHOUSE}/Tables/"
CATALOG="ducklake:duckdb_objfs://tpch.ducklake"
SCHEMA="tpch_sf${SF}"
LAYOUT="${WORK_DIR}/tpch_layout.md"
TIMINGS="${WORK_DIR}/tpch_timings.md"
TABLES=(region nation supplier customer part partsupp orders lineitem)

{
	echo "## TPC-H SF${SF} on DuckLake: catalog \`${WORKSPACE}/${ROOT}/tpch.ducklake\`, data \`${LAKEHOUSE}/Tables/${SCHEMA}\`"
	echo
} >>"${SUMMARY}"

# bench_phase <name> <<SQL: one duckdb process (azure + ducklake + duckdb_objfs loaded), timed.
bench_phase() {
	local name="$1" sql="${WORK_DIR}/$1.sql" log="${WORK_DIR}/$1.log" start=${SECONDS} status=0
	{
		preamble ducklake
		cat
	} >"${sql}"
	echo "=== ${name}"
	"${DUCKDB_BIN}" -bail -f "${sql}" 2>&1 | tee "${log}" || status=$?
	if [[ ${status} -eq 0 ]]; then
		echo "${name}: $((SECONDS - start))s" | tee -a "${SUMMARY}"
	else
		{
			echo "${name}: ❌ failed after $((SECONDS - start))s"
			echo '```'
			grep -a -i "error" "${log}" | tail -n 5
			echo '```'
		} | tee -a "${SUMMARY}"
		FAILED+=("${name}")
	fi
	echo >>"${SUMMARY}"
}

# ---------------------------------------------------------------- generate (local)

echo "=== generate SF${SF} with tpchgen-cli"
start=${SECONDS}
rm -rf "${DATA}"
tpchgen-cli --scale-factor "${SF}" --format parquet --output-dir "${DATA}"
echo "generate (tpchgen-cli): $((SECONDS - start))s, $(du -sh "${DATA}" | cut -f1) of parquet" | tee -a "${SUMMARY}"
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
# Q11's FRACTION is 0.0001 / SF in the spec; DuckDB ships the SF1 value, which returns 0 rows at SF10.
sed -i "s|\* 0\.0001000000|* (0.0001000000 / ${SF})|" "${QUERY_DIR}/q11.sql"

# ---------------------------------------------------------------- load into DuckLake on OneLake

# Each run replaces the tables, then expires the old snapshots and deletes the previous run's files.
bench_phase load < <(
	echo "ATTACH '${CATALOG}' AS lake (DATA_PATH '${DATA_PATH}', DATA_INLINING_ROW_LIMIT 0);"
	echo "CREATE SCHEMA IF NOT EXISTS lake.${SCHEMA};"
	# lakehouse_benchmark's Iceberg files use 1M-row row groups. DuckDB's writer default is
	# 122,880 rows, 8x the row groups and so 8x the small range reads from OneLake.
	echo "CALL lake.set_option('parquet_row_group_size', ${TPCH_ROW_GROUP_ROWS:-1048576});"
	echo "CALL lake.set_option('parquet_compression', 'snappy');"
	for t in "${TABLES[@]}"; do
		echo "CREATE OR REPLACE TABLE lake.${SCHEMA}.${t} AS FROM read_parquet('$(parquet_of "${t}")');"
	done
	echo "CALL ducklake_expire_snapshots('lake', older_than => now());"
	echo "CALL ducklake_cleanup_old_files('lake', cleanup_all => true);"
	echo ".mode markdown"
	echo ".output ${LAYOUT}"
	sep=""
	for t in "${TABLES[@]}"; do
		echo "${sep}SELECT '${t}' AS \"table\", count(*) AS files, sum(num_rows) AS \"rows\", sum(num_row_groups) AS row_groups"
		echo "FROM parquet_file_metadata('${DATA_PATH}${SCHEMA}/${t}/*.parquet')"
		sep="UNION ALL "
	done
	echo ";"
	echo ".output"
	echo "DETACH lake;"
)
if [[ -s "${LAYOUT}" ]]; then
	{
		echo "#### Parquet layout written by DuckLake"
		cat "${LAYOUT}"
		echo
	} >>"${SUMMARY}"
fi

# ---------------------------------------------------------------- 22 queries: cold, then hot

# run_queries <run>: all 22 queries, each into a temp table (no output), timed.
run_queries() {
	local f q
	for f in "${QUERY_DIR}"/q*.sql; do
		q=$(basename "${f}" .sql)
		echo "SET VARIABLE t0 = epoch_us(get_current_timestamp());"
		echo "CREATE OR REPLACE TEMP TABLE result_${q} AS"
		cat "${f}"
		echo ";"
		echo "INSERT INTO memory.timings VALUES ('${q^^}', '$1', (epoch_us(get_current_timestamp()) - getvariable('t0')) / 1e6);"
	done
}

bench_phase queries < <(
	echo "ATTACH '${CATALOG}' AS lake (READ_ONLY);"
	echo "CREATE TABLE memory.timings (query VARCHAR, run VARCHAR, seconds DOUBLE);"
	echo "USE lake.${SCHEMA};"
	run_queries cold
	run_queries hot
	cat <<SQL
.mode markdown
.output ${TIMINGS}
PIVOT (SELECT query, run, round(seconds, 2) AS seconds
       FROM (FROM memory.timings UNION ALL SELECT 'total', run, sum(seconds) FROM memory.timings GROUP BY run))
ON run IN ('cold', 'hot') USING first(seconds) GROUP BY query ORDER BY query;
.output
USE memory;
DETACH lake;
SQL
)
if [[ -s "${TIMINGS}" ]]; then
	{
		echo "#### Query seconds (cold = first run from OneLake, hot = second run in the same process)"
		cat "${TIMINGS}"
		echo
	} >>"${SUMMARY}"
fi

if [[ ${#FAILED[@]} -gt 0 ]]; then
	echo "=== failed: ${FAILED[*]}" | tee -a "${SUMMARY}"
	exit 1
fi
