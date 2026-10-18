#!/usr/bin/env bash
# End-to-end test of the azure backend against live Microsoft OneLake, plus a DuckLake whose catalog
# lives on duckdb_objfs and whose data lives in the lakehouse Tables/ section.
#
# Requires ONELAKE_TOKEN (bearer token for https://storage.azure.com). The database lives under
# ${ONELAKE_WORKSPACE}/${ONELAKE_ROOT} (OneLake: container = workspace, root = <item>.Lakehouse/Files/...).
# Every phase records named checks; results go to stdout and to the GitHub job summary.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DUCKDB_BIN="${DUCKDB_BIN:-${ROOT_DIR}/build/release/duckdb}"
WORKSPACE="${ONELAKE_WORKSPACE:?set ONELAKE_WORKSPACE}"
ROOT="${ONELAKE_ROOT:?set ONELAKE_ROOT}"
: "${ONELAKE_TOKEN:?set ONELAKE_TOKEN}"
ROWS="${DUCKDB_OBJFS_E2E_ROWS:-1000000}"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/null}"
# The azure extension's curl transport needs to be told where the CA bundle is.
export CURL_CA_INFO="${CURL_CA_INFO:-/etc/ssl/certs/ca-certificates.crt}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT
FAILED=()

if [[ ! -x "${DUCKDB_BIN}" ]]; then
	echo "DuckDB executable not found at ${DUCKDB_BIN}" >&2
	exit 1
fi

# Every phase is a separate process, so each one initializes the filesystem from scratch.
preamble() {
	cat <<EOF
.timer on
INSTALL azure;
LOAD azure;
LOAD duckdb_object_storage;
SET GLOBAL azure_transport_option_type = 'curl';
CREATE OR REPLACE SECRET onelake (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN getenv('ONELAKE_TOKEN'));
SET duckdb_objfs_backend = 'azure';
SET duckdb_objfs_bucket = '${WORKSPACE}';
SET duckdb_objfs_root = '${ROOT}';
CREATE TEMP TABLE checks (check_name VARCHAR, expected VARCHAR, actual VARCHAR);
EOF
}

# Prints the checks as markdown into $1, then fails the phase if any check did not match.
report() {
	cat <<EOF
SELECT * FROM duckdb_objfs_io_stats();
.mode markdown
.output $1
SELECT check_name AS "check", expected, actual,
       CASE WHEN expected IS NOT DISTINCT FROM actual THEN '✅' ELSE '❌' END AS result
FROM checks ORDER BY rowid;
.output
.mode duckbox
SELECT check_name, expected, actual, expected IS NOT DISTINCT FROM actual AS ok FROM checks ORDER BY rowid;
SELECT CASE WHEN count(*) > 0 THEN error('failed checks: ' || string_agg(check_name, ', ')) ELSE 'all checks passed' END
FROM checks WHERE expected IS DISTINCT FROM actual;
EOF
}

run_phase() {
	local name="$1"
	local sql="${WORK_DIR}/${name}.sql" log="${WORK_DIR}/${name}.log" checks="${WORK_DIR}/${name}.md"
	{
		preamble
		cat
		report "${checks}"
	} >"${sql}"
	echo "=== phase: ${name}"
	local start=${SECONDS} status=0
	"${DUCKDB_BIN}" -bail -f "${sql}" 2>&1 | tee "${log}" || status=$?
	local took=$((SECONDS - start)) mark="✅"
	[[ ${status} -eq 0 ]] || mark="❌"
	echo "=== phase ${name}: ${mark} ${took}s"
	{
		echo "### ${mark} ${name} (${took}s)"
		[[ -s "${checks}" ]] && cat "${checks}"
		if [[ ${status} -ne 0 ]]; then
			echo '```'
			grep -a -i "error" "${log}" | tail -n 5
			echo '```'
		fi
		echo
	} >>"${SUMMARY}"
	[[ ${status} -eq 0 ]] || FAILED+=("${name}")
}

{
	echo "## duckdb_objfs on OneLake: \`${WORKSPACE}/${ROOT}\`"
	echo
} >>"${SUMMARY}"

# ---------------------------------------------------------------- DuckDB database on OneLake

run_phase write <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
CREATE OR REPLACE TABLE obj.items AS SELECT range AS i, md5(range::VARCHAR) AS payload FROM range(${ROWS});
CHECKPOINT obj;
INSERT INTO checks SELECT 'rows written', '${ROWS}', count(*)::VARCHAR FROM obj.items;
DETACH obj;
EOF

run_phase read_only <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj (READ_ONLY);
INSERT INTO checks SELECT 'rows after reopen', '${ROWS}', count(*)::VARCHAR FROM obj.items;
INSERT INTO checks SELECT 'sum after reopen', (${ROWS}::HUGEINT * (${ROWS} - 1) / 2)::VARCHAR, sum(i)::VARCHAR FROM obj.items;
INSERT INTO checks SELECT 'point lookup', md5('4242'), payload FROM obj.items WHERE i = 4242;
DETACH obj;
EOF

# Crash consistency: commit rows to the WAL (no checkpoint), then kill -9 the process mid-query.
CRASH_SQL="${WORK_DIR}/crash.sql"
MARKER="${WORK_DIR}/committed.csv"
{
	preamble
	cat <<EOF
SET checkpoint_threshold = '10GB';
ATTACH 'duckdb_objfs://e2e.db' AS obj;
INSERT INTO obj.items SELECT range AS i, 'crash' FROM range(${ROWS}, ${ROWS} + 1000);
COPY (SELECT 1) TO '${MARKER}';
SELECT count(*) FROM range(1000000000000);
EOF
} >"${CRASH_SQL}"
echo "=== phase: crash"
"${DUCKDB_BIN}" -f "${CRASH_SQL}" &
CRASH_PID=$!
for _ in $(seq 1 600); do
	[[ -f "${MARKER}" ]] && break
	kill -0 "${CRASH_PID}" 2>/dev/null || break
	sleep 0.5
done
if [[ -f "${MARKER}" ]]; then
	kill -9 "${CRASH_PID}"
	wait "${CRASH_PID}" 2>/dev/null || true
	echo "### ✅ crash: committed 1000 rows without a checkpoint, then kill -9" >>"${SUMMARY}"
else
	echo "### ❌ crash: duckdb exited before committing" >>"${SUMMARY}"
	FAILED+=("crash")
fi

run_phase recover <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
INSERT INTO checks SELECT 'rows after kill -9 (WAL replay)', '$((ROWS + 1000))', count(*)::VARCHAR FROM obj.items;
CHECKPOINT obj;
DETACH obj;
ATTACH 'duckdb_objfs://e2e.db' AS obj (READ_ONLY);
INSERT INTO checks SELECT 'rows after checkpoint + reopen', '$((ROWS + 1000))', count(*)::VARCHAR FROM obj.items;
DETACH obj;
EOF

# ---------------------------------------------------------------- DuckLake: catalog on duckdb_objfs
# Data in the lakehouse Tables/ section (schema-enabled lakehouse: DuckLake's <data_path>/<schema>/<table>/
# maps onto Tables/<schema>/<table>/). test_sales is the Delta-exported table; test_customers gets the DDL.

LAKEHOUSE="${ONELAKE_LAKEHOUSE:?set ONELAKE_LAKEHOUSE, e.g. lh.Lakehouse}"
S="${DUCKLAKE_SCHEMA:-ducklake_test}"
LAKE_DATA="abfss://${WORKSPACE}@onelake.dfs.fabric.microsoft.com/${LAKEHOUSE}/Tables/"
SALES="lake.${S}.test_sales"
ATTACH_RW="ATTACH 'ducklake:duckdb_objfs://catalog.ducklake' AS lake (DATA_PATH '${LAKE_DATA}', METADATA_CATALOG '__ducklake_metadata_lake', DATA_INLINING_ROW_LIMIT 0);"
ATTACH_RO="ATTACH 'ducklake:duckdb_objfs://catalog.ducklake' AS lake (READ_ONLY);"
v() { cat "${WORK_DIR}/$1.csv"; }
save() { echo "COPY ($2) TO '${WORK_DIR}/$1.csv' (HEADER false);"; }

# Expected test_sales rows: 150000 inserted, minus the 1547 ids below 150000 divisible by 97,
# plus 10000 more, plus 500 inserted by MERGE.
SALES_ROWS=158953

run_phase ducklake_write <<EOF
INSTALL ducklake;
LOAD ducklake;
${ATTACH_RW}
-- One catalog reused by every run: start the schema over.
DROP SCHEMA IF EXISTS lake.${S} CASCADE;
CREATE SCHEMA lake.${S};
CREATE TABLE ${SALES} (
    sale_id BIGINT, sale_date DATE, sale_ts TIMESTAMP, region VARCHAR, product VARCHAR,
    quantity INTEGER, unit_price DECIMAL(18, 4), discount DOUBLE, is_online BOOLEAN
);
ALTER TABLE ${SALES} SET PARTITIONED BY (region);
CREATE MACRO sales_batch(lo, hi) AS TABLE
    SELECT range AS sale_id,
           DATE '2024-01-01' + (range % 730)::INTEGER AS sale_date,
           TIMESTAMP '2024-01-01' + to_seconds(range * 37) AS sale_ts,
           ['north', 'south', 'east', 'west'][1 + range % 4] AS region,
           'product_' || (range % 250) AS product,
           (1 + range % 20)::INTEGER AS quantity,
           (5 + (range % 1000) * 0.37)::DECIMAL(18, 4) AS unit_price,
           (range % 15) / 100.0 AS discount,
           range % 3 = 0 AS is_online
    FROM range(lo, hi);

INSERT INTO ${SALES} SELECT * FROM sales_batch(0, 100000);
$(save sales_first_snapshot "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")
$(save sales_first_time "SELECT max(snapshot_time) FROM ducklake_snapshots('lake')")
INSERT INTO ${SALES} SELECT * FROM sales_batch(100000, 150000);
UPDATE ${SALES} SET discount = 0.5 WHERE region = 'north' AND quantity = 20;
DELETE FROM ${SALES} WHERE sale_id % 97 = 0;
$(save sales_delete_snapshot "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")

-- A rolled-back transaction leaves no trace.
BEGIN;
DELETE FROM ${SALES};
ROLLBACK;
INSERT INTO checks SELECT 'rollback leaves rows', '148453', count(*)::VARCHAR FROM ${SALES};

ALTER TABLE ${SALES} ADD COLUMN channel VARCHAR;
INSERT INTO ${SALES}
SELECT *, CASE WHEN is_online THEN 'web' ELSE 'store' END FROM sales_batch(150000, 160000);

-- Upsert: update ids 1000..1499 that still exist, insert 160000..160499.
MERGE INTO ${SALES} AS target
USING (SELECT * FROM sales_batch(1000, 1500) UNION ALL SELECT * FROM sales_batch(160000, 160500)) AS source
ON target.sale_id = source.sale_id
WHEN MATCHED THEN UPDATE SET quantity = 999
WHEN NOT MATCHED THEN INSERT VALUES (source.sale_id, source.sale_date, source.sale_ts, source.region,
    source.product, source.quantity, source.unit_price, source.discount, source.is_online, 'merge');

CREATE VIEW lake.${S}.test_sales_by_region AS
SELECT region, count(*) AS sales, sum(quantity * unit_price) AS revenue FROM ${SALES} GROUP BY region;

-- DDL table: rename column, type promotion, drop column, partition evolution, rename table.
CREATE TABLE lake.${S}.test_customers (
    customer_id INTEGER, customer_name VARCHAR, country VARCHAR, segment VARCHAR,
    signup_date DATE, lifetime_value DOUBLE
);
ALTER TABLE lake.${S}.test_customers SET PARTITIONED BY (country);
INSERT INTO lake.${S}.test_customers
SELECT range::INTEGER, 'customer_' || range, ['AU', 'NZ', 'FR', 'DZ', 'US'][1 + range % 5],
       ['retail', 'smb', 'enterprise'][1 + range % 3], DATE '2020-01-01' + (range % 2000)::INTEGER,
       (range % 5000) * 1.5
FROM range(20000);
$(save customers_before_ddl "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")
ALTER TABLE lake.${S}.test_customers RENAME COLUMN customer_name TO full_name;
ALTER TABLE lake.${S}.test_customers ALTER COLUMN customer_id SET DATA TYPE BIGINT;
ALTER TABLE lake.${S}.test_customers DROP COLUMN segment;
ALTER TABLE lake.${S}.test_customers SET PARTITIONED BY (country, year(signup_date));
INSERT INTO lake.${S}.test_customers (customer_id, full_name, country, signup_date, lifetime_value)
SELECT range, 'customer_' || range, ['AU', 'NZ', 'FR', 'DZ', 'US'][1 + range % 5],
       DATE '2020-01-01' + (range % 2000)::INTEGER, (range % 5000) * 1.5
FROM range(20000, 25000);
ALTER TABLE lake.${S}.test_customers RENAME TO test_customers_v2;

SELECT snapshot_id, snapshot_time, changes FROM ducklake_snapshots('lake') ORDER BY snapshot_id;
INSERT INTO checks SELECT 'snapshots written', 'true', (count(*) >= 15)::VARCHAR FROM ducklake_snapshots('lake');
DETACH lake;
EOF

run_phase ducklake_read <<EOF
LOAD ducklake;
${ATTACH_RO}
INSERT INTO checks SELECT 'test_sales rows', '${SALES_ROWS}', count(*)::VARCHAR FROM ${SALES};
INSERT INTO checks SELECT 'time travel by version', '100000', count(*)::VARCHAR
FROM ${SALES} AT (VERSION => $(v sales_first_snapshot));
INSERT INTO checks SELECT 'time travel by timestamp', '100000', count(*)::VARCHAR
FROM ${SALES} AT (TIMESTAMP => TIMESTAMPTZ '$(v sales_first_time)');
INSERT INTO checks SELECT 'change feed: first insert', '100000', count(*)::VARCHAR
FROM ducklake_table_changes('lake', '${S}', 'test_sales', $(v sales_first_snapshot), $(v sales_first_snapshot))
WHERE change_type = 'insert';
INSERT INTO checks SELECT 'change feed: delete', '1547', count(*)::VARCHAR
FROM ducklake_table_changes('lake', '${S}', 'test_sales', $(v sales_delete_snapshot), $(v sales_delete_snapshot))
WHERE change_type = 'delete';
INSERT INTO checks SELECT 'update applied', 'true', (count(*) > 0)::VARCHAR FROM ${SALES} WHERE discount = 0.5;
INSERT INTO checks SELECT 'merge inserted', '500', count(*)::VARCHAR FROM ${SALES} WHERE channel = 'merge';
INSERT INTO checks SELECT 'merge updated', '495', count(*)::VARCHAR
FROM ${SALES} WHERE sale_id BETWEEN 1000 AND 1499 AND quantity = 999;
INSERT INTO checks SELECT 'added column filled', '10500', count(channel)::VARCHAR FROM ${SALES};
INSERT INTO checks SELECT 'partitions (region)', 'east,north,south,west',
    string_agg(DISTINCT regexp_extract(data_file, 'region=([^/]+)', 1), ',' ORDER BY regexp_extract(data_file, 'region=([^/]+)', 1))
FROM ducklake_list_files('lake', 'test_sales', schema => '${S}');
INSERT INTO checks SELECT 'view', '4', count(*)::VARCHAR FROM lake.${S}.test_sales_by_region;

INSERT INTO checks SELECT 'renamed table rows', '25000', count(*)::VARCHAR FROM lake.${S}.test_customers_v2;
INSERT INTO checks SELECT 'old table name gone', '0', count(*)::VARCHAR
FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = '${S}' AND table_name = 'test_customers';
INSERT INTO checks SELECT 'renamed column', 'full_name', string_agg(column_name, ',')
FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = 'test_customers_v2' AND column_name LIKE '%name';
INSERT INTO checks SELECT 'dropped column', '0', count(*)::VARCHAR
FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = 'test_customers_v2' AND column_name = 'segment';
INSERT INTO checks SELECT 'type promotion', 'BIGINT', data_type
FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = 'test_customers_v2' AND column_name = 'customer_id';
INSERT INTO checks SELECT 'partition evolution (levels per file)', '1,2',
    string_agg(DISTINCT len(regexp_extract_all(data_file, '/[^/=]+=[^/]+'))::VARCHAR, ',' ORDER BY len(regexp_extract_all(data_file, '/[^/=]+=[^/]+'))::VARCHAR)
FROM ducklake_list_files('lake', 'test_customers_v2', schema => '${S}');
INSERT INTO checks SELECT 'time travel before rename (old name)', '20000', count(*)::VARCHAR
FROM lake.${S}.test_customers AT (VERSION => $(v customers_before_ddl));
INSERT INTO checks SELECT 'time travel sees dropped column', '3', count(DISTINCT segment)::VARCHAR
FROM lake.${S}.test_customers AT (VERSION => $(v customers_before_ddl));
DETACH lake;
EOF

run_phase ducklake_maintenance <<EOF
LOAD ducklake;
${ATTACH_RW}
CREATE TEMP TABLE files_before AS
SELECT count(*) AS n FROM ducklake_list_files('lake', 'test_sales', schema => '${S}');
CALL ducklake_merge_adjacent_files('lake');
INSERT INTO checks SELECT 'compaction (data files)', 'fewer',
    CASE WHEN count(*) < (SELECT n FROM files_before) THEN 'fewer'
         ELSE (SELECT n FROM files_before)::VARCHAR || ' -> ' || count(*)::VARCHAR END
FROM ducklake_list_files('lake', 'test_sales', schema => '${S}');
CALL ducklake_expire_snapshots('lake', older_than => now());
CALL ducklake_cleanup_old_files('lake', cleanup_all => true);
INSERT INTO checks SELECT 'snapshots after expire', '1', count(*)::VARCHAR FROM ducklake_snapshots('lake');
INSERT INTO checks SELECT 'rows after maintenance', '${SALES_ROWS}', count(*)::VARCHAR FROM ${SALES};
DETACH lake;
EOF

SALES_COLS="sale_id, sale_date, sale_ts::TIMESTAMP AS sale_ts, region, product, quantity, unit_price, discount, is_online, channel"
DELTA_SALES="delta_scan('${LAKE_DATA}${S}/test_sales')"
run_phase delta_export <<EOF
LOAD ducklake;
INSTALL delta_export FROM community;
LOAD delta_export;
INSTALL delta;
LOAD delta;
${ATTACH_RW}
-- Writes each table's _delta_log next to its Parquet, so Fabric sees Tables/<schema>/<table> as Delta.
CALL delta_export();
DESCRIBE SELECT * FROM ${DELTA_SALES};
INSERT INTO checks SELECT 'delta test_sales rows', '${SALES_ROWS}', count(*)::VARCHAR FROM ${DELTA_SALES};
INSERT INTO checks SELECT 'delta = ducklake, row for row', '0', (
    (SELECT count(*) FROM (SELECT ${SALES_COLS} FROM ${SALES} EXCEPT ALL SELECT ${SALES_COLS} FROM ${DELTA_SALES}))
  + (SELECT count(*) FROM (SELECT ${SALES_COLS} FROM ${DELTA_SALES} EXCEPT ALL SELECT ${SALES_COLS} FROM ${SALES}))
)::VARCHAR;
INSERT INTO checks SELECT 'delta test_customers rows', '25000', count(*)::VARCHAR
FROM delta_scan('${LAKE_DATA}${S}/test_customers');
DETACH lake;
EOF

echo "=== OneLake objects under ${WORKSPACE}/${ROOT}"
curl -sf -H "Authorization: Bearer ${ONELAKE_TOKEN}" -H "x-ms-version: 2023-11-03" \
	"https://onelake.blob.fabric.microsoft.com/${WORKSPACE}?restype=container&comp=list&prefix=${ROOT}/" |
	python3 -c '
import sys, xml.etree.ElementTree as ET
blobs = [(b.findtext("Name"), int(b.findtext("Properties/Content-Length") or 0))
         for b in ET.parse(sys.stdin).getroot().iter("Blob")]
print(f"{len(blobs)} objects, {sum(s for _, s in blobs) / 1048576:.1f} MiB (first page)")' |
	tee -a "${SUMMARY}" || true

if [[ ${#FAILED[@]} -gt 0 ]]; then
	echo "=== failed phases: ${FAILED[*]}" | tee -a "${SUMMARY}"
	exit 1
fi
echo "=== OneLake e2e passed"
