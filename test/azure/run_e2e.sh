#!/usr/bin/env bash
# End-to-end test of the azure backend against live Microsoft OneLake.
#
# Requires ONELAKE_TOKEN (bearer token for https://storage.azure.com). The database lives under
# ${ONELAKE_WORKSPACE}/${ONELAKE_ROOT} (OneLake: container = workspace, root = <item>.Lakehouse/Files/...).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DUCKDB_BIN="${DUCKDB_BIN:-${ROOT_DIR}/build/release/duckdb}"
WORKSPACE="${ONELAKE_WORKSPACE:?set ONELAKE_WORKSPACE}"
ROOT="${ONELAKE_ROOT:?set ONELAKE_ROOT}"
: "${ONELAKE_TOKEN:?set ONELAKE_TOKEN}"
ROWS="${DUCKDB_OBJFS_E2E_ROWS:-1000000}"
# The azure extension's curl transport needs to be told where the CA bundle is.
export CURL_CA_INFO="${CURL_CA_INFO:-/etc/ssl/certs/ca-certificates.crt}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

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
CREATE OR REPLACE SECRET onelake (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN getenv('ONELAKE_TOKEN'));
SET duckdb_objfs_backend = 'azure';
SET duckdb_objfs_bucket = '${WORKSPACE}';
SET duckdb_objfs_root = '${ROOT}';
EOF
}

stats() {
	cat <<'EOF'
SELECT * FROM duckdb_objfs_io_stats();
SELECT * FROM duckdb_objfs_cache_stats();
EOF
}

run_phase() {
	local name="$1"
	local sql="${WORK_DIR}/${name}.sql"
	{
		preamble
		cat
		stats
	} >"${sql}"
	echo "=== phase: ${name}"
	local start=${SECONDS}
	"${DUCKDB_BIN}" -bail -f "${sql}"
	echo "=== phase ${name} took $((SECONDS - start))s"
}

run_phase write <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
CREATE TABLE obj.items AS SELECT range AS i, md5(range::VARCHAR) AS payload FROM range(${ROWS});
CHECKPOINT obj;
DETACH obj;
EOF

run_phase read_only <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj (READ_ONLY);
SELECT CASE WHEN count(*) = ${ROWS} AND sum(i) = ${ROWS}::HUGEINT * (${ROWS} - 1) / 2 THEN 'read_only ok'
            ELSE error('unexpected row count/sum after reopen') END
FROM obj.items;
SELECT payload FROM obj.items WHERE i = 4242;
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
	if ! kill -0 "${CRASH_PID}" 2>/dev/null; then
		echo "crash phase exited before committing" >&2
		exit 1
	fi
	sleep 0.5
done
[[ -f "${MARKER}" ]] || { echo "crash phase never committed" >&2; exit 1; }
kill -9 "${CRASH_PID}"
wait "${CRASH_PID}" 2>/dev/null || true
echo "=== killed duckdb after commit"

run_phase recover <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
SELECT CASE WHEN count(*) = ${ROWS} + 1000 THEN 'wal replay ok'
            ELSE error('committed rows lost after kill -9: ' || count(*)) END
FROM obj.items;
CHECKPOINT obj;
DETACH obj;
ATTACH 'duckdb_objfs://e2e.db' AS obj (READ_ONLY);
SELECT CASE WHEN count(*) = ${ROWS} + 1000 THEN 'post-checkpoint ok' ELSE error('rows lost after checkpoint') END
FROM obj.items;
DETACH obj;
EOF

# DuckLake with its DuckDB catalog on duckdb_objfs and its Parquet data in the lakehouse Tables/ section
# (schema-enabled lakehouse: DuckLake's <data_path>/<schema>/<table>/ maps onto Tables/<schema>/<table>/).
LAKEHOUSE="${ONELAKE_LAKEHOUSE:?set ONELAKE_LAKEHOUSE, e.g. lh.Lakehouse}"
LAKE_SCHEMA="${DUCKLAKE_SCHEMA:-e2e}"
LAKE_DATA="abfss://${WORKSPACE}@onelake.dfs.fabric.microsoft.com/${LAKEHOUSE}/Tables/"
TABLE="lake.${LAKE_SCHEMA}.test_sales"
SNAPSHOT_FILE="${WORK_DIR}/snapshot_after_first_insert.csv"
# A realistic table: mixed types, partitioned, three inserts, an update, a delete and an added column.
run_phase ducklake_write <<EOF
INSTALL ducklake;
LOAD ducklake;
INSTALL delta_export FROM community;
LOAD delta_export;
SET GLOBAL azure_transport_option_type = 'curl';
-- delta_export() finds the catalog through METADATA_CATALOG; inlining off so every row is in Parquet.
ATTACH 'ducklake:duckdb_objfs://catalog.ducklake' AS lake
    (DATA_PATH '${LAKE_DATA}', METADATA_CATALOG '__ducklake_metadata_lake', DATA_INLINING_ROW_LIMIT 0);
CREATE SCHEMA lake.${LAKE_SCHEMA};
CREATE TABLE ${TABLE} (
    sale_id BIGINT,
    sale_date DATE,
    sale_ts TIMESTAMP,
    region VARCHAR,
    product VARCHAR,
    quantity INTEGER,
    unit_price DECIMAL(18, 4),
    discount DOUBLE,
    is_online BOOLEAN
);
ALTER TABLE ${TABLE} SET PARTITIONED BY (region);
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
INSERT INTO ${TABLE} SELECT * FROM sales_batch(0, 100000);
COPY (SELECT max(snapshot_id) FROM ducklake_snapshots('lake')) TO '${SNAPSHOT_FILE}' (HEADER false);
INSERT INTO ${TABLE} SELECT * FROM sales_batch(100000, 150000);
UPDATE ${TABLE} SET discount = 0.5 WHERE region = 'north' AND quantity = 20;
DELETE FROM ${TABLE} WHERE sale_id % 97 = 0;
ALTER TABLE ${TABLE} ADD COLUMN channel VARCHAR;
INSERT INTO ${TABLE}
SELECT *, CASE WHEN is_online THEN 'web' ELSE 'store' END AS channel FROM sales_batch(150000, 160000);
SELECT snapshot_id, changes FROM ducklake_snapshots('lake');
-- Writes each table's _delta_log next to its Parquet, so Fabric sees Tables/<schema>/test_sales as Delta.
CALL delta_export();
DETACH lake;
EOF

# 150000 rows, minus the 1547 ids divisible by 97 below 150000, plus 10000.
EXPECTED_ROWS=158453
DELTA_TABLE="delta_scan('${LAKE_DATA}${LAKE_SCHEMA}/test_sales')"
run_phase ducklake_read <<EOF
LOAD ducklake;
INSTALL delta;
LOAD delta;
SET GLOBAL azure_transport_option_type = 'curl';
ATTACH 'ducklake:duckdb_objfs://catalog.ducklake' AS lake (READ_ONLY);
SELECT CASE WHEN count(*) = ${EXPECTED_ROWS} THEN 'ducklake ok' ELSE error('ducklake rows: ' || count(*)) END
FROM ${TABLE};
SELECT CASE WHEN count(*) = 100000 THEN 'time travel ok' ELSE error('rows after first insert: ' || count(*)) END
FROM ${TABLE} AT (VERSION => $(cat "${SNAPSHOT_FILE}"));
SELECT regexp_extract(data_file, 'region=([^/]+)', 1) AS region_partition, count(*) AS data_files
FROM ducklake_list_files('lake', 'test_sales', schema => '${LAKE_SCHEMA}') GROUP BY ALL ORDER BY ALL;
-- Read it back as plain Delta, the way Fabric / any Delta reader will, and compare row for row.
DESCRIBE SELECT * FROM ${DELTA_TABLE};
SELECT CASE
    WHEN (SELECT count(*) FROM ${DELTA_TABLE}) <> ${EXPECTED_ROWS}
        THEN error('delta rows: ' || (SELECT count(*) FROM ${DELTA_TABLE}))
    WHEN (SELECT count(*) FROM (SELECT sale_id, sale_date, sale_ts::TIMESTAMP AS sale_ts, region, product, quantity, unit_price, discount, is_online, channel FROM ${TABLE} EXCEPT ALL SELECT sale_id, sale_date, sale_ts::TIMESTAMP AS sale_ts, region, product, quantity, unit_price, discount, is_online, channel FROM ${DELTA_TABLE})) > 0
        OR (SELECT count(*) FROM (SELECT sale_id, sale_date, sale_ts::TIMESTAMP AS sale_ts, region, product, quantity, unit_price, discount, is_online, channel FROM ${DELTA_TABLE} EXCEPT ALL SELECT sale_id, sale_date, sale_ts::TIMESTAMP AS sale_ts, region, product, quantity, unit_price, discount, is_online, channel FROM ${TABLE})) > 0
        THEN error('delta export differs from ducklake')
    ELSE 'delta matches ducklake row for row' END;
DETACH lake;
EOF

echo "=== OneLake objects under ${WORKSPACE}/${ROOT}"
curl -sf -H "Authorization: Bearer ${ONELAKE_TOKEN}" -H "x-ms-version: 2023-11-03" \
	"https://onelake.blob.fabric.microsoft.com/${WORKSPACE}?restype=container&comp=list&prefix=${ROOT}/" |
	python3 -c '
import sys, xml.etree.ElementTree as ET
blobs = [(b.findtext("Name"), int(b.findtext("Properties/Content-Length") or 0))
         for b in ET.parse(sys.stdin).getroot().iter("Blob")]
for name, size in blobs:
    print(f"{size:>12} {name}")
print(f"{len(blobs)} objects, {sum(s for _, s in blobs) / 1048576:.1f} MiB (first page)")'

echo "=== OneLake e2e passed"
