#!/usr/bin/env bash
# End-to-end test of the azure backend against live Microsoft OneLake (DuckLake: see test.sql).
#
# Requires ONELAKE_TOKEN (bearer token for https://storage.azure.com). The database lives under
# ${ONELAKE_WORKSPACE}/${ONELAKE_ROOT} (OneLake: container = workspace, root = <item>.Lakehouse/Files/...).
# Every phase records named checks; results go to stdout and to the GitHub job summary.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROWS="${DUCKDB_OBJFS_E2E_ROWS:-1000000}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT
# shellcheck source=test/azure/common.sh
source "${ROOT_DIR}/test/azure/common.sh"

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
INSERT INTO checks SELECT 'sum after reopen', (${ROWS}::HUGEINT * (${ROWS} - 1) // 2)::VARCHAR, sum(i)::VARCHAR FROM obj.items;
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

# Multi-table transaction: one committed across orders + order_lines, then one left open across both
# tables when duckdb is killed. After recovery: committed rows in both tables, uncommitted in neither.
run_phase multi_table_commit <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
CREATE OR REPLACE TABLE obj.orders (order_id BIGINT, status VARCHAR);
CREATE OR REPLACE TABLE obj.order_lines (order_id BIGINT, line_no INTEGER, status VARCHAR);
BEGIN;
INSERT INTO obj.orders SELECT range, 'committed' FROM range(100);
INSERT INTO obj.order_lines SELECT o.range, l.range, 'committed' FROM range(100) o, range(3) l;
COMMIT;
INSERT INTO checks SELECT 'orders committed', '100', count(*)::VARCHAR FROM obj.orders;
DETACH obj;
EOF

TXN_SQL="${WORK_DIR}/open_txn.sql"
TXN_MARKER="${WORK_DIR}/txn_open.csv"
{
	preamble
	cat <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
BEGIN;
INSERT INTO obj.orders SELECT range, 'uncommitted' FROM range(100, 200);
INSERT INTO obj.order_lines SELECT o.range, l.range, 'uncommitted' FROM range(100, 200) o, range(3) l;
COPY (SELECT 1) TO '${TXN_MARKER}';
SELECT count(*) FROM range(1000000000000);
EOF
} >"${TXN_SQL}"
"${DUCKDB_BIN}" -f "${TXN_SQL}" &
TXN_PID=$!
for _ in $(seq 1 600); do
	[[ -f "${TXN_MARKER}" ]] && break
	kill -0 "${TXN_PID}" 2>/dev/null || break
	sleep 0.5
done
if [[ -f "${TXN_MARKER}" ]]; then
	kill -9 "${TXN_PID}"
	wait "${TXN_PID}" 2>/dev/null || true
	echo "### ✅ open transaction across orders + order_lines, then kill -9 before COMMIT" >>"${SUMMARY}"
else
	echo "### ❌ open-transaction writer exited early" >>"${SUMMARY}"
	FAILED+=("open_txn")
fi

run_phase multi_table_recover <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
INSERT INTO checks SELECT 'committed orders kept', '100', count(*)::VARCHAR FROM obj.orders WHERE status = 'committed';
INSERT INTO checks SELECT 'committed order_lines kept', '300', count(*)::VARCHAR FROM obj.order_lines WHERE status = 'committed';
INSERT INTO checks SELECT 'uncommitted orders absent', '0', count(*)::VARCHAR FROM obj.orders WHERE status = 'uncommitted';
INSERT INTO checks SELECT 'uncommitted order_lines absent', '0', count(*)::VARCHAR FROM obj.order_lines WHERE status = 'uncommitted';
DETACH obj;
EOF

# Accidental second writer: A attaches and commits; B attaches read-write while A is still open and
# commits (fencing A); A then tries to commit again. The database must stay consistent: A's first
# commit and B's rows survive, A's late write does not, and the database still opens.
A_IN="${WORK_DIR}/writer_a.in"
A_LOG="${WORK_DIR}/writer_a.log"
mkfifo "${A_IN}"
"${DUCKDB_BIN}" <"${A_IN}" >"${A_LOG}" 2>&1 &
A_PID=$!
exec 3>"${A_IN}"
preamble >&3
cat >&3 <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
INSERT INTO obj.items SELECT range, 'writer_a' FROM range(10000000, 10001000);
COPY (SELECT 1) TO '${WORK_DIR}/writer_a_committed.csv';
EOF
if wait_for "${WORK_DIR}/writer_a_committed.csv"; then
	echo "### ✅ writer A attached read-write and committed 1000 rows" >>"${SUMMARY}"
else
	echo "### ❌ writer A never committed" >>"${SUMMARY}"
	FAILED+=("writer_a")
fi

run_phase second_writer <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
INSERT INTO checks SELECT 'B sees A''s commit', '1000', count(*)::VARCHAR FROM obj.items WHERE payload = 'writer_a';
INSERT INTO obj.items SELECT range, 'writer_b' FROM range(20000000, 20000500);
CHECKPOINT obj;
DETACH obj;
EOF

cat >&3 <<EOF
.print LATE_WRITE_BEGIN
INSERT INTO obj.items SELECT range, 'writer_a_late' FROM range(30000000, 30001000);
CHECKPOINT obj;
COPY (SELECT 1) TO '${WORK_DIR}/writer_a_done.csv';
.quit
EOF
exec 3>&-
wait_for "${WORK_DIR}/writer_a_done.csv" || kill -9 "${A_PID}" 2>/dev/null || true
wait "${A_PID}" 2>/dev/null || true
A_REJECTED=false
sed -n '/LATE_WRITE_BEGIN/,$p' "${A_LOG}" | grep -qi "error" && A_REJECTED=true
echo "=== writer A after takeover:"
sed -n '/LATE_WRITE_BEGIN/,$p' "${A_LOG}" | grep -ai "error" | head -n 3 || true

run_phase after_second_writer <<EOF
ATTACH 'duckdb_objfs://e2e.db' AS obj;
INSERT INTO checks VALUES ('A''s late write rejected with an error', 'true', '${A_REJECTED}');
INSERT INTO checks SELECT 'A''s first commit kept', '1000', count(*)::VARCHAR FROM obj.items WHERE payload = 'writer_a';
INSERT INTO checks SELECT 'B''s commit kept', '500', count(*)::VARCHAR FROM obj.items WHERE payload = 'writer_b';
INSERT INTO checks SELECT 'A''s late write absent', '0', count(*)::VARCHAR FROM obj.items WHERE payload = 'writer_a_late';
INSERT INTO checks SELECT 'total rows', '$((ROWS + 2500))', count(*)::VARCHAR FROM obj.items;
CHECKPOINT obj;
DETACH obj;
ATTACH 'duckdb_objfs://e2e.db' AS obj (READ_ONLY);
INSERT INTO checks SELECT 'total rows after checkpoint + reopen', '$((ROWS + 2500))', count(*)::VARCHAR FROM obj.items;
DETACH obj;
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
