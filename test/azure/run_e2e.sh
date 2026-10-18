#!/usr/bin/env bash
# End-to-end test of the azure backend against live Microsoft OneLake (DuckLake: see test.sql).
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
