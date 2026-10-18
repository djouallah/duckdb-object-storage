# Shared helpers for the OneLake scripts (run_e2e.sh, stress.sh). Source it after setting ROOT_DIR
# and WORK_DIR. Every phase is a separate duckdb process; checks go to stdout and the job summary.

DUCKDB_BIN="${DUCKDB_BIN:-${ROOT_DIR}/build/release/duckdb}"
WORKSPACE="${ONELAKE_WORKSPACE:?set ONELAKE_WORKSPACE}"
ROOT="${ONELAKE_ROOT:?set ONELAKE_ROOT}"
: "${ONELAKE_TOKEN:?set ONELAKE_TOKEN}"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/null}"
# The azure extension's curl transport needs to be told where the CA bundle is.
export CURL_CA_INFO="${CURL_CA_INFO:-/etc/ssl/certs/ca-certificates.crt}"
FAILED=()

if [[ ! -x "${DUCKDB_BIN}" ]]; then
	echo "DuckDB executable not found at ${DUCKDB_BIN}" >&2
	exit 1
fi

# Each process initializes the filesystem from scratch. Pass "ducklake" to also load ducklake.
preamble() {
	cat <<EOF
.timer on
INSTALL azure;
LOAD azure;
EOF
	if [[ "${1:-}" == ducklake ]]; then
		printf 'INSTALL ducklake;\nLOAD ducklake;\n'
	fi
	cat <<EOF
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

# run_phase <name> [preamble arg] <<SQL. PHASE_WRAP, if set, prefixes the duckdb command (e.g. timeout).
run_phase() {
	local name="$1"
	local sql="${WORK_DIR}/${name}.sql" log="${WORK_DIR}/${name}.log" checks="${WORK_DIR}/${name}.md"
	{
		preamble "${2:-}"
		cat
		report "${checks}"
	} >"${sql}"
	echo "=== phase: ${name}"
	local start=${SECONDS} status=0
	# shellcheck disable=SC2086
	${PHASE_WRAP:-} "${DUCKDB_BIN}" -bail -f "${sql}" 2>&1 | tee "${log}" || status=$?
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

# wait_for <file> [seconds]: waits for a marker file, default 120 s.
wait_for() {
	local tries=$((${2:-120} * 2))
	for _ in $(seq 1 "${tries}"); do
		[[ -f "$1" ]] && return 0
		sleep 0.5
	done
	return 1
}
