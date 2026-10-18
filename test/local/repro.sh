#!/usr/bin/env bash
# Reproductions, on the local backend (plain disk, no object store), of three bugs first seen in a
# multi-process stress test. Each prints REPRODUCED or NOT REPRODUCED; the script always exits 0.
#   1. readers:  a READ_ONLY attach held while another process writes and checkpoints reads blocks
#                from a newer version of the file (corruption errors / wrong answers).
#   2. abort:    a writer fenced by a second writer aborts the whole process (std::terminate)
#                instead of raising an error.
#   3. reattach: once fenced, a process can never attach that database again, not even READ_ONLY.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DUCKDB_BIN="${DUCKDB_BIN:-${ROOT_DIR}/build/release/duckdb}"
WORK="${REPRO_WORK_DIR:-$(mktemp -d)}"
SECONDS_READERS="${REPRO_SECONDS:-120}"
TRIALS="${REPRO_TRIALS:-10}"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/null}"
STORE="${WORK}/store"
mkdir -p "${WORK}" "${STORE}"
PIDS=()
trap 'for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done' EXIT

say() {
	echo "$*"
	echo "$*" >>"${SUMMARY}"
}

setup() {
	echo "LOAD duckdb_object_storage;"
	echo "SET duckdb_objfs_root = '${STORE}';"
}

wait_for() { # wait_for <file> <seconds>
	local i
	for ((i = 0; i < $2 * 10; i++)); do
		[[ -f "$1" ]] && return 0
		sleep 0.1
	done
	return 1
}

say "## duckdb_objfs bug repros, local backend (\`${STORE}\`)"
say ""
"${DUCKDB_BIN}" -c "SELECT version();" | tee -a "${SUMMARY}"

# ---------------------------------------------------------------- 1. readers
# filler always holds exactly 80 batches x 50000 rows with payload = md5(batch || i): every commit
# of the writer deletes the oldest batch and inserts a new one, and it checkpoints every 3 commits,
# so freed blocks get reused. Readers hold one READ_ONLY attach for many queries with a small
# memory_limit, so blocks are evicted and read again from storage. Every committed state has
# 4000000 rows, 80 batches and no bad payload; any other answer, or a corruption error, is the bug.
say "### 1. readers: READ_ONLY attach while another process writes"
R="${WORK}/readers"
mkdir -p "${R}/answers"
{
	setup
	cat <<EOF
ATTACH 'duckdb_objfs://readers.db' AS obj;
CREATE OR REPLACE TABLE obj.filler AS
SELECT 'b' || (range // 50000) AS batch, range % 50000 AS i, md5(('b' || (range // 50000)) || (range % 50000)) AS payload
FROM range(80 * 50000);
CHECKPOINT obj;
DETACH obj;
EOF
} >"${R}/setup.sql"
"${DUCKDB_BIN}" -bail -f "${R}/setup.sql" >"${R}/setup.log" 2>&1 || {
	say "setup failed: $(grep -a -i -m1 error "${R}/setup.log")"
}
{
	setup
	echo "ATTACH 'duckdb_objfs://readers.db' AS obj;"
	for ((s = 1; s <= 20000; s++)); do
		echo "BEGIN;"
		echo "DELETE FROM obj.filler WHERE batch = 'b$((s - 1))';"
		echo "INSERT INTO obj.filler SELECT 'b$((s + 79))', range, md5('b$((s + 79))' || range) FROM range(50000);"
		echo "COMMIT;"
		if ((s % 3 == 0)); then echo "CHECKPOINT obj;"; fi
	done
} >"${R}/writer.sql"
for r in 1 2 3; do
	{
		setup
		echo "SET memory_limit = '64MB';"
		echo "SET threads = 2;"
		for ((session = 1; session <= 200; session++)); do
			echo "ATTACH 'duckdb_objfs://readers.db' AS obj (READ_ONLY);"
			for ((q = 1; q <= 30; q++)); do
				echo "COPY (SELECT ${r} AS reader, ${session} AS session, ${q} AS query, count(*) AS row_count, count(DISTINCT batch) AS batches, count(*) FILTER (WHERE payload IS DISTINCT FROM md5(batch || i)) AS bad_payloads FROM obj.filler) TO '${R}/answers/${r}-${session}-${q}.csv';"
			done
			echo "DETACH obj;"
		done
	} >"${R}/reader_${r}.sql"
done
"${DUCKDB_BIN}" -bail -f "${R}/writer.sql" >"${R}/writer.log" 2>&1 &
WRITER=$!
PIDS+=("${WRITER}")
sleep 3
READER_PIDS=()
for r in 1 2 3; do
	# No -bail: a reader carries on after an error so errors can be counted.
	"${DUCKDB_BIN}" -f "${R}/reader_${r}.sql" >"${R}/reader_${r}.log" 2>&1 &
	READER_PIDS+=($!)
	PIDS+=($!)
done
sleep "${SECONDS_READERS}"
kill -0 "${WRITER}" 2>/dev/null || say "writer died: $(grep -a -i -m1 error "${R}/writer.log")"
kill -9 "${WRITER}" "${READER_PIDS[@]}" 2>/dev/null
wait "${WRITER}" "${READER_PIDS[@]}" 2>/dev/null
ANSWERS=$(find "${R}/answers" -name '*.csv' | wc -l | tr -d ' ')
WRONG=0
if [[ ${ANSWERS} -gt 0 ]]; then
	WRONG=$("${DUCKDB_BIN}" -noheader -list -c "SELECT count(*) FROM read_csv('${R}/answers/*.csv') WHERE row_count <> 4000000 OR batches <> 80 OR bad_payloads <> 0;")
	"${DUCKDB_BIN}" -markdown -c "SELECT * FROM read_csv('${R}/answers/*.csv') WHERE row_count <> 4000000 OR batches <> 80 OR bad_payloads <> 0 LIMIT 10;" | tee -a "${SUMMARY}"
fi
ERRORS=$(cat "${R}"/reader_*.log | grep -a -c -i -E '(^|[^a-z])error' || true)
say "reader answers: ${ANSWERS}, wrong answers: ${WRONG}, reader errors: ${ERRORS}"
if [[ ${ERRORS} -gt 0 ]]; then
	say '```'
	cat "${R}"/reader_*.log | grep -a -i -E '(^|[^a-z])error' | sed -E 's/near line [0-9]+: //' | cut -c1-250 |
		sort | uniq -c | sort -rn | head -n 8 | tee -a "${SUMMARY}"
	say '```'
fi
"${DUCKDB_BIN}" -bail -c "$(setup) ATTACH 'duckdb_objfs://readers.db' AS obj (READ_ONLY); SELECT count(*) AS row_count, count(DISTINCT batch) AS batches, count(*) FILTER (WHERE payload IS DISTINCT FROM md5(batch || i)) AS bad_payloads FROM obj.filler;" >"${R}/final.log" 2>&1
say "fresh process afterwards: $(grep -a -E '4000000|rror' "${R}/final.log" | tr -s ' │' ' ' | head -n 1)"
if [[ ${ERRORS} -gt 0 || ${WRONG} -gt 0 ]]; then say "**1. readers: REPRODUCED**"; else say "1. readers: NOT REPRODUCED"; fi
say ""

# ---------------------------------------------------------------- 2. abort
# Writer A commits in a loop. B attaches the same database read-write (fencing A) and commits.
# A should fail with an IO error; instead it is often killed by std::terminate (exit 134).
say "### 2. abort: fenced writer, ${TRIALS} trials each for a plain database and a DuckLake catalog"
A_DIR="${WORK}/abort"
mkdir -p "${A_DIR}"
abort_trial() { # abort_trial <kind> <n>: prints A's exit status and how it ended
	local kind=$1 n=$2 attach table a b i
	a="${A_DIR}/${kind}_${n}_a"
	b="${A_DIR}/${kind}_${n}_b"
	if [[ ${kind} == plain ]]; then
		attach="ATTACH 'duckdb_objfs://abort_${n}.db' AS obj;"
		table="obj.t"
	else
		mkdir -p "${A_DIR}/lake_${n}"
		attach="INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:duckdb_objfs://abort_${n}.ducklake' AS obj (DATA_PATH '${A_DIR}/lake_${n}/');"
		table="obj.t"
	fi
	{
		setup
		echo "${attach}"
		echo "CREATE OR REPLACE TABLE ${table} (i BIGINT);"
		echo "COPY (SELECT 1) TO '${a}.ready';"
		for ((i = 0; i < 200000; i++)); do echo "INSERT INTO ${table} VALUES (${i});"; done
	} >"${a}.sql"
	{
		setup
		echo "${attach}"
		echo "INSERT INTO ${table} VALUES (-1);"
		echo "DETACH obj;"
	} >"${b}.sql"
	"${DUCKDB_BIN}" -bail -f "${a}.sql" >"${a}.log" 2>&1 &
	local pid=$!
	PIDS+=("${pid}")
	wait_for "${a}.ready" 60 || true
	sleep 1
	"${DUCKDB_BIN}" -bail -f "${b}.sql" >"${b}.log" 2>&1
	local b_status=$?
	local status=0 i
	for ((i = 0; i < 300; i++)); do
		kill -0 "${pid}" 2>/dev/null || break
		sleep 0.1
	done
	kill -9 "${pid}" 2>/dev/null
	wait "${pid}" 2>/dev/null
	status=$?
	local how
	if grep -aq 'terminate called' "${a}.log"; then
		how="std::terminate: $(grep -a -m1 'what()' "${a}.log" | cut -c1-200)"
	else
		how="$(grep -a -i -m1 -E '(^|[^a-z])error' "${a}.log" | cut -c1-200)"
	fi
	echo "${kind} trial ${n}: A exit ${status}, B exit ${b_status}: ${how}"
}
ABORTS=0
for kind in plain ducklake; do
	for ((n = 1; n <= TRIALS; n++)); do
		line=$(abort_trial "${kind}" "${n}")
		say "- ${line}"
		[[ ${line} == *"std::terminate"* ]] && ABORTS=$((ABORTS + 1))
	done
done
if [[ ${ABORTS} -gt 0 ]]; then say "**2. abort: REPRODUCED (${ABORTS} of $((2 * TRIALS)) trials)**"; else say "2. abort: NOT REPRODUCED"; fi
say ""

# ---------------------------------------------------------------- 3. reattach
# A attaches read-write and commits, then idles. B attaches read-write and commits (fencing A). A
# then DETACHes and tries to attach the database again READ_ONLY under a new name. A fresh process
# can read it; A cannot, for as long as it lives.
say "### 3. reattach: a fenced process attaching the database again"
D="${WORK}/reattach"
mkdir -p "${D}"
mkfifo "${D}/a.in"
"${DUCKDB_BIN}" <"${D}/a.in" >"${D}/a.log" 2>&1 &
A_PID=$!
PIDS+=("${A_PID}")
exec 5>"${D}/a.in"
{
	setup
	echo "ATTACH 'duckdb_objfs://reattach.db' AS obj;"
	echo "CREATE OR REPLACE TABLE obj.t AS SELECT range AS i FROM range(1000);"
	echo "COPY (SELECT 1) TO '${D}/a_ready';"
} >&5
wait_for "${D}/a_ready" 60 || say "A never got ready: $(grep -a -i -m1 error "${D}/a.log")"
"${DUCKDB_BIN}" -bail -c "$(setup) ATTACH 'duckdb_objfs://reattach.db' AS obj; INSERT INTO obj.t VALUES (1000); DETACH obj;" >"${D}/b.log" 2>&1 ||
	say "B failed: $(grep -a -i -m1 error "${D}/b.log")"
sleep 3
{
	echo ".print ---- DETACH obj"
	echo "DETACH obj;"
	echo ".print ---- ATTACH READ_ONLY AS again"
	echo "ATTACH 'duckdb_objfs://reattach.db' AS again (READ_ONLY);"
	echo "SELECT 'ROWS=' || count(*) FROM again.t;"
	echo "COPY (SELECT 1) TO '${D}/a_done';"
	echo ".quit"
} >&5
exec 5>&-
wait_for "${D}/a_done" 60 || kill -9 "${A_PID}" 2>/dev/null
wait "${A_PID}" 2>/dev/null
"${DUCKDB_BIN}" -bail -c "$(setup) ATTACH 'duckdb_objfs://reattach.db' AS obj (READ_ONLY); SELECT 'ROWS=' || count(*) FROM obj.t;" >"${D}/c.log" 2>&1
say '```'
sed -n '/---- DETACH obj/,$p' "${D}/a.log" | grep -a -v '^Run Time' | cut -c1-250 | tee -a "${SUMMARY}"
say "fresh process: $(grep -a -o -m1 -E 'ROWS=[0-9]+|[A-Za-z]+ Error.*' "${D}/c.log" | cut -c1-200)"
say '```'
if ! grep -aq 'ROWS=1001' "${D}/a.log" && grep -aq 'ROWS=1001' "${D}/c.log"; then
	say "**3. reattach: REPRODUCED**"
else
	say "3. reattach: NOT REPRODUCED"
fi
exit 0
