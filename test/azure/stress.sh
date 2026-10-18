#!/usr/bin/env bash
# Stress test of the azure backend against live OneLake. Tries to break correctness: racing writers
# that fence each other, kill -9 at random points, readers during writes, a sync above SlateDB's
# 1 GiB unflushed limit, and the conditional writes OneLake must honour for fencing to work.
#
# Invariant everywhere: a commit that returned OK (an "ack", written locally only after COMMIT, as
# every writer runs with -bail) is never lost, no commit is torn, and nothing is read back wrong.
# Same requirements as run_e2e.sh. Knobs: STRESS_WRITERS, STRESS_READERS, STRESS_SECONDS,
# STRESS_CYCLES, STRESS_BIG_MB, STRESS_WORK_DIR (kept, for the CI artifact).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WRITERS="${STRESS_WRITERS:-4}"
READERS="${STRESS_READERS:-3}"
DURATION="${STRESS_SECONDS:-90}"
CYCLES="${STRESS_CYCLES:-6}"
BIG_MB="${STRESS_BIG_MB:-1500}"
WORK_DIR="${STRESS_WORK_DIR:-$(mktemp -d)}"
mkdir -p "${WORK_DIR}"
# shellcheck source=test/azure/common.sh
source "${ROOT_DIR}/test/azure/common.sh"
LAKEHOUSE="${ONELAKE_LAKEHOUSE:-${ROOT%%/*}}"
BLOB="https://onelake.blob.fabric.microsoft.com/${WORKSPACE}"
FILLER_ROWS=50000
# What a fenced writer reports; any other error from a writer is a finding.
FENCE_RE='newer DB client|[Ff]enced'
BUSY="SELECT count(*) FROM range(1000000000000);"

# Never a bare `wait` below: it would also wait for the long-lived storm process.
PIDS=()
cleanup() {
	for pid in "${PIDS[@]}"; do kill -9 "${pid}" 2>/dev/null || true; done
}
trap cleanup EXIT

# launch <sql> <log>: starts duckdb in the background; its pid is left in LAUNCHED.
launch() {
	"${DUCKDB_BIN}" -bail -f "$1" >"$2" 2>&1 &
	LAUNCHED=$!
	PIDS+=("${LAUNCHED}")
}

info() {
	echo "$*"
	echo "$*" >>"${SUMMARY}"
}

fail() {
	FAILED+=("$1")
	info "### ❌ $1: $2"
}

# classify <log>: "ok" (no error), "fenced", or "unexpected: <first error line>".
classify() {
	local first
	first=$(grep -a -m1 -i -E '(^|[^a-z])error' "$1" || true)
	if [[ -z "${first}" ]]; then
		echo ok
	elif grep -aqE "${FENCE_RE}" "$1"; then
		echo fenced
	else
		echo "unexpected: ${first:0:300}"
	fi
}

# acks_table <dir>: loads every ack (writer, incarnation, seq) written by the writers in <dir>.
acks_table() {
	cat <<EOF
CREATE OR REPLACE TEMP TABLE acks AS
SELECT * FROM read_csv('$1/*.csv', header = true,
                       columns = {'writer': 'VARCHAR', 'incarnation': 'INTEGER', 'seq': 'INTEGER'});
EOF
}

new_acks_dir() {
	mkdir -p "$1"
	echo "writer,incarnation,seq" >"$1/_none.csv"
}

ack_sql() { # ack_sql <dir> <writer> <incarnation> <seq>
	echo "COPY (SELECT '$2' AS writer, $3 AS incarnation, $4 AS seq) TO '$1/$2-$3-$4.csv';"
}

ack_count() {
	find "$1" -name '*-*.csv' | wc -l | tr -d ' '
}

# prefix_check <label> <relation with writer, incarnation, seq>: one writer process commits in order,
# so what survives of it must be seqs 1..k with no hole, k >= its last ack, and at most one more
# (committed, killed before the ack). Catches lost, out-of-order and phantom commits.
prefix_check() {
	cat <<EOF
INSERT INTO checks SELECT '$1', '0', count(*)::VARCHAR FROM (
    SELECT writer, incarnation, count(DISTINCT seq) AS n, max(seq) AS k FROM $2 GROUP BY ALL) l
FULL JOIN (SELECT writer, incarnation, max(seq) AS acked FROM acks GROUP BY ALL) a USING (writer, incarnation)
WHERE coalesce(l.n, 0) <> coalesce(l.k, 0)
   OR coalesce(l.k, 0) < coalesce(a.acked, 0)
   OR coalesce(l.k, 0) > coalesce(a.acked, 0) + 1;
EOF
}

# reap <pid...>: kill -9 and collect.
reap() {
	local pid
	for pid in "$@"; do
		kill -9 "${pid}" 2>/dev/null || true
		wait "${pid}" 2>/dev/null || true
	done
}

# ---------------------------------------------------------------- the bank
# accounts: 1000 x 1000. Each transfer moves money between two accounts and logs it in the ledger in
# one transaction. filler: 50k-row batches with payload = md5(batch || i), inserted, deleted and
# checkpointed so blocks get freed and reused.

bank_setup() { # bank_setup <db>
	cat <<EOF
ATTACH 'duckdb_objfs://$1' AS obj;
CREATE OR REPLACE TABLE obj.accounts AS SELECT range::INTEGER AS id, 1000::BIGINT AS balance FROM range(1000);
CREATE OR REPLACE TABLE obj.ledger (writer VARCHAR, incarnation INTEGER, seq INTEGER, src INTEGER, dst INTEGER, amount INTEGER);
CREATE OR REPLACE TABLE obj.filler (batch VARCHAR, i BIGINT, payload VARCHAR);
CHECKPOINT obj;
INSERT INTO checks SELECT 'bank created', '1000000', sum(balance)::VARCHAR FROM obj.accounts;
DETACH obj;
EOF
}

transfer_sql() { # transfer_sql <writer> <incarnation> <seq>: one transfer, body of a transaction
	local src=$((RANDOM % 1000)) amount=$((1 + RANDOM % 50))
	local dst=$(((src + 1 + RANDOM % 999) % 1000))
	cat <<EOF
UPDATE obj.accounts SET balance = balance + CASE WHEN id = ${dst} THEN ${amount} ELSE -${amount} END WHERE id IN (${src}, ${dst});
INSERT INTO obj.ledger VALUES ('$1', $2, $3, ${src}, ${dst}, ${amount});
EOF
}

filler_insert_sql() { # filler_insert_sql <batch>
	echo "INSERT INTO obj.filler SELECT '$1', range, md5('$1' || range) FROM range(${FILLER_ROWS});"
}

# bank_writer_sql <db> <writer> <incarnation> <txns> <acks>: transfers; every 20th also inserts a
# filler batch and deletes the one from 40 transfers ago; every 30th checkpoints.
bank_writer_sql() {
	local db=$1 w=$2 inc=$3 n=$4 acks=$5 s
	preamble
	echo "ATTACH 'duckdb_objfs://${db}' AS obj;"
	for ((s = 1; s <= n; s++)); do
		echo "BEGIN;"
		transfer_sql "${w}" "${inc}" "${s}"
		echo "COMMIT;"
		ack_sql "${acks}" "${w}" "${inc}" "${s}"
		if ((s % 20 == 0)); then
			filler_insert_sql "${w}-${inc}-${s}"
			if ((s > 40)); then echo "DELETE FROM obj.filler WHERE batch = '${w}-${inc}-$((s - 40))';"; fi
		fi
		if ((s % 30 == 0)); then echo "CHECKPOINT obj;"; fi
	done
}

# bank_checks <alias> <label prefix>: the bank invariants (needs the acks table).
bank_checks() {
	local a=$1 p=$2
	cat <<EOF
INSERT INTO checks SELECT '${p}total balance | accounts', '1000000 | 1000',
    sum(balance)::VARCHAR || ' | ' || count(*)::VARCHAR FROM ${a}.accounts;
INSERT INTO checks SELECT '${p}accounts not matching the ledger', '0', count(*)::VARCHAR
FROM ${a}.accounts acc LEFT JOIN (
    SELECT id, sum(delta) AS delta FROM (
        SELECT dst AS id, amount AS delta FROM ${a}.ledger UNION ALL SELECT src, -amount FROM ${a}.ledger)
    GROUP BY id) l USING (id)
WHERE acc.balance <> 1000 + coalesce(l.delta, 0);
INSERT INTO checks SELECT '${p}duplicate ledger rows', '0',
    (count(*) - count(DISTINCT writer || '/' || incarnation || '/' || seq))::VARCHAR FROM ${a}.ledger;
INSERT INTO checks SELECT '${p}acked transfers missing', '0', count(*)::VARCHAR
FROM acks ANTI JOIN ${a}.ledger USING (writer, incarnation, seq);
EOF
	prefix_check "${p}commits not a gapless prefix of acks" "${a}.ledger"
	cat <<EOF
INSERT INTO checks SELECT '${p}torn filler batches', '0', count(*)::VARCHAR FROM (
    SELECT batch FROM ${a}.filler GROUP BY batch
    HAVING count(*) <> ${FILLER_ROWS} OR count(DISTINCT i) <> ${FILLER_ROWS});
INSERT INTO checks SELECT '${p}corrupted filler payloads', '0', count(*)::VARCHAR
FROM ${a}.filler WHERE payload IS DISTINCT FROM md5(batch || i);
EOF
}

# bank_verify <db> <acks> [full]: read-only check; "full" first replays + checkpoints read-write.
bank_verify() {
	acks_table "$2"
	if [[ "${3:-}" == full ]]; then
		echo "ATTACH 'duckdb_objfs://$1' AS obj;"
		bank_checks obj "rw: "
		echo "CHECKPOINT obj;"
		echo "DETACH obj;"
	fi
	echo "ATTACH 'duckdb_objfs://$1' AS obj (READ_ONLY);"
	bank_checks obj "ro: "
	echo "DETACH obj;"
}

# classify_writers <dir> <label>: sorts the w*.log of a writer storm into fenced / killed by us /
# unexpected; unexpected errors fail <label>.
classify_writers() {
	local dir=$1 label=$2 log result fenced=0 clean=0 unexpected=()
	for log in "${dir}"/w*.log; do
		result=$(classify "${log}")
		case "${result}" in
		ok) clean=$((clean + 1)) ;;
		fenced) fenced=$((fenced + 1)) ;;
		*) unexpected+=("$(basename "${log}"): ${result}") ;;
		esac
	done
	info "${label}: $((fenced + clean + ${#unexpected[@]})) writer processes, ${fenced} fenced, ${clean} killed by the harness"
	if [[ ${#unexpected[@]} -gt 0 ]]; then
		fail "${label}_errors" "${#unexpected[@]} writers failed with a non-fencing error"
		printf -- '- `%s`\n' "${unexpected[@]:0:10}" | tee -a "${SUMMARY}"
	fi
	if [[ ${fenced} -eq 0 ]]; then
		fail "${label}_fencing" "no writer was ever fenced: the writers did not race"
	fi
}

{
	echo "## duckdb_objfs stress on OneLake: \`${WORKSPACE}/${ROOT}\`"
	echo
	echo "writers=${WRITERS} readers=${READERS} seconds=${DURATION} cycles=${CYCLES} big=${BIG_MB}MB"
	echo
} >>"${SUMMARY}"

# ---------------------------------------------------------------- 1. cas_race
# SlateDB fences writers with conditional PUTs (If-None-Match: * to create a WAL/manifest object,
# If-Match: <etag> to replace one). Race 8 PUTs per round straight at the OneLake blob endpoint:
# exactly one may win, and the stored body must be the winner's.
echo "=== phase: cas_race"
CAS_DIR="${WORK_DIR}/cas"
mkdir -p "${CAS_DIR}"
CAS_PREFIX="${ROOT}/stress_cas/${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$"
CAS_BAD=0
cas_put() { # cas_put <blob url> <body> <condition header> <out file>
	curl -s -o /dev/null -w '%{http_code}' -X PUT -H "Authorization: Bearer ${ONELAKE_TOKEN}" \
		-H "x-ms-version: 2023-11-03" -H "x-ms-blob-type: BlockBlob" -H "$3" \
		--data-binary "$2" "$1" >"$4" || echo "curl-failed" >"$4"
}
cas_round() { # cas_round <url> <condition header> <round tag>
	local k code body winners=0 winner="" codes="" pids=()
	for k in $(seq 1 8); do
		cas_put "$1" "$3-$k" "$2" "${CAS_DIR}/$3-$k" &
		pids+=($!)
	done
	wait "${pids[@]}" || true
	for k in $(seq 1 8); do
		code=$(cat "${CAS_DIR}/$3-$k")
		codes+="${code} "
		if [[ "${code}" == 201 ]]; then
			winners=$((winners + 1))
			winner="$3-$k"
		fi
	done
	body=$(curl -s -H "Authorization: Bearer ${ONELAKE_TOKEN}" -H "x-ms-version: 2023-11-03" "$1" || true)
	echo "$3: ${codes}" >>"${CAS_DIR}/codes.txt"
	if [[ ${winners} -ne 1 || "${body}" != "${winner}" ]]; then
		CAS_BAD=$((CAS_BAD + 1))
		echo "cas round $3: ${winners} winners (${codes}), stored body '${body}'" | tee -a "${SUMMARY}"
	fi
}
cas_etag() {
	curl -s -I -H "Authorization: Bearer ${ONELAKE_TOKEN}" -H "x-ms-version: 2023-11-03" "$1" |
		tr -d '\r' | sed -n 's/^[Ee][Tt][Aa][Gg]: //p'
}
for round in $(seq 1 30); do
	url="${BLOB}/${CAS_PREFIX}/blob-${round}"
	cas_round "${url}" "If-None-Match: *" "create${round}"
	cas_round "${url}" "If-Match: $(cas_etag "${url}")" "update${round}"
	curl -s -o /dev/null -X DELETE -H "Authorization: Bearer ${ONELAKE_TOKEN}" -H "x-ms-version: 2023-11-03" "${url}" || true
done
CAS_SEEN=$(cut -d: -f2 "${CAS_DIR}/codes.txt" | tr ' ' '\n' | grep . | sort | uniq -c | awk '{printf " %s x%s", $2, $1}')
if [[ ${CAS_BAD} -eq 0 ]]; then
	info "### ✅ cas_race: 30 create + 30 replace rounds of 8 racing conditional PUTs, one winner each (codes:${CAS_SEEN})"
else
	fail cas_race "${CAS_BAD} of 60 rounds did not have exactly one winner holding the blob (codes:${CAS_SEEN})"
fi
echo >>"${SUMMARY}"

# ---------------------------------------------------------------- 2. crash_torture
# One bank writer killed -9 at a random moment, CYCLES times; each cycle a new incarnation carries on
# from whatever the last crash left (WAL replay included). Read-only check after each crash.
CR_DB=stress_crash.db
CR_ACKS="${WORK_DIR}/crash_acks"
new_acks_dir "${CR_ACKS}"
run_phase crash_setup < <(bank_setup "${CR_DB}")
for cycle in $(seq 1 "${CYCLES}"); do
	sql="${WORK_DIR}/crash_${cycle}.sql" log="${WORK_DIR}/crash_${cycle}.log"
	bank_writer_sql "${CR_DB}" w1 "${cycle}" 3000 "${CR_ACKS}" >"${sql}"
	launch "${sql}" "${log}"
	pid=${LAUNCHED}
	delay=$((2 + RANDOM % 29))
	sleep "${delay}.$((RANDOM % 10))"
	if kill -0 "${pid}" 2>/dev/null; then
		reap "${pid}"
		info "crash cycle ${cycle}: kill -9 after ${delay}s, $(ack_count "${CR_ACKS}") acked transfers so far"
	else
		reap "${pid}"
		fail "crash_${cycle}" "writer died before the kill: $(classify "${log}")"
	fi
	run_phase "crash_verify_${cycle}" < <(bank_verify "${CR_DB}" "${CR_ACKS}")
done
run_phase crash_verify_final < <(bank_verify "${CR_DB}" "${CR_ACKS}" full)

# ---------------------------------------------------------------- 3. checkpoint_kill
# Each iteration commits (delete tag k-3, insert tag k) and acks, then: even k checkpoints and is
# killed within ~20 ms of it returning (WAL remove/move not flushed yet); odd k is killed 0-3 s into
# the checkpoint. The table must hold exactly the acked tags, every time.
CK_DB=stress_ckpt.db
CK_ROWS=300000
CK_DIR="${WORK_DIR}/ckpt"
mkdir -p "${CK_DIR}"
run_phase ckpt_setup <<EOF
ATTACH 'duckdb_objfs://${CK_DB}' AS obj;
CREATE OR REPLACE TABLE obj.t (tag INTEGER, i BIGINT, payload VARCHAR);
CHECKPOINT obj;
DETACH obj;
EOF
declare -A CK_PRESENT=()
ckpt_checks() { # ckpt_checks <label prefix>
	local tags count
	tags=$(printf '%s\n' "${!CK_PRESENT[@]}" | grep . | sort -n | paste -sd, - || true)
	count=${#CK_PRESENT[@]}
	cat <<EOF
INSERT INTO checks SELECT '$1tags present', '${tags}',
    (SELECT coalesce(string_agg(tag::VARCHAR, ',' ORDER BY tag), '') FROM (SELECT DISTINCT tag FROM obj.t));
INSERT INTO checks SELECT '$1rows', '$((count * CK_ROWS))', count(*)::VARCHAR FROM obj.t;
INSERT INTO checks SELECT '$1torn tags', '0', count(*)::VARCHAR FROM (
    SELECT tag FROM obj.t GROUP BY tag HAVING count(*) <> ${CK_ROWS} OR count(DISTINCT i) <> ${CK_ROWS});
INSERT INTO checks SELECT '$1corrupted payloads', '0', count(*)::VARCHAR
FROM obj.t WHERE payload IS DISTINCT FROM md5(tag || '-' || i);
EOF
}
for k in $(seq 1 10); do
	sql="${CK_DIR}/${k}.sql" log="${CK_DIR}/${k}.log" ack="${CK_DIR}/ack_${k}.csv" mark="${CK_DIR}/mark_${k}.csv"
	{
		preamble
		cat <<EOF
ATTACH 'duckdb_objfs://${CK_DB}' AS obj;
BEGIN;
DELETE FROM obj.t WHERE tag = $((k - 3));
INSERT INTO obj.t SELECT ${k}, range, md5('${k}-' || range) FROM range(${CK_ROWS});
COMMIT;
COPY (SELECT 1) TO '${ack}';
EOF
		if ((k % 2 == 0)); then
			echo "CHECKPOINT obj;"
			echo "COPY (SELECT 1) TO '${mark}';"
		else
			echo "COPY (SELECT 1) TO '${mark}';"
			echo "CHECKPOINT obj;"
		fi
		echo "${BUSY}"
	} >"${sql}"
	launch "${sql}" "${log}"
	pid=${LAUNCHED}
	for _ in $(seq 1 15000); do
		[[ -f "${mark}" ]] && break
		kill -0 "${pid}" 2>/dev/null || break
		sleep 0.02
	done
	if [[ -f "${mark}" ]]; then
		if ((k % 2 == 1)); then sleep "$((RANDOM % 3)).$((RANDOM % 10))"; fi
	else
		fail "ckpt_${k}" "writer exited before its marker: $(classify "${log}")"
	fi
	reap "${pid}"
	if [[ -f "${ack}" ]]; then
		CK_PRESENT[${k}]=1
		unset "CK_PRESENT[$((k - 3))]"
	fi
	run_phase "ckpt_verify_${k}" < <(
		echo "ATTACH 'duckdb_objfs://${CK_DB}' AS obj (READ_ONLY);"
		ckpt_checks ""
		echo "DETACH obj;"
	)
done
run_phase ckpt_verify_final < <(
	echo "ATTACH 'duckdb_objfs://${CK_DB}' AS obj;"
	ckpt_checks "rw: "
	echo "CHECKPOINT obj;"
	echo "DETACH obj;"
	echo "ATTACH 'duckdb_objfs://${CK_DB}' AS obj (READ_ONLY);"
	ckpt_checks "ro: "
	echo "DETACH obj;"
)

# ---------------------------------------------------------------- 4. writer_storm
# WRITERS bank writers on one database. Every writer that dies is relaunched as a new incarnation,
# whose ATTACH fences the others: continuous ping-pong fencing. Fence errors are expected; any
# other error is a finding. A long-lived process fenced at the start then tries to re-attach.
ST_DB=stress_storm.db
ST_DIR="${WORK_DIR}/storm"
ST_ACKS="${ST_DIR}/acks"
new_acks_dir "${ST_ACKS}"
run_phase storm_setup < <(bank_setup "${ST_DB}")

# The long-lived process: attaches, commits one transfer, then idles until the storm is over.
P_IN="${ST_DIR}/long_lived.in"
P_LOG="${ST_DIR}/long_lived.log"
mkfifo "${P_IN}"
"${DUCKDB_BIN}" <"${P_IN}" >"${P_LOG}" 2>&1 &
P_PID=$!
PIDS+=("${P_PID}")
exec 4>"${P_IN}"
preamble >&4
{
	echo "ATTACH 'duckdb_objfs://${ST_DB}' AS obj;"
	echo "BEGIN;"
	transfer_sql long 1 1
	echo "COMMIT;"
	ack_sql "${ST_ACKS}" long 1 1
} >&4
wait_for "${ST_ACKS}/long-1-1.csv" || fail storm_long_lived "long-lived writer never committed"

declare -A ST_PID=() ST_INC=()
storm_launch() {
	local w=$1
	ST_INC[${w}]=$((${ST_INC[${w}]:-0} + 1))
	local tag="w${w}-${ST_INC[${w}]}"
	bank_writer_sql "${ST_DB}" "w${w}" "${ST_INC[${w}]}" 2000 "${ST_ACKS}" >"${ST_DIR}/${tag}.sql"
	launch "${ST_DIR}/${tag}.sql" "${ST_DIR}/${tag}.log"
	ST_PID[${w}]=${LAUNCHED}
}
for w in $(seq 1 "${WRITERS}"); do storm_launch "${w}"; done
end=$((SECONDS + DURATION))
while ((SECONDS < end)); do
	for w in $(seq 1 "${WRITERS}"); do
		if ! kill -0 "${ST_PID[${w}]}" 2>/dev/null; then
			reap "${ST_PID[${w}]}"
			sleep "0.$((RANDOM % 10))"
			storm_launch "${w}"
		fi
	done
	sleep 0.5
done
for w in $(seq 1 "${WRITERS}"); do reap "${ST_PID[${w}]}"; done

# The long-lived process was fenced by the first storm writer. Can it detach and re-attach?
{
	echo ".print REATTACH_BEGIN"
	echo "DETACH obj;"
	echo "ATTACH 'duckdb_objfs://${ST_DB}' AS again (READ_ONLY);"
	echo "SELECT 'REATTACH_ROWS=' || count(*) FROM again.accounts;"
	echo "COPY (SELECT 1) TO '${ST_DIR}/long_lived_done.csv';"
	echo ".quit"
} >&4
exec 4>&-
wait_for "${ST_DIR}/long_lived_done.csv" 180 || true
reap "${P_PID}"
if grep -aq 'REATTACH_ROWS=1000' "${P_LOG}"; then
	info "fenced long-lived process re-attached read-only after DETACH: yes"
else
	info "fenced long-lived process re-attached read-only after DETACH: **no** (informational): $(sed -n '/REATTACH_BEGIN/,$p' "${P_LOG}" | grep -a -i -m1 error | cut -c1-300 || true)"
fi

classify_writers "${ST_DIR}" storm
info "storm: $(ack_count "${ST_ACKS}") acked transfers"
for w in $(seq 1 "${WRITERS}"); do
	info "- w${w}: ${ST_INC[${w}]} incarnations, $(find "${ST_ACKS}" -name "w${w}-*.csv" | wc -l | tr -d ' ') acks"
done
run_phase storm_verify < <(bank_verify "${ST_DB}" "${ST_ACKS}" full)

# ---------------------------------------------------------------- 5. readers_during_writes
# A writer churns 80 filler batches (each transaction deletes the oldest, inserts a new one and makes
# a transfer; checkpoint every 3), so blocks are freed and reused. READERS read-only processes with a
# 64MB memory limit (blocks get evicted and re-read) hold each attach for minutes, well past the
# SlateDB reader's 10 s refresh, re-checking the invariants. Every committed state satisfies them,
# so a wrong answer is a bug. Reader errors are reported.
RD_DB=stress_readers.db
RD_DIR="${WORK_DIR}/readers"
RD_ACKS="${RD_DIR}/acks"
RD_RESULTS="${RD_DIR}/results"
RD_BATCHES=80
new_acks_dir "${RD_ACKS}"
mkdir -p "${RD_RESULTS}"
readers_setup_sql() {
	bank_setup "${RD_DB}"
	cat <<EOF
ATTACH 'duckdb_objfs://${RD_DB}' AS obj;
INSERT INTO obj.filler SELECT 'pre-' || (range // ${FILLER_ROWS} + 1), range % ${FILLER_ROWS},
    md5(('pre-' || (range // ${FILLER_ROWS} + 1)) || (range % ${FILLER_ROWS})) FROM range(${RD_BATCHES} * ${FILLER_ROWS});
CHECKPOINT obj;
INSERT INTO checks SELECT 'filler batches', '${RD_BATCHES}', count(DISTINCT batch)::VARCHAR FROM obj.filler;
DETACH obj;
EOF
}
run_phase readers_setup < <(readers_setup_sql)
{
	preamble
	echo "ATTACH 'duckdb_objfs://${RD_DB}' AS obj;"
	for s in $(seq 1 3000); do
		if ((s <= RD_BATCHES)); then oldest="pre-${s}"; else oldest="r-$((s - RD_BATCHES))"; fi
		echo "BEGIN;"
		echo "DELETE FROM obj.filler WHERE batch = '${oldest}';"
		filler_insert_sql "r-${s}"
		transfer_sql rw 1 "${s}"
		echo "COMMIT;"
		ack_sql "${RD_ACKS}" rw 1 "${s}"
		if ((s % 3 == 0)); then echo "CHECKPOINT obj;"; fi
	done
} >"${RD_DIR}/writer.sql"
for r in $(seq 1 "${READERS}"); do
	{
		preamble
		echo "SET memory_limit = '64MB';"
		echo "SET threads = 2;"
		for session in $(seq 1 40); do
			echo "ATTACH 'duckdb_objfs://${RD_DB}' AS obj (READ_ONLY);"
			for q in $(seq 1 8); do
				cat <<EOF
COPY (SELECT ${r} AS reader, ${session} AS session, ${q} AS query, now() AS ts,
    (SELECT sum(balance) FROM obj.accounts) AS balance,
    (SELECT count(*) FROM obj.accounts) AS accounts,
    (SELECT count(*) FROM obj.accounts acc LEFT JOIN (
        SELECT id, sum(delta) AS delta FROM (
            SELECT dst AS id, amount AS delta FROM obj.ledger UNION ALL SELECT src, -amount FROM obj.ledger)
        GROUP BY id) l USING (id) WHERE acc.balance <> 1000 + coalesce(l.delta, 0)) AS mismatched,
    (SELECT count(*) FROM (SELECT batch FROM obj.filler GROUP BY batch HAVING count(*) <> ${FILLER_ROWS})) AS torn,
    (SELECT count(*) FROM obj.filler WHERE payload IS DISTINCT FROM md5(batch || i)) AS corrupt,
    (SELECT count(DISTINCT batch) FROM obj.filler) AS batches
) TO '${RD_RESULTS}/r${r}-${session}-${q}.csv';
EOF
			done
			echo "DETACH obj;"
		done
	} >"${RD_DIR}/reader_${r}.sql"
done
launch "${RD_DIR}/writer.sql" "${RD_DIR}/writer.log"
RD_WRITER=${LAUNCHED}
sleep 5
RD_PIDS=()
for r in $(seq 1 "${READERS}"); do
	# No -bail: a reader keeps going after an error, and errors are counted.
	"${DUCKDB_BIN}" -f "${RD_DIR}/reader_${r}.sql" >"${RD_DIR}/reader_${r}.log" 2>&1 &
	RD_PIDS+=($!)
	PIDS+=($!)
done
sleep "${DURATION}"
if ! kill -0 "${RD_WRITER}" 2>/dev/null; then
	fail readers_writer "writer died during the read phase: $(classify "${RD_DIR}/writer.log")"
fi
reap "${RD_WRITER}" "${RD_PIDS[@]}"
RD_ERRORS=$(cat "${RD_DIR}"/reader_*.log | grep -a -c -i -E '(^|[^a-z])error' || true)
info "readers_during_writes: $(ack_count "${RD_ACKS}") writer commits, $(find "${RD_RESULTS}" -name '*.csv' | wc -l | tr -d ' ') reader answers, ${RD_ERRORS} reader errors"
if [[ ${RD_ERRORS} -gt 0 ]]; then
	info "reader errors (most frequent):"
	cat "${RD_DIR}"/reader_*.log | grep -a -i -E '(^|[^a-z])error' | sed -E 's/near line [0-9]+: //' | cut -c1-200 |
		sort | uniq -c | sort -rn | head -n 5 | sed 's/^/    /' | tee -a "${SUMMARY}"
fi
readers_verify_sql() {
	if compgen -G "${RD_RESULTS}/*.csv" >/dev/null; then
		echo "CREATE TEMP TABLE answers AS SELECT * FROM read_csv('${RD_RESULTS}/*.csv');"
	else
		echo "CREATE TEMP TABLE answers (balance BIGINT, accounts BIGINT, mismatched BIGINT, torn BIGINT, corrupt BIGINT, batches BIGINT);"
	fi
	cat <<EOF
INSERT INTO checks SELECT 'readers answered', 'yes', CASE WHEN count(*) > 0 THEN 'yes' ELSE 'no' END FROM answers;
INSERT INTO checks SELECT 'reader wrong answers', '0', count(*)::VARCHAR FROM answers
WHERE NOT (balance = 1000000 AND accounts = 1000 AND mismatched = 0 AND torn = 0 AND corrupt = 0
           AND batches = ${RD_BATCHES});
EOF
	bank_verify "${RD_DB}" "${RD_ACKS}" full
	echo "ATTACH 'duckdb_objfs://${RD_DB}' AS obj (READ_ONLY);"
	echo "INSERT INTO checks SELECT 'filler batches after churn', '${RD_BATCHES}', count(DISTINCT batch)::VARCHAR FROM obj.filler;"
	echo "DETACH obj;"
}
run_phase readers_verify < <(readers_verify_sql)

# ---------------------------------------------------------------- 6. independent_dbs
# WRITERS processes each write their own database under the same root at the same time, then are
# killed -9. Separate databases must not fence or disturb each other.
IN_DIR="${WORK_DIR}/independent"
mkdir -p "${IN_DIR}"
IN_PIDS=()
for n in $(seq 1 "${WRITERS}"); do
	{
		preamble
		cat <<EOF
ATTACH 'duckdb_objfs://stress_par_${n}.db' AS obj;
CREATE OR REPLACE TABLE obj.t AS SELECT range AS i, md5('${n}-' || range) AS payload FROM range(200000);
CHECKPOINT obj;
INSERT INTO obj.t SELECT range, md5('${n}-' || range) FROM range(200000, 201000);
COPY (SELECT 1) TO '${IN_DIR}/mark_${n}.csv';
${BUSY}
EOF
	} >"${IN_DIR}/${n}.sql"
	launch "${IN_DIR}/${n}.sql" "${IN_DIR}/${n}.log"
	IN_PIDS+=("${LAUNCHED}")
done
for n in $(seq 1 "${WRITERS}"); do wait_for "${IN_DIR}/mark_${n}.csv" 300 || true; done
reap "${IN_PIDS[@]}"
for n in $(seq 1 "${WRITERS}"); do
	if [[ ! -f "${IN_DIR}/mark_${n}.csv" ]]; then
		fail "independent_${n}" "$(classify "${IN_DIR}/${n}.log")"
	fi
done
independent_verify_sql() {
	for n in $(seq 1 "${WRITERS}"); do
		echo "ATTACH 'duckdb_objfs://stress_par_${n}.db' AS p${n};"
	done
	for n in $(seq 1 "${WRITERS}"); do
		cat <<EOF
INSERT INTO checks SELECT 'db ${n}: rows | corrupted', '201000 | 0',
    count(*)::VARCHAR || ' | ' || count(*) FILTER (WHERE payload IS DISTINCT FROM md5('${n}-' || i))::VARCHAR FROM p${n}.t;
CHECKPOINT p${n};
EOF
	done
}
run_phase independent_verify < <(independent_verify_sql)

# ---------------------------------------------------------------- 7. ducklake_writers
# WRITERS processes insert 1000-row batches into one DuckLake table (catalog in duckdb_objfs, data in
# the lakehouse Tables/), relaunched when fenced. Acked batches must all be there, whole and exactly
# once; snapshots must stay ordered; compaction and expiry must keep the rows.
DL_CATALOG="ducklake:duckdb_objfs://stress_catalog.ducklake"
DL_DATA="abfss://${WORKSPACE}@onelake.dfs.fabric.microsoft.com/${LAKEHOUSE}/Tables/"
DL_ATTACH="ATTACH '${DL_CATALOG}' AS lake (DATA_PATH '${DL_DATA}', DATA_INLINING_ROW_LIMIT 0);"
DL_DIR="${WORK_DIR}/ducklake"
DL_ACKS="${DL_DIR}/acks"
new_acks_dir "${DL_ACKS}"
run_phase ducklake_setup ducklake <<EOF
${DL_ATTACH}
CREATE SCHEMA IF NOT EXISTS lake.ducklake_stress;
CREATE OR REPLACE TABLE lake.ducklake_stress.events (writer VARCHAR, incarnation INTEGER, seq INTEGER, i BIGINT, payload VARCHAR);
INSERT INTO checks SELECT 'events empty', '0', count(*)::VARCHAR FROM lake.ducklake_stress.events;
DETACH lake;
EOF
declare -A DL_PID=() DL_INC=()
dl_launch() {
	local w=$1 s
	DL_INC[${w}]=$((${DL_INC[${w}]:-0} + 1))
	local inc=${DL_INC[${w}]}
	{
		preamble ducklake
		echo "${DL_ATTACH}"
		for s in $(seq 1 1000); do
			echo "INSERT INTO lake.ducklake_stress.events SELECT 'w${w}', ${inc}, ${s}, range, md5('w${w}-${inc}-${s}-' || range) FROM range(1000);"
			ack_sql "${DL_ACKS}" "w${w}" "${inc}" "${s}"
		done
	} >"${DL_DIR}/w${w}-${inc}.sql"
	launch "${DL_DIR}/w${w}-${inc}.sql" "${DL_DIR}/w${w}-${inc}.log"
	DL_PID[${w}]=${LAUNCHED}
}
for w in $(seq 1 "${WRITERS}"); do dl_launch "${w}"; done
end=$((SECONDS + DURATION))
while ((SECONDS < end)); do
	for w in $(seq 1 "${WRITERS}"); do
		if ! kill -0 "${DL_PID[${w}]}" 2>/dev/null; then
			reap "${DL_PID[${w}]}"
			sleep "0.$((RANDOM % 10))"
			dl_launch "${w}"
		fi
	done
	sleep 0.5
done
for w in $(seq 1 "${WRITERS}"); do reap "${DL_PID[${w}]}"; done
classify_writers "${DL_DIR}" ducklake
info "ducklake: $(ack_count "${DL_ACKS}") acked batches"
ducklake_verify_sql() {
	acks_table "${DL_ACKS}"
	cat <<EOF
${DL_ATTACH}
CREATE TEMP TABLE batches AS
SELECT writer, incarnation, seq, count(*) AS n, count(DISTINCT i) AS d FROM lake.ducklake_stress.events GROUP BY ALL;
INSERT INTO checks SELECT 'acked batches missing or partial', '0', count(*)::VARCHAR
FROM acks LEFT JOIN batches USING (writer, incarnation, seq) WHERE coalesce(n, 0) <> 1000;
INSERT INTO checks SELECT 'batches not exactly 1000 distinct rows', '0', count(*)::VARCHAR FROM batches WHERE n <> 1000 OR d <> 1000;
EOF
	prefix_check "commits not a gapless prefix of acks" batches
	cat <<EOF
INSERT INTO checks SELECT 'corrupted payloads (full scan)', '0', count(*)::VARCHAR FROM lake.ducklake_stress.events
WHERE payload IS DISTINCT FROM md5(writer || '-' || incarnation || '-' || seq || '-' || i);
INSERT INTO checks SELECT 'snapshot ids unique and time-ordered', '0', count(*)::VARCHAR FROM (
    SELECT snapshot_id, snapshot_time, lag(snapshot_id) OVER w AS prev_id, lag(snapshot_time) OVER w AS prev_time
    FROM ducklake_snapshots('lake') WINDOW w AS (ORDER BY snapshot_id))
WHERE snapshot_id = prev_id OR snapshot_time < prev_time;
SET VARIABLE rows_before = (SELECT count(*) FROM lake.ducklake_stress.events);
CALL ducklake_merge_adjacent_files('lake');
CALL ducklake_expire_snapshots('lake', older_than => now());
CALL ducklake_cleanup_old_files('lake', cleanup_all => true);
INSERT INTO checks SELECT 'rows after compaction + expiry', getvariable('rows_before')::VARCHAR, count(*)::VARCHAR
FROM lake.ducklake_stress.events;
INSERT INTO checks SELECT 'corrupted payloads after compaction', '0', count(*)::VARCHAR FROM lake.ducklake_stress.events
WHERE payload IS DISTINCT FROM md5(writer || '-' || incarnation || '-' || seq || '-' || i);
DETACH lake;
EOF
}
run_phase ducklake_verify ducklake < <(ducklake_verify_sql)

# ---------------------------------------------------------------- 8. big_sync
# One transaction of ~BIG_MB of incompressible strings, then a checkpoint. The file handle stages every
# chunk in memory and syncs them as one SlateDB write batch, here above the 1 GiB unflushed limit.
BIG_ROWS=$((BIG_MB * 1048576 / 40))
TIME_BIN=""
if [[ -x /usr/bin/time ]]; then TIME_BIN="/usr/bin/time -v"; fi
PHASE_WRAP="${TIME_BIN} timeout -s KILL 1200" run_phase big_sync_write <<EOF
ATTACH 'duckdb_objfs://stress_big.db' AS obj;
CREATE OR REPLACE TABLE obj.big AS
SELECT range AS i, md5(range::VARCHAR) || md5((range * 7)::VARCHAR) AS payload FROM range(${BIG_ROWS});
CHECKPOINT obj;
INSERT INTO checks SELECT 'database at least 1 GiB', 'yes',
    CASE WHEN total_blocks * block_size >= 1073741824 THEN 'yes' ELSE (total_blocks * block_size // 1048576)::VARCHAR || ' MiB' END
FROM pragma_database_size() WHERE database_name = 'obj';
DETACH obj;
EOF
grep -a -E 'Maximum resident set size|Elapsed \(wall' "${WORK_DIR}/big_sync_write.log" | sed 's/^\s*/    /' | tee -a "${SUMMARY}" || true
PHASE_WRAP="timeout -s KILL 1200" run_phase big_sync_verify <<EOF
ATTACH 'duckdb_objfs://stress_big.db' AS obj (READ_ONLY);
INSERT INTO checks SELECT 'rows | corrupted', '${BIG_ROWS} | 0',
    count(*)::VARCHAR || ' | ' || count(*) FILTER (WHERE payload IS DISTINCT FROM md5(i::VARCHAR) || md5((i * 7)::VARCHAR))::VARCHAR
FROM obj.big;
DETACH obj;
EOF

# ---------------------------------------------------------------- 9. bad_token (informational)
# A wrong token must fail fast rather than retry forever.
BT_SQL="${WORK_DIR}/bad_token.sql"
preamble | sed "s/getenv('ONELAKE_TOKEN')/'not-a-valid-token'/" >"${BT_SQL}"
echo "ATTACH 'duckdb_objfs://${CR_DB}' AS obj (READ_ONLY);" >>"${BT_SQL}"
start=${SECONDS} status=0
timeout -s KILL 120 "${DUCKDB_BIN}" -bail -f "${BT_SQL}" >"${WORK_DIR}/bad_token.log" 2>&1 || status=$?
case ${status} in
137) info "bad_token: **hung**, killed after 120 s (informational)" ;;
0) info "bad_token: **ATTACH succeeded with an invalid token** (informational)" ;;
*) info "bad_token: failed in $((SECONDS - start)) s: $(grep -a -i -m1 error "${WORK_DIR}/bad_token.log" | cut -c1-200 || true)" ;;
esac

echo
if [[ ${#FAILED[@]} -gt 0 ]]; then
	echo "=== failed phases: ${FAILED[*]}" | tee -a "${SUMMARY}"
	exit 1
fi
echo "=== OneLake stress passed" | tee -a "${SUMMARY}"
