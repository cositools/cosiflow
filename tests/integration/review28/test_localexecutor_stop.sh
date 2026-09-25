#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../.." && pwd)"
compose_file="$repo_root/tests/integration/docker-compose.review28.yml"
project="cosiflow-review28-$PPID-$$"
owned_run_id="perf__review28__$PPID-$$"
manual_run_id="manual__review28__$PPID-$$"
scheduler_log="$repo_root/tests/integration/review28/localexecutor-stop.log"

compose() {
    docker compose -p "$project" -f "$compose_file" "$@"
}

psql_scalar() {
    compose exec -T postgres psql \
        -X -v ON_ERROR_STOP=1 -U review28 -d review28 -Atc "$1"
}

airflow_cli() {
    compose exec -T airflow-scheduler airflow "$@"
}

cleanup() {
    status=$?
    if (( status != 0 )); then
        compose logs --no-color airflow-scheduler postgres >"$scheduler_log" 2>&1 || true
        printf 'Review 28 LocalExecutor test failed; logs: %s\n' "$scheduler_log" >&2
    else
        rm -f "$scheduler_log"
    fi
    compose down -v --remove-orphans >/dev/null 2>&1 || true
    exit "$status"
}
trap cleanup EXIT INT TERM

wait_until() {
    description=$1
    timeout_seconds=$2
    query=$3
    expected=$4
    deadline=$(( $(date +%s) + timeout_seconds ))
    value=""
    while (( $(date +%s) < deadline )); do
        value="$(psql_scalar "$query")"
        if [[ "$value" == "$expected" ]]; then
            return 0
        fi
        sleep 1
    done
    printf 'Timed out waiting for %s; last value=%s expected=%s\n' \
        "$description" "${value:-<none>}" "$expected" >&2
    return 1
}

wait_for_dag() {
    deadline=$(( $(date +%s) + 60 ))
    while (( $(date +%s) < deadline )); do
        if airflow_cli dags list --output json 2>/dev/null | grep -q '"dag_id": "review28_long_running"'; then
            return 0
        fi
        sleep 1
    done
    printf 'Review 28 DAG was not parsed within 60 seconds\n' >&2
    return 1
}

heartbeat_size() {
    run_id=$1
    compose exec -T airflow-scheduler sh -c \
        'test -f "$1" && wc -c <"$1" || printf 0' \
        sh "/tmp/review28-${run_id}.heartbeat" | tr -d '[:space:]'
}

compose up -d --wait postgres
compose run --rm airflow-init
compose up -d airflow-scheduler
wait_for_dag

airflow_cli dags trigger --run-id "$owned_run_id" review28_long_running >/dev/null
airflow_cli dags trigger --run-id "$manual_run_id" review28_long_running >/dev/null

owned_running="SELECT count(*) FROM task_instance WHERE dag_id='review28_long_running' AND run_id='$owned_run_id' AND state='running';"
manual_running="SELECT count(*) FROM task_instance WHERE dag_id='review28_long_running' AND run_id='$manual_run_id' AND state='running';"
wait_until "owned task to run" 45 "$owned_running" "1"
wait_until "manual task to run" 45 "$manual_running" "1"

owned_before="$(heartbeat_size "$owned_run_id")"
manual_before="$(heartbeat_size "$manual_run_id")"
if (( owned_before == 0 || manual_before == 0 )); then
    printf 'Heartbeat files were not populated: owned=%s manual=%s\n' "$owned_before" "$manual_before" >&2
    exit 1
fi

compose exec -T airflow-scheduler \
    python /opt/review28/stop_run.py review28_long_running "$owned_run_id" --timeout 30

owned_failed="SELECT count(*) FROM task_instance WHERE dag_id='review28_long_running' AND run_id='$owned_run_id' AND state='failed';"
wait_until "owned task to be failed" 15 "$owned_failed" "1"

owned_stopped_size="$(heartbeat_size "$owned_run_id")"
manual_mid="$(heartbeat_size "$manual_run_id")"
sleep 3
owned_after="$(heartbeat_size "$owned_run_id")"
manual_after="$(heartbeat_size "$manual_run_id")"

if [[ "$owned_after" != "$owned_stopped_size" ]]; then
    printf 'Owned task continued writing after verified stop: before=%s after=%s\n' \
        "$owned_stopped_size" "$owned_after" >&2
    exit 1
fi
if (( manual_after <= manual_mid )); then
    printf 'Unrelated manual run stopped unexpectedly: before=%s after=%s\n' \
        "$manual_mid" "$manual_after" >&2
    exit 1
fi
manual_state="$(psql_scalar "SELECT state FROM task_instance WHERE dag_id='review28_long_running' AND run_id='$manual_run_id' AND task_id='write_heartbeat';")"
if [[ "$manual_state" != "running" ]]; then
    printf 'Unrelated manual run state changed unexpectedly: %s\n' "$manual_state" >&2
    exit 1
fi

printf 'PASS: owned_run=stopped manual_run=running owned_bytes=%s manual_bytes=%s\n' \
    "$owned_after" "$manual_after"
