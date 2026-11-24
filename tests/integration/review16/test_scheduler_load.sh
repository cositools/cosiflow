#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../.." && pwd)"
compose_file="$repo_root/tests/integration/docker-compose.review16.yml"
project="cosiflow-review16-$PPID-$$"
waiting_run_id="review16-waiting-$PPID-$$"
ready_run_id="review16-ready-$PPID-$$"
scheduler_log="$repo_root/tests/integration/review16/scheduler-load.log"

compose() {
    docker compose -p "$project" -f "$compose_file" "$@"
}

psql_scalar() {
    compose exec -T postgres psql \
        -X -v ON_ERROR_STOP=1 -U review16 -d review16 -Atc "$1"
}

airflow_cli() {
    compose exec -T airflow-scheduler airflow "$@"
}

cleanup() {
    status=$?
    if (( status != 0 )); then
        compose logs --no-color airflow-scheduler postgres >"$scheduler_log" 2>&1 || true
        printf 'Review 16 scheduler load test failed; logs: %s\n' "$scheduler_log" >&2
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
    dag_id=$1
    deadline=$(( $(date +%s) + 60 ))
    while (( $(date +%s) < deadline )); do
        if airflow_cli dags list --output json 2>/dev/null | grep -q "\"dag_id\": \"$dag_id\""; then
            return 0
        fi
        sleep 1
    done
    printf 'DAG was not parsed within 60 seconds: %s\n' "$dag_id" >&2
    return 1
}

compose up -d --wait postgres
compose run --rm airflow-init
compose up -d airflow-scheduler

wait_for_dag review16_waiting_sensors
wait_for_dag review16_ready_work

parallelism="$(airflow_cli config get-value core parallelism | tail -n 1)"
pool_slots="$(psql_scalar "SELECT slots FROM slot_pool WHERE pool='default_pool';")"
if [[ "$parallelism" != "2" || "$pool_slots" != "2" ]]; then
    printf 'Invalid capacity: parallelism=%s default_pool_slots=%s\n' \
        "$parallelism" "$pool_slots" >&2
    exit 1
fi

compose exec -T airflow-scheduler rm -f /tmp/review16-ready-work.success
airflow_cli dags trigger --run-id "$waiting_run_id" review16_waiting_sensors >/dev/null

sensor_count_query="SELECT count(*) FROM task_instance WHERE dag_id='review16_waiting_sensors' AND run_id='$waiting_run_id';"
wait_until "six sensor task instances" 30 "$sensor_count_query" "6"

sensor_reschedule_query="SELECT count(*) FROM task_instance WHERE dag_id='review16_waiting_sensors' AND run_id='$waiting_run_id' AND state='up_for_reschedule';"
wait_until "all sensors to release their worker slots" 45 "$sensor_reschedule_query" "6"

for _ in 1 2 3; do
    running="$(psql_scalar "SELECT count(*) FROM task_instance WHERE dag_id='review16_waiting_sensors' AND run_id='$waiting_run_id' AND state='running';")"
    if [[ "$running" != "0" ]]; then
        printf 'A waiting sensor remained running after the reschedule barrier: %s\n' "$running" >&2
        exit 1
    fi
    sleep 2
done

airflow_cli dags trigger --run-id "$ready_run_id" review16_ready_work >/dev/null
ready_success_query="SELECT count(*) FROM task_instance WHERE dag_id='review16_ready_work' AND run_id='$ready_run_id' AND task_id='create_ready_sentinel' AND state='success';"
wait_until "independent ready work to succeed" 30 "$ready_success_query" "1"

waiting_after_ready="$(psql_scalar "$sensor_reschedule_query")"
if (( waiting_after_ready <= pool_slots )); then
    printf 'Expected waiting sensors to exceed worker slots; waiting=%s slots=%s\n' \
        "$waiting_after_ready" "$pool_slots" >&2
    exit 1
fi

if ! compose exec -T airflow-scheduler test -f /tmp/review16-ready-work.success; then
    printf 'Ready task succeeded without creating its sentinel file\n' >&2
    exit 1
fi

printf 'PASS: parallelism=%s pool_slots=%s waiting_sensors=%s ready_task=success\n' \
    "$parallelism" "$pool_slots" "$waiting_after_ready"
