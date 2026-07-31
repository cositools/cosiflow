#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${SCRIPT_DIR}/../env/docker-compose.yaml}"
PROJECT_DIR="${PROJECT_DIR:-${SCRIPT_DIR}/../env}"
AIRFLOW_SERVICE="${AIRFLOW_SERVICE:-airflow}"
CONTAINER_SCRIPT="${CONTAINER_SCRIPT:-/shared_dir/test/performance_test.py}"
CONTAINER_CONFIG="${CONTAINER_CONFIG:-/shared_dir/test/performance_test.yaml}"

cd "${PROJECT_DIR}"

docker compose -f "${COMPOSE_FILE}" exec -T "${AIRFLOW_SERVICE}" \
  python "${CONTAINER_SCRIPT}" \
  --config "${CONTAINER_CONFIG}" \
  --inside-container \
  "$@"
