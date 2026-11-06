#!/bin/bash
# KubernetesExecutor supplies "airflow tasks run ..." as this script's args.
set -euo pipefail
python /home/gamma/validate_runtime_secrets.py --scope runtime
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$(python /home/gamma/validate_runtime_secrets.py --scope runtime --sqlalchemy-dsn)"
exec "$@"
