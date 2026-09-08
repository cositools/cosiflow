#!/bin/bash
set -euo pipefail

cd /home/gamma

log() {
    printf '\033[32m%s\033[0m\n' "$1"
}

error() {
    printf '\033[31m%s\033[0m\n' "$1" >&2
    exit 1
}

configure_runtime() {
    python /home/gamma/validate_runtime_secrets.py
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$(
        python /home/gamma/validate_runtime_secrets.py --sqlalchemy-dsn
    )"
    export AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp

    if [ -n "${ALERT_EMAIL_SENDER:-}" ]; then
        export AIRFLOW__SMTP__SMTP_MAIL_FROM="$ALERT_EMAIL_SENDER"
    fi

    export MAILHOG_WEBUI_URL="http://${HOST_IP:-127.0.0.1}:${MAILHOG_WEBUI_PORT:-8025}"
    export COSIFLOW_HOME_URL="http://${HOST_IP:-127.0.0.1}:${AIRFLOW_WEBUI_PORT:-8080}/heasarcbrowser"
    mkdir -p "${COSI_DATA_DIR:?COSI_DATA_DIR is required}"/{obs,transient,tdrss,maps,source}
}

admin_exists_exactly() {
    airflow users list --output json | python -c '
import json, sys
username = sys.argv[1]
rows = json.load(sys.stdin)
raise SystemExit(0 if any(str(row.get("username", "")) == username for row in rows) else 1)
' "$AIRFLOW_ADMIN_USERNAME"
}

run_init() {
    configure_runtime
    log "Validated runtime secrets before database migration."
    airflow db migrate
    python /home/gamma/reencrypt_airflow_secrets.py
    airflow sync-perm

    if admin_exists_exactly; then
        airflow users reset-password \
            --username "$AIRFLOW_ADMIN_USERNAME" \
            --password "$AIRFLOW_ADMIN_PASSWORD"
        log "Airflow administrator password synchronized with the external secret."
    else
        airflow users create \
            --username "$AIRFLOW_ADMIN_USERNAME" \
            --firstname COSI \
            --lastname Admin \
            --role Admin \
            --email "$AIRFLOW_ADMIN_EMAIL" \
            --password "$AIRFLOW_ADMIN_PASSWORD"
        log "Airflow administrator created."
    fi

    python /home/gamma/configure_rbac.py
    log "COSIflow roles and permissions reconciled and verified."
}

run_runtime() {
    configure_runtime
    log "Starting Airflow runtime after successful init."
    airflow webserver --port 8080 &
    webserver_pid=$!
    trap 'kill -TERM "$webserver_pid" 2>/dev/null || true' EXIT INT TERM
    airflow scheduler
}

case "${1:-}" in
    init)
        run_init
        ;;
    runtime)
        run_runtime
        ;;
    *)
        error "Usage: entrypoint-airflow.sh init|runtime"
        ;;
esac
