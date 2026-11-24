#!/bin/bash
set -euo pipefail

COSI_RUNTIME_HOME="${COSI_RUNTIME_HOME:-/home/gamma}"
cd "$COSI_RUNTIME_HOME"

log() {
    printf '\033[32m%s\033[0m\n' "$1"
}

error() {
    printf '\033[31m%s\033[0m\n' "$1" >&2
    exit 1
}

configure_runtime() {
    local scope="$1"
    python "$COSI_RUNTIME_HOME/validate_runtime_secrets.py" --scope "$scope"
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$(
        python "$COSI_RUNTIME_HOME/validate_runtime_secrets.py" --scope "$scope" --sqlalchemy-dsn
    )"
    export AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp

    if [ -n "${ALERT_EMAIL_SENDER:-}" ]; then
        export AIRFLOW__SMTP__SMTP_MAIL_FROM="$ALERT_EMAIL_SENDER"
    fi

    if [ "$scope" = "runtime" ]; then
        export AIRFLOW__WEBSERVER__BASE_URL="${AIRFLOW_PUBLIC_BASE_URL:-http://${HOST_IP:-127.0.0.1}:${AIRFLOW_WEBUI_PORT}}"
        export MAILHOG_WEBUI_URL="http://${HOST_IP:-127.0.0.1}:${MAILHOG_WEBUI_PORT:-8025}"
        export COSIFLOW_HOME_URL="${AIRFLOW__WEBSERVER__BASE_URL%/}/heasarcbrowser"
    fi
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
    configure_runtime init
    log "Validated runtime secrets before database migration."
    airflow db migrate
    python "$COSI_RUNTIME_HOME/airflow/modules/cosidag_state.py" migrate \
        --sql "$COSI_RUNTIME_HOME/migrations/001_cosidag_state.sql"
    log "COSIDAG transactional state schema and legacy migration completed."
    python "$COSI_RUNTIME_HOME/airflow/modules/notification_subscriptions.py" migrate \
        --sql "$COSI_RUNTIME_HOME/migrations/002_notification_subscriptions.sql"
    log "Notification subscription schema migration completed."
    python "$COSI_RUNTIME_HOME/reencrypt_airflow_secrets.py"
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

    python "$COSI_RUNTIME_HOME/configure_rbac.py"
    log "COSIflow roles and permissions reconciled and verified."
    python "$COSI_RUNTIME_HOME/airflow/modules/notification_subscriptions.py" seed-admin
    log "Default administrator failure subscriptions reconciled."
}

run_webserver() {
    configure_runtime runtime
    log "Starting Airflow webserver after successful init."
    exec airflow webserver --port 8080
}

run_scheduler() {
    configure_runtime runtime
    log "Starting Airflow scheduler after successful init."
    exec airflow scheduler
}

case "${1:-}" in
    init)
        run_init
        ;;
    webserver)
        run_webserver
        ;;
    scheduler)
        run_scheduler
        ;;
    *)
        error "Usage: entrypoint-airflow.sh init|webserver|scheduler"
        ;;
esac
