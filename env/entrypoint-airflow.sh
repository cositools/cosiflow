#!/bin/bash
#set -euo pipefail

cd /home/gamma

# define a macro `log` for printing messages with color green
log() {
    echo -e "\033[32m$1\033[0m"
}

# define a macro `error` for printing messages with color red
error() {
    echo -e "\033[31m$1\033[0m"
    exit 1
}

# define a macro `warning` for printing messages with color yellow
warning() {
    echo -e "\033[33m$1\033[0m"
}

if [ -n "${ALERT_EMAIL_SENDER:-}" ]; then
  export AIRFLOW__SMTP__SMTP_MAIL_FROM="$ALERT_EMAIL_SENDER"
fi

# Always use this email backend
export AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp

# Construct URLs from HOST_IP and ports (defined once in docker-compose.yaml)
# This allows changing HOST_IP in one place and having all URLs update automatically
HOST_IP="${HOST_IP:-localhost}"
MAILHOG_WEBUI_PORT="${MAILHOG_WEBUI_PORT:-8025}"
AIRFLOW_WEBUI_PORT="${AIRFLOW_WEBUI_PORT:-8080}"

# Build URLs dynamically
export MAILHOG_WEBUI_URL="http://${HOST_IP}:${MAILHOG_WEBUI_PORT}"
export COSIFLOW_HOME_URL="http://${HOST_IP}:${AIRFLOW_WEBUI_PORT}/heasarcbrowser"

log "URLs configured:"
echo "   MAILHOG_WEBUI_URL=${MAILHOG_WEBUI_URL}"
echo "   COSIFLOW_HOME_URL=${COSIFLOW_HOME_URL}"

# Export COSI directory structure environment variables if present
if [ -n "${COSI_DATA_DIR:-}" ]; then
  export COSI_DATA_DIR="$COSI_DATA_DIR"
fi

if [ -n "${COSI_OBS_DIR:-}" ]; then
  export COSI_OBS_DIR="$COSI_OBS_DIR"
fi

if [ -n "${COSI_TRANSIENT_DIR:-}" ]; then
  export COSI_TRANSIENT_DIR="$COSI_TRANSIENT_DIR"
fi

if [ -n "${COSI_TRIGGER_DIR:-}" ]; then
  export COSI_TRIGGER_DIR="$COSI_TRIGGER_DIR"
fi

if [ -n "${COSI_MAPS_DIR:-}" ]; then
  export COSI_MAPS_DIR="$COSI_MAPS_DIR"
fi

if [ -n "${COSI_SOURCE_DIR:-}" ]; then
  export COSI_SOURCE_DIR="$COSI_SOURCE_DIR"
fi

if [ -n "${COSI_INPUT_DIR:-}" ]; then
  export COSI_INPUT_DIR="$COSI_INPUT_DIR"
fi

if [ -n "${COSI_LOG_DIR:-}" ]; then
  export COSI_LOG_DIR="$COSI_LOG_DIR"
fi

# Create COSI directory structure if not present
mkdir -p $COSI_DATA_DIR/{obs,transient,tdrss,maps,source}

# Activate Python venv
if [ -f "/home/gamma/venv/bin/activate" ]; then
    source /home/gamma/venv/bin/activate
    log "Virtual environment activated."
else
    warning "venv activate script not found, assuming PATH is correct."
fi
# export PATH="$PATH:~/.local/bin" # Not needed with venv in PATH

# Initialize Airflow DB
airflow db init

# Create admin user if not present
if ! airflow users list | grep -q "$AIRFLOW_ADMIN_USERNAME"; then
  airflow users create \
    --username "$AIRFLOW_ADMIN_USERNAME" \
    --firstname COSI \
    --lastname Admin \
    --role Admin \
    --email "$AIRFLOW_ADMIN_EMAIL" \
    --password "$AIRFLOW_ADMIN_PASSWORD"
  log "Admin user created."
else
  warning "Admin user already exists. Skipping creation."
fi

# Start webserver (in background) and scheduler
airflow webserver --port 8080 &
airflow scheduler
