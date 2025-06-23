#!/bin/bash
set -euo pipefail

cd /home/gamma

ENV_FILE="/shared_dir/env/.env"

if [ ! -f "$ENV_FILE" ]; then
  echo "❌ Missing .env file at $ENV_FILE"
  echo "👉 Please create the file with the following structure:"
  echo ""
  echo "AIRFLOW_ADMIN_USERNAME=admin"
  echo "AIRFLOW_ADMIN_EMAIL=admin@localhost"
  echo "AIRFLOW_ADMIN_PASSWORD=yourpassword"
  echo ""
  echo "ALERT_SMTP_SERVER=mailhog"
  echo "ALERT_SMTP_PORT=1025"
  echo "ALERT_EMAIL_SENDER=donotreply@cosiflow.alert.errors.it"
  exit 1
fi

# Load environment variables
set -o allexport
source "$ENV_FILE"
set +o allexport

# Check required variables
if [ -z "${AIRFLOW_ADMIN_USERNAME:-}" ] || [ -z "${AIRFLOW_ADMIN_EMAIL:-}" ] || [ -z "${AIRFLOW_ADMIN_PASSWORD:-}" ]; then
  echo "❌ Missing one or more required environment variables in $ENV_FILE"
  exit 1
fi

# Export SMTP settings for Airflow if present
if [ -n "${ALERT_SMTP_SERVER:-}" ]; then
  export AIRFLOW__SMTP__SMTP_HOST="$ALERT_SMTP_SERVER"
fi

if [ -n "${ALERT_SMTP_PORT:-}" ]; then
  export AIRFLOW__SMTP__SMTP_PORT="$ALERT_SMTP_PORT"
fi

if [ -n "${ALERT_EMAIL_SENDER:-}" ]; then
  export AIRFLOW__SMTP__SMTP_MAIL_FROM="$ALERT_EMAIL_SENDER"
fi

# Always use this email backend
export AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp


# Activate conda environment
source activate gamma
export PATH="$PATH:~/.local/bin"
echo "✅ Environment activated."

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
  echo "✅ Admin user created."
else
  echo "ℹ️ Admin user already exists. Skipping creation."
fi

# Start webserver (in background) and scheduler
airflow webserver --port 8080 &
airflow scheduler
