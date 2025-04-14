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

# Start Airflow
airflow standalone