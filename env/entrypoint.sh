#!/bin/bash

cd /home/gamma

ENV_FILE="/shared_dir/env/.env"

if [ ! -f "$ENV_FILE" ]; then
  echo "❌ Missing .env file: $ENV_FILE"
  echo "👉 You must run the setup container first:"
  echo "   docker compose run --rm setup"
  exit 1
fi

set -o allexport
source "$ENV_FILE"
set +o allexport

if [ -z "$AIRFLOW_ADMIN_USERNAME" ] || [ -z "$AIRFLOW_ADMIN_PASSWORD" ]; then
  echo "❌ Missing required environment variables in $ENV_FILE"
  exit 1
fi

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
  echo "ℹ️  Admin user already exists. Skipping creation."
fi

# Start Airflow
airflow standalone