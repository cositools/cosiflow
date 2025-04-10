#!/bin/bash

# Load secrets from .env file (e.g., AIRFLOW_ADMIN_PASSWORD)
set -o allexport
source /home/gamma/.env
set +o allexport

# Move to working directory
cd /home/gamma

# Activate the conda environment
source activate gamma

# Add local user bin directory to PATH
export PATH="$PATH:~/.local/bin"

# Initialize Airflow database
airflow db init

# Create an admin user if it doesn't already exist
if ! airflow users list | grep -q admin; then
  airflow users create \
    --username admin \
    --firstname COSI \
    --lastname Admin \
    --role Admin \
    --email cosiadmin@localhost \
    --password "$AIRFLOW_ADMIN_PASSWORD"
fi

# Run any initial setup tasks (e.g., downloading data for the DAGs)
# python /shared_dir/pipeline/initialize_pipeline.py

# Start Airflow webserver and scheduler
airflow standalone
