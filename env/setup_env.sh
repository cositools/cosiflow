#!/bin/bash

ENV_FILE="/shared_dir/env/.env"

# Check for --reset flag
FORCE_RESET=false
if [ "$1" == "--reset" ]; then
  FORCE_RESET=true
fi

# If .env exists and not resetting, skip setup
if [ -f "$ENV_FILE" ] && [ "$FORCE_RESET" == false ]; then
  echo "✔️  .env file already exists. No setup needed."
  echo "ℹ️  Run with './setup_env.sh --reset' to overwrite it."
  exit 0
fi

echo "🔧 Setting up admin credentials for Airflow..."

read -s -p "Enter Airflow admin password: " PASSWORD
echo ""
read -s -p "Confirm password: " CONFIRM
echo ""

if [ "$PASSWORD" != "$CONFIRM" ]; then
  echo "❌ Passwords do not match. Exiting."
  exit 1
fi

cat <<EOF > "$ENV_FILE"
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_EMAIL=admin@localhost
AIRFLOW_ADMIN_PASSWORD=$PASSWORD
EOF

# chmod 600 "$ENV_FILE"
echo "✅ .env file created at $ENV_FILE"