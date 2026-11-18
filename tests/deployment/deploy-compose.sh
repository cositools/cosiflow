#!/usr/bin/env bash
set -euo pipefail

# All configuration and secrets for this deployment live in ~/.config/cosiflow-compose/
# so they are separated from the source tree and not accidentally committed.
umask 077
cd "$HOME/cosiflow"
site_dir="$HOME/.config/cosiflow-compose"
mkdir -p "$site_dir"

# Create an isolated Python virtual environment for the deployment tooling.
# This avoids polluting the system Python with deployment-specific packages.
python3 -m venv "$HOME/.venvs/cosiflow-deploy"
source "$HOME/.venvs/cosiflow-deploy/bin/activate"

# Install the exact set of Python packages that cosiflowctl and its helpers need.
python -m pip install -r deployment/requirements.txt

# Generate site.yaml from the bundled Compose example if one does not already exist.
# The script stamps in the deployment name, the current user/group IDs (so bind-mounted
# volumes are owned by the right user), and the absolute paths for the runtime workspace
# and the secrets env file.
python - "$site_dir" <<'PY'
from pathlib import Path
import os
import sys
import yaml

directory = Path(sys.argv[1])
site = directory / 'site.yaml'
if not site.exists():
    config = yaml.safe_load(Path('deployment/examples/site.compose.yaml').read_text())
    config['deployment']['name'] = 'cosiflow-compose-retest'
    config['runtime'].update(uid=os.getuid(), gid=os.getgid())
    config['compose'].update(
        workspace=str(Path.home() / 'cosiflow-compose-runtime'),
        secrets_env_file=str(directory / 'runtime.env'),
    )
    site.write_text(yaml.safe_dump(config, sort_keys=False))
PY

# Copy the example env file as a starting point for secrets if none exists yet.
# bootstrap-secrets.py will then fill in any missing or placeholder values.
if [[ ! -e "$site_dir/runtime.env" ]]; then
  cp env/.env.example "$site_dir/runtime.env"
fi

# Generate random credentials for all secrets that are still at their placeholder values
# (database passwords, Fernet encryption keys, etc.).  Idempotent: existing secrets are
# left untouched so re-running this script does not rotate them.
python env/bootstrap-secrets.py --env-file "$site_dir/runtime.env"
chmod 600 "$site_dir/runtime.env"

site="$site_dir/site.yaml"

# Validate: check that site.yaml is well-formed and all required fields are present.
./deployment/cosiflowctl validate --config "$site"

# Prepare: create the runtime workspace directory tree and render all config templates.
./deployment/cosiflowctl prepare --config "$site"

# Build: build the custom Airflow Docker image with COSIflow plugins baked in.
./deployment/cosiflowctl build --config "$site"

# Deploy: start all Compose services (Postgres, Redis, Airflow webserver/scheduler/worker).
# --install-modules: clone and install scientific pipeline modules (e.g. FasTP) at boot.
./deployment/cosiflowctl deploy --config "$site" --install-modules

# Print a summary of the running services and their health status.
./deployment/cosiflowctl status --config "$site"
