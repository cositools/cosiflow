#!/usr/bin/env bash
set -euo pipefail
umask 077

# Point kubectl at the kubeconfig copied from cosi-1 in the previous step.
# K3S_CONFIG_FILE=/dev/null prevents the k3s binary from overriding KUBECONFIG.
export KUBECONFIG="$HOME/.kube/config" K3S_CONFIG_FILE=/dev/null
cd "$HOME/cosiflow"
site_dir="$HOME/.config/cosiflow-kubernetes"
site="$site_dir/site.yaml"
# /srv/cosiflow-shared is exported from cosi-1 via NFS and mounted on cosi-2 and cosi-3.
shared=/srv/cosiflow-shared
ns=cosiflow-distributed-test
image=cosiflow-kubernetes:retest-20261110
mkdir -p "$site_dir"

case "${1:?Expected prepare or deploy}" in
  prepare)
    # Install the NFS server (runs on cosi-1) and the NFS client (all nodes).
    sudo apt-get install -y nfs-kernel-server nfs-common

    # Create the shared directory and hand ownership to the ubuntu user so that
    # Airflow (also running as ubuntu) can write directly without root.
    sudo install -d -o "$(id -u)" -g "$(id -g)" -m 0755 "$shared"
    sudo install -d -m 0755 /etc/exports.d

    # Export the shared directory to the three cluster nodes using their static IPs.
    # root_squash: root on a client maps to nobody on the server (security default).
    sudo tee /etc/exports.d/cosiflow-retest.exports >/dev/null <<EOF
$shared 192.168.89.11(rw,sync,no_subtree_check,root_squash) 192.168.89.12(rw,sync,no_subtree_check,root_squash) 192.168.89.13(rw,sync,no_subtree_check,root_squash)
EOF
    # Start and enable the NFS server, then refresh the export table.
    sudo systemctl enable --now nfs-server
    sudo exportfs -ra
    sudo exportfs -v

    # Set up the Python virtualenv and install deployment dependencies (same as Compose).
    python3 -m venv "$HOME/.venvs/cosiflow-deploy"
    source "$HOME/.venvs/cosiflow-deploy/bin/activate"
    python -m pip install -r deployment/requirements.txt

    # Generate site.yaml from the Kubernetes example if one does not exist yet.
    # Also patches infrastructure.json with the NFS server IP and a longer PostgreSQL
    # readiness-probe timeout (TCG emulation makes Postgres start slowly).
    # The patched version is saved as infrastructure-retest.json and deployed below.
    python - "$site" "$shared" "$ns" "$image" "$(kubectl config current-context)" <<'PY'
import json
import os
from pathlib import Path
import sys
import yaml

site, shared, namespace, image, context = sys.argv[1:]
site = Path(site)
if not site.exists():
    config = yaml.safe_load(Path('deployment/examples/site.kubernetes.yaml').read_text())
    config['deployment']['name'] = 'cosiflow-k3s-retest'
    config['runtime'].update(uid=os.getuid(), gid=os.getgid())
    config['application'].update(image=image, public_url='http://127.0.0.1:18082')
    k8s = config['kubernetes']
    k8s.update(context=context, namespace=namespace, shared_path=shared)
    # Pin the control-plane components (scheduler, webserver, triggerer, flower) to cosi-2.
    for component in k8s['components'].values():
        component['node_selector']['kubernetes.io/hostname'] = 'cosi-2'
    # Pin task-runner workers to cosi-1 so science tasks land on the NFS server node.
    k8s['workers']['node_selector']['kubernetes.io/hostname'] = 'cosi-1'
    for profile in config['execution']['dag_profiles'].values():
        profile['primary_node'] = 'cosi-1'
    site.write_text(yaml.safe_dump(config, sort_keys=False))

# Patch the bundled infrastructure.json:
# 1. Set the NFS server IP to 192.168.89.11 (cosi-1 static cluster address).
# 2. Raise the PostgreSQL readiness-probe timeout to 10 s (default 1 s is too tight for TCG).
infra = json.loads((site.parent / 'infrastructure.json').read_text())
volume = next(item for item in infra['items'] if item['kind'] == 'PersistentVolume')
volume['spec']['nfs'] = {'server': '192.168.89.11', 'path': shared}
postgres = next(item for item in infra['items']
                if item['kind'] == 'Deployment' and item['metadata']['name'] == 'postgres')
postgres['spec']['template']['spec']['containers'][0]['readinessProbe']['timeoutSeconds'] = 10
(site.parent / 'infrastructure-retest.json').write_text(json.dumps(infra, indent=2) + '\n')
PY

    # Bootstrap secrets into runtime.env (same logic as the Compose deployment).
    if [[ ! -e "$site_dir/runtime.env" ]]; then
      cp env/.env.example "$site_dir/runtime.env"
    fi
    python env/bootstrap-secrets.py --env-file "$site_dir/runtime.env"

    # Create the Kubernetes namespace (idempotent: dry-run + apply).
    kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -

    # Create a Kubernetes secret from the generated runtime.env so pods can consume it
    # as environment variables without the values appearing in the manifest files.
    if ! kubectl -n "$ns" get secret cosiflow-runtime >/dev/null 2>&1; then
      (
        source "$site_dir/runtime.env"
        for key in POSTGRES_PASSWORD AIRFLOW__CORE__FERNET_KEY AIRFLOW__WEBSERVER__SECRET_KEY \
          AIRFLOW__CORE__INTERNAL_API_SECRET_KEY AIRFLOW_ADMIN_PASSWORD GCN_DB_PASSWORD; do
          printf '%s=%s\n' "$key" "${!key}"
        done
        printf 'AIRFLOW_ADMIN_USERNAME=%s\n' "${AIRFLOW_ADMIN_USERNAME:-admin}"
        printf 'AIRFLOW_ADMIN_EMAIL=%s\n' "${AIRFLOW_ADMIN_EMAIL:-admin@localhost}"
        printf 'MYSQL_ROOT_PASSWORD=%s\n' "$GCN_MYSQL_ROOT_PASSWORD"
      ) | kubectl -n "$ns" create secret generic cosiflow-runtime --from-env-file=/dev/stdin
    fi

    # Apply the patched infrastructure manifest (PersistentVolume, Postgres, GCN MySQL, Mailhog).
    kubectl apply -f "$site_dir/infrastructure-retest.json"

    # Wait for all three infrastructure deployments to become ready before proceeding.
    for app in postgres gcn-mysql mailhog; do
      kubectl -n "$ns" rollout status "deployment/$app" --timeout=600s
    done
    kubectl -n "$ns" get pvc,pods -o wide

    # Validate the site.yaml before attempting to build anything.
    ./deployment/cosiflowctl validate --config "$site"

    # Prepare the shared runtime workspace.
    # umask 022: NFS root_squash means files must be group/world-readable for kubelet to traverse them.
    (umask 022; ./deployment/cosiflowctl prepare --config "$site")

    # Verify that the two base images built in the Compose phase are present locally.
    docker image inspect cosiflow-airflow:site cosiflow-gcn-client:prototype >/dev/null

    # Build an AMD64 Kubernetes-specific image that layers on top of the site image.
    docker build -t "$image" --build-arg COSIFLOW_BASE_IMAGE=cosiflow-airflow:site \
      -f deployment/kubernetes/Dockerfile .

    # Export the image to a tar archive and import it into the K3s image store.
    # K3s uses containerd, not Docker, so the image must be imported separately.
    docker save -o "$HOME/cosiflow-kubernetes.tar" "$image"
    sudo k3s ctr images import "$HOME/cosiflow-kubernetes.tar"
    ;;

  deploy)
    source "$HOME/.venvs/cosiflow-deploy/bin/activate"
    source "$site_dir/runtime.env"

    # Look up the Cluster IP of the GCN MySQL service so the Docker initialiser can reach it.
    GCN_DB_HOST=$(kubectl -n "$ns" get svc gcn-mysql -o jsonpath='{.spec.clusterIP}')
    export GCN_DB_HOST GCN_DB_PASSWORD
    # Run the GCN schema initialiser via Docker on cosi-1 (which shares the host network).
    # DRY_RUN=true: the GCN client will not actually connect to the GCN Kafka broker.
    export GCN_DB_NAME=gcn GCN_DB_USER=gcn_user GCN_CONSUMER_ENABLED=false
    export GCN_PRODUCER_ENABLED=false GCN_DRY_RUN=true
    docker run --rm --network host -e GCN_DB_HOST -e GCN_DB_PASSWORD -e GCN_DB_NAME \
      -e GCN_DB_USER -e GCN_CONSUMER_ENABLED -e GCN_PRODUCER_ENABLED -e GCN_DRY_RUN \
      cosiflow-gcn-client:prototype python -m app.main init-db

    # Deploy all Airflow pods into the cluster.
    # --install-modules: clone and install scientific pipeline modules (e.g. FasTP) into the shared volume.
    ./deployment/cosiflowctl deploy --config "$site" --install-modules

    # Tune scheduler performance for TCG emulation:
    # - PARSING_PROCESSES=1: single DAG-file parser thread (fewer processes = less memory on QEMU).
    # - DAGBAG_IMPORT_TIMEOUT=120: allow up to 2 min to import a DAG file (TCG is slow).
    # - DAG_FILE_PROCESSOR_TIMEOUT=180: allow up to 3 min for the file-processor subprocess.
    kubectl -n "$ns" set env deployment/cosiflow-scheduler \
      AIRFLOW__SCHEDULER__PARSING_PROCESSES=1 \
      AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120 \
      AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=180

    # Wait for the scheduler pod to restart with the new env vars before returning.
    kubectl -n "$ns" rollout status deployment/cosiflow-scheduler --timeout=600s
    ./deployment/cosiflowctl status --config "$site"
    ;;
  *) exit 2 ;;
esac
