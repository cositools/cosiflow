# Configurable COSIflow deployment

`cosiflowctl` manages application deployment from one validated, site-owned
`site.yaml`. Supported modes are Docker Compose / LocalExecutor and an existing
Kubernetes cluster with configurable LocalExecutor/KubernetesExecutor profiles.
Scientific DAGs and their source repositories are not modified.

This CLI is new. Its configuration, generation and orchestration are tested
offline; the earlier successful VM scientific runs used the manual procedure,
not this CLI. Repeat acceptance on a **new isolated instance** before adopting it.

## Layout and ownership

- `cosiflowctl`: prepare, build, render, plan, deploy, status and stop.
- `config.py`, `site.schema.json`: strict configuration contract, no unknown or duplicate keys.
- `bundle.py`: adapters for the existing Compose recipe and Kubernetes renderer.
- `examples/site.compose.yaml`, `examples/site.kubernetes.yaml`: public examples.
- `kubernetes/`: existing image, worker entrypoint and legacy JSON renderer remain supported.
- `requirements.txt`: Python CLI dependencies; Airflow is not needed on the administrator's host.

Commit these sources/examples/docs with COSIflow. Optionally package the same
files from a release tag as a release asset. Keep actual site configuration,
credentials, generated bundles, scientific data and logs outside the checkout.
Generated bundles contain local filesystem paths and should not be published.

## Install the command

Use Python 3.9+ and Git on the administration host. Run the commands from the
COSIflow checkout with the virtual environment active. The host needs access to
the configured module repositories. The build and module installer also need
access to their image and package sources.

```bash
python3 -m venv "$HOME/.venvs/cosiflow-deploy"
. "$HOME/.venvs/cosiflow-deploy/bin/activate"
python -m pip install -r deployment/requirements.txt
./deployment/cosiflowctl --help
```

For real Compose deployment run on the Docker host, normally the Linux node.
For real Kubernetes deployment run on a Linux administration host with kubectl,
the explicit selected context, and the shared PVC root mounted locally at
`kubernetes.shared_path`. This local path must expose the **same filesystem as
the PVC**, not an unrelated directory. Offline commands can run on a workstation.

## Test 1: offline configuration and rendering

Choose **one** example, copy it outside the checkout, and edit it:

```bash
mkdir -p "$HOME/.config/cosiflow-test"
cp -n deployment/examples/site.compose.yaml "$HOME/.config/cosiflow-test/site.yaml"
# For Kubernetes choose site.kubernetes.yaml instead, in a separate config directory.
```

Set the runtime UID/GID to match the target shared directories (`id -u`, `id -g`).
Set unique instance name, runtime path and ports; for Kubernetes set a dedicated
namespace/context/PVC/Secret and real node labels/image. `~` in filesystem paths
is expanded; other relative paths resolve relative to the YAML file.

```bash
./deployment/cosiflowctl validate --config "$HOME/.config/cosiflow-test/site.yaml"
./deployment/cosiflowctl plan \
  --config "$HOME/.config/cosiflow-test/site.yaml" \
  --output "$(mktemp -d)/bundle" --install-modules
```

Inspect the printed plan and generated files. Neither command reads credentials,
clones repositories or contacts Docker/Kubernetes. A bundle output directory is
created once and never overwritten. `render` writes the same files without
printing a plan. The YAML is parsed safely; duplicate/unknown keys, floating
module refs and paths escaping module repositories are rejected.

Automated tests (no live services modified):

```bash
python -m unittest discover -s tests -p 'test_deployment_config.py' -v
python -m unittest discover -s tests -p 'test_kubernetes_configuration.py' -v
```

## Test 2: isolated Compose installation

Run the Compose tutorial from the COSIflow checkout on the machine that runs
Docker. Before starting, check that you have:

- Python 3.9+, Git, and Docker Engine or Docker Desktop with Compose v2. Your
  account needs access to the running Docker daemon. Check with
  `docker info` and `docker compose version`.
- Network access to the module repositories and the image and package sources
  used by the builds and installers.
- A writable workspace with enough room for images, environments and
  scientific input and output. The DC4 VM rehearsal used a 100 GiB disk.
- Free local ports for Airflow and MailHog. On ARM, enable amd64 emulation for
  the bundled MailHog image.

The example publishes Airflow at 18081, MailHog UI at 18026 and SMTP at 11025,
so it can coexist with the existing 18080 rehearsal bridge. Change them if busy.
It has no fixed container names; volumes and databases are under the configured
workspace. This mode manages its bundled postgres:5432 and gcn-mysql:3306;
external databases belong to the Kubernetes mode in this first implementation.

Copy the Compose example to a dedicated configuration directory:

```bash
mkdir -p "$HOME/.config/cosiflow-compose"
cp -n deployment/examples/site.compose.yaml "$HOME/.config/cosiflow-compose/site.yaml"
```

Edit `site.yaml` before continuing. Set `runtime.uid` and `runtime.gid` to
match the workspace owner, choose a dedicated `compose.workspace`, and set
`compose.secrets_env_file` to `~/.config/cosiflow-compose/runtime.env`.
Keep `application.public_url` consistent with the URL you will use to open
Airflow. The secret bootstrap preserves existing values unless you request
rotation.

```bash
SITE="$HOME/.config/cosiflow-compose/site.yaml"
ENV_FILE="$HOME/.config/cosiflow-compose/runtime.env"
cp -n env/.env.example "$ENV_FILE"
python env/bootstrap-secrets.py --env-file "$ENV_FILE"
chmod 600 "$ENV_FILE"
./deployment/cosiflowctl validate --config "$SITE"
./deployment/cosiflowctl prepare --config "$SITE"
./deployment/cosiflowctl build --config "$SITE"
./deployment/cosiflowctl deploy --config "$SITE" --install-modules
./deployment/cosiflowctl status --config "$SITE"
```

`prepare` creates runtime directories and clones modules at their pinned 40-digit
Git SHAs. Existing dirty/wrong-revision checkouts cause an error; nothing is
reset, cleaned or pulled over them. Choose a fresh workspace for a new trial.
The module name must match the cloned directory and each module config is
relative to its own repository. Scientific environments use that module's own
configuration; ensure different modules do not overwrite incompatible venvs.

`deploy --install-modules` starts database/SMTP services, runs Airflow init,
initializes the local GCN schema, explicitly installs each module with the
existing hot-loader in container-local mode, sets the pool, then starts runtime.
Kafka consumer/producer are disabled. The GCN client is used only for init-db.

The initial environments must be installed before runtime. On later deploys
omit `--install-modules` to preserve them. That flag intentionally runs installers
and can recreate environments; pause DAGs and wait for idle tasks first. Without
it, the command checks that the configured module DAG links already exist.

With the example ports, open `http://127.0.0.1:18081/` on the Docker host.
If Docker runs in a remote Linux node, run
`ssh -N -L 18081:127.0.0.1:18081 USER@NODE` from your workstation and open the
same URL there. Check `/health` and follow the scientific acceptance steps below.

## Test 3: isolated Kubernetes installation

Run this tutorial once from the Linux administration host described above.
For an isolated acceptance run, use a dedicated namespace, storage and databases.
The site team prepares the cluster and the services listed below.

Required prerequisites:

1. Healthy existing cluster, explicit kubectl context, and a namespace created
   by the site. The deployment account needs to inspect the PVC and runtime
   Secret and manage the application resources in that namespace.
2. RWX PVC already Bound and its root mounted at `shared_path` on this host.
   Runtime directories must be writable by configured UID/GID.
3. PostgreSQL and compatible GCN MySQL endpoints reachable from pods, including
   the GCN outbox schema; run the client image's `python -m app.main init-db`
   against the site database beforehand. SMTP must also be reachable.
4. A pre-existing runtime Secret containing POSTGRES_PASSWORD,
   AIRFLOW__CORE__FERNET_KEY, AIRFLOW__WEBSERVER__SECRET_KEY,
   AIRFLOW__CORE__INTERNAL_API_SECRET_KEY, AIRFLOW_ADMIN_PASSWORD,
   AIRFLOW_ADMIN_USERNAME, AIRFLOW_ADMIN_EMAIL and GCN_DB_PASSWORD. Values never
   belong in site.yaml. Reuse it on subsequent deployments; do not regenerate
   credentials against existing database volumes.
5. Cluster image built for the nodes' architecture and available to every node
   allowed by component placement or task profiles. `build` only builds locally;
   publish/import the image separately using the site's registry or lab tools.
   Use a new versioned tag (or a prebuilt digest) for each changed image; reusing
   a mutable tag does not identify an upgrade. Skip build for a prebuilt digest.

Create the site configuration on the administration host:

```bash
mkdir -p "$HOME/.config/cosiflow-kubernetes"
cp -n deployment/examples/site.kubernetes.yaml "$HOME/.config/cosiflow-kubernetes/site.yaml"
```

Set the context, namespace, Secret, PVC, shared path, service endpoints and
image in `site.yaml`. Use the Kubernetes node names from `kubectl get nodes`
in the component and worker selectors and the DAG profiles. For the
three-machine rehearsal, `cosi-1` ran the FasTP workers and served NFS,
`cosi-2` ran scheduler, webserver and PostgreSQL, and `cosi-3` ran GCN MySQL.
The database endpoints refer to services that the site has already deployed.

Run these commands from the administration host. Replace `YOUR-CONTEXT` with
the context in your YAML and confirm the expected nodes are Ready.

```bash
SITE="$HOME/.config/cosiflow-kubernetes/site.yaml"
kubectl --context YOUR-CONTEXT get nodes -o wide
./deployment/cosiflowctl validate --config "$SITE"
./deployment/cosiflowctl plan --config "$SITE" \
  --output "$(mktemp -d)/bundle" --install-modules
./deployment/cosiflowctl prepare --config "$SITE"
./deployment/cosiflowctl build --config "$SITE"
```

Make the configured image available to eligible nodes through the site
registry or the lab image import procedure. Then deploy from the same shell:

```bash
./deployment/cosiflowctl deploy --config "$SITE" --install-modules
./deployment/cosiflowctl status --config "$SITE"
```

The sequence applies namespace-scoped RBAC/config, waits for the Airflow init
Job, optionally waits for each module installer Job, then applies runtime and
waits for its Deployments. Job names include a spec/config hash; runtime templates carry configuration
checksums so changed settings trigger a rollout. Completed matching
Jobs are retained and skipped. A failed Job is not automatically deleted: inspect
it, correct its cause, and deliberately delete only that failed Job before retry.
Updating a module ref requires a new clean prepared checkout; it is never reset
by the tool. Use separate versioned paths/instances for upgrades requiring a new
module source/environment until a dedicated upgrade procedure is implemented.

`shared_path` is an administration-host path. Pods mount the PVC with the same
internal paths as the original runtime; it is not a hostPath volume. Pipeline
sources stay read-only inside installer/runtime pods. `default_executor` sets
the first executor in Airflow's multi-executor list, so ordinary init_pipelines
also uses Kubernetes when configured that way. Per-DAG overrides use exact IDs.

Component selectors and worker selectors are independent. With a strict worker
selector compute-a, a DAG fallback to compute-b cannot work; remove/broaden the
worker selector if fallbacks are intended. Profile affinity and selectors must
agree. The example intentionally pins all workers to one science node, matching
our acceptance scenario; adapt the actual Kubernetes node names.

For the tested GeD input use at least the observed VM capacity (8 GiB RAM) and
the configured 6 GiB worker limit: the earlier 4 GiB pod was OOMKilled, while
observed usage reached about 4.69 GiB. This is evidence for that input, not a
universal sizing rule for all pipelines. Keep default_pool_slots: 1 until the
future FasTP shared-YAML implementation passes parallel validation.

For this UI example set `application.public_url` to
`http://127.0.0.1:18082` before deploying. Run the forwarding command from your
workstation with the selected context and leave the terminal open:

```bash
kubectl --context YOUR-CONTEXT -n YOUR-NAMESPACE \
  port-forward service/cosiflow-webserver 18082:8080
```

If your kubeconfig is on a remote node, run the same `kubectl` command there and
run `ssh -N -L 18082:127.0.0.1:18082 USER@NODE` on your workstation. Open
`http://127.0.0.1:18082/` from the workstation.

## Scientific acceptance after either deployment

1. Verify scheduler/metadatabase health, no DAG import errors and expected executor.
2. Trigger init_pipelines with pipeline_branch=both; wait for success and inspect
   the separate GeD/BGO products paths in create_products_dir XCom.
3. Trigger cosidag_GeD scoped to its event parent, level 1, auto_retrig false.
4. Trigger cosidag_BGO scoped to its own event parent with the same settings.
5. Verify all tasks, output files, finalization processed/succeeded and local GCN
   outbox. Record exact run IDs; no external GCN delivery is needed for acceptance.
6. In Kubernetes inspect pod annotations DAG/run/task/try and nodeName; all actual
   workers must follow the configured placement. EmptyOperator optimizations
   may complete without a pod. Retain pod evidence during the test.
7. Pause the three DAGs, wait for idle, then check that a deploy **without**
   --install-modules preserves credentials, module source/envs and completed Jobs.

The reference local and distributed reports live outside this repository under
Riccardo's `cosi/tmp/` workspace. They establish the manual baseline; the CLI
needs its own fresh runtime acceptance. Parallelism, mixed-family auto-routing,
numerical science comparison and HA/reaper/failover are separate criteria.

## Adding another pipeline and stopping

Add a new `module-pipelines` element with its repository, immutable SHA and
module-relative config, then add its exact COSIDAG IDs under execution.dag_profiles.
A module may contain multiple DAGs. No FasTP-specific installer is hard-coded in
the new bundle generator. Other four pipelines still need individual acceptance.

```bash
./deployment/cosiflowctl stop --config /path/to/site.yaml
```

Pause DAGs and wait first. Active task instances/workers cause stop/deploy to
refuse. This is a guard, not a lock against newly scheduled work: pausing is
required. Compose stop retains containers/data; Kubernetes stop scales only
scheduler/webserver to zero, retaining Jobs/PVC/Secret and site services. No
namespace, database or volume deletion is implemented. No automatic VM shutdown,
secret rotation, reset of repositories or release publication is performed.
