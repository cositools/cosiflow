# Optional Kubernetes execution

This implementation ports the execution contract from laboratory commit
`76dba4321586792da71b45bb250c06d55d590a30`. The local Compose configuration
continues to use `LocalExecutor`. The cluster profile enables
`LocalExecutor,KubernetesExecutor` and can route existing COSIDAGs through a
site-owned JSON file. **FasTP DAG source files do not need to change.**

The code and offline manifest renderer are prepared for review. Scientific
LocalExecutor regression runs, image builds, installation and cluster
acceptance have not been performed as part of this port. Do not interpret
rendering a manifest as validating its deployment.

## Execution profiles without pipeline edits

`COSIFLOW_TASK_EXECUTION_FILE` is optional. When unset, COSIDAG preserves the
previous defaults and does not import the Kubernetes SDK. If set, it points
to a JSON object mapping exact DAG IDs to execution profiles:

```json
{
  "cosidag_BGO": {
    "executor": "KubernetesExecutor",
    "primary_node": "compute-a",
    "fallback_nodes": ["compute-b"],
    "preferred_node": "compute-a",
    "node_label": "kubernetes.io/hostname"
  },
  "cosidag_GeD": {
    "executor": "KubernetesExecutor",
    "primary_node": "compute-b",
    "fallback_nodes": ["compute-a"]
  }
}
```

Only COSIDAG consumes this setting. The ordinary Airflow `init_pipelines`
DAG keeps the environment's default executor. There is no wildcard routing.
An unreadable or malformed configured file fails parsing instead of silently
running the scientific task on a different executor.

New DAG authors may pass `task_execution={...}` directly to COSIDAG. An
explicit mapping overrides the file; `{}` explicitly opts out. Existing
`default_args` and `default_args_extra` continue to work. A profile overrides
the inherited `executor`, but individual task arguments still override DAG
defaults through Airflow. Other inherited settings are retained. Two competing
DAG-level `executor_config.pod_override` definitions raise a configuration
error instead of silently discarding affinity or volumes. Keep one owner of
the DAG-level pod override; per-task overrides require separate review.

Node membership is required affinity; the preferred node is soft affinity.
A running pod is not migrated by changing its preference. A retry may create
a new worker on an authorized fallback. `node_label` lets the site use its own
labels instead of literal hostnames. The profile also accepts `identity`,
`resources` (`requests`/`limits`), `priority_class_name` and `tolerations`.
Eviction tolerations are explicit site settings: the lab's ten-second values
are not silently applied to every deployment.

Pod metadata contains a literal DAG ID and a normalized pipeline label.
Node/pod names and IPs use the Downward API. Task/run correlation must use
Airflow's actual worker metadata or `AIRFLOW_CTX_*` inside the task process;
Jinja placeholders in pod overrides are not assumed to be rendered.

See the [Airflow 2.10.3 executor documentation](https://airflow.apache.org/docs/apache-airflow/2.10.3/core-concepts/executor/index.html#using-multiple-executors-concurrently).
Concurrent multi-executor configuration is experimental in this version and
remains part of the candidate's acceptance scope.

## FasTP and ExternalPythonOperator

The real module is `fast-transient-analysis-pipeline/`, not its laboratory
placeholder. Its existing `env/fta-pipe.config.yaml` declares:

| Environment | Python | Existing interpreter path |
| --- | --- | --- |
| `cosipy` | 3.12 | `/home/gamma/envs/cosipy/bin/python` |
| `bct` | 3.11 | `/home/gamma/envs/bct/bin/python` |
| `nimcosipy` | 3.12 | `/home/gamma/envs/nimcosipy/bin/python` |

The FasTP `env/Dockerfile` currently builds only the `cosipy` environment on
Python 3.12 slim. It is a scientific runtime image, not a complete Airflow
KubernetesExecutor worker, and does not provide all three environments.
Keep it unchanged. This profile extends the COSIflow base image (which has
Python 3.9, 3.11 and 3.12), and uses the existing module loader/configuration
to create the scientific environments on a shared **Linux** volume. The same
source, symlinks, interpreters and data paths are mounted in scheduler and
worker pods. The loader's container-local mode avoids needing a Docker socket
inside Kubernetes. It does not change the host-side Docker loader default.

The source directory on the shared volume must be available at
`modules-pool/fast-transient-analysis-pipeline`, including `env/`, `src/` and
configuration. All environments must be built for the workers' OS/CPU
architecture. Do not copy Mac virtualenvs or assume an environment built for
one CPU architecture works on another. Mixed-architecture workers need a
separate environment/image strategy before scheduling these tasks.

Before acceptance, check imports, serialization and actual interpreter paths
for BGO/GeD. In particular, validate the compatible serialization dependencies
in the managed environments; this port does not change their requirements.
The interpreter availability and serialization requirements are described by
[ExternalPythonOperator](https://airflow.apache.org/docs/apache-airflow/2.10.3/howto/operator/python.html#externalpythonoperator).

## Prepare a site bundle (offline)

Tori and Wing own cluster provisioning and site infrastructure. The supplied
renderer prepares **application** resources for an existing namespace, PVC,
PostgreSQL service and site-managed Secret. It never calls `kubectl`.

Copy `deployment/kubernetes/site.example.json` outside the repository and
replace the sample registry, namespace, node labels, endpoints and image.
Configure `pvc`, `secret`, `uid`/`gid`, image pull secrets/policy, component
node selector and worker/scheduler/webserver resources. No secret values
belong in this file. Then render into a new output directory:

```bash
python3 deployment/kubernetes/render.py /path/to/site.json --output /path/to/new-bundle
```

The output contains Kubernetes JSON Lists (JSON is accepted by Kubernetes):

| File | Content |
| --- | --- |
| `bootstrap.json` | Service account, namespace-scoped worker RBAC, runtime ConfigMap, pod template and DAG profiles |
| `init.json` | Explicit database migration/bootstrap Job |
| `module-install.json` | Explicit FasTP module/environment installation Job |
| `runtime.json` | One scheduler, one webserver, internal web Service |

The shared PVC must permit simultaneous read/write access from the relevant
nodes and provide these pre-created subdirectories, owned/accessed by the
configured UID/GID: `envs`, `modules-pool`, `dags`, `pipeline`, `data`, `logs`.
Stage the real FasTP source under `modules-pool` before module installation.
Code for COSIflow modules/callbacks/plugins is baked into the cluster image;
pipeline code and environments are shared through the PVC. Neither hostPath
nor Multipass mounts are used.

The Secret is referenced by name and is not generated. It must supply the
existing entrypoint's required credentials:
`POSTGRES_PASSWORD`, `AIRFLOW__CORE__FERNET_KEY`,
`AIRFLOW__WEBSERVER__SECRET_KEY`, `AIRFLOW__CORE__INTERNAL_API_SECRET_KEY`,
`AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_EMAIL`, and
`GCN_DB_PASSWORD`. Runtime secret validation still requires the latter under
the current release contract; GCN service removal and dbusers separation are
separate work. The bundle does not provision GCN or send GCN notices (producer
disabled/dry-run defaults), but the scientific DAG's existing GCN interactions
still require a compatible site service or an explicit later scope decision.
SMTP and database endpoints must be reachable from every scheduler/worker.

The cluster image build recipe is `deployment/kubernetes/Dockerfile`, with
repository-root build context and a current `cosiflow-airflow:native` base
built from `env/Dockerfile.airflow`. Publish an image for the site's CPU
architecture and use that reference in the site file. Both the base and the
cluster extension must contain the ported code; no image build was run here.

During the later acceptance phase, install bootstrap resources, run and wait
for the init Job, run and wait for the module-install Job, and only then start
the runtime Deployments. Kubernetes does not automatically enforce that
ordering across these files. Re-running completed Jobs requires a deliberate
upgrade procedure; do not delete persistent data. The init Job is Airflow's
service bootstrap, distinct from the FasTP scientific `init_pipelines` DAG.

## Acceptance boundary

This initial site profile intentionally starts one scheduler and includes no
lab reaper, scheduler restarts, node-observer ClusterRole, database HA,
automatic PVC provisioning or ingress. The lab reaper writes exact Airflow
task state and restarts schedulers; it needs its own version-specific review
and failure tests before promotion. Do not claim the lab's complete automatic
node-recovery behavior for this generated bundle yet.

The next checks are recorded in [the local FasTP acceptance checklist](../development/fastp-local-acceptance.md).
Until those pass, preserve the current local Compose mounts, environment
installation and LocalExecutor setting. Cluster deployment, failure tests and
clean installation belong to the subsequent acceptance phase.
