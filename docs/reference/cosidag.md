# COSIDAG developer guide

`COSIDAG` is an Airflow `DAG` subclass for reactive, filesystem-driven
scientific workflows. Its implementation is in
[`modules/cosidag.py`](https://github.com/cositools/cosiflow/blob/dev-review/modules/cosidag.py).

## Workflow shape

Depending on the constructor arguments, COSIDAG creates:

```text
check_new_file
  -> automatic_retrig
  -> resolve_inputs
  -> custom task roots ... custom task leaves
  -> show_results
  -> finalize_cosidag_state
```

- `check_new_file` exists only when `monitoring_folders` is non-empty.
- `automatic_retrig` exists only when `auto_retrig=True`.
- `resolve_inputs` exists only when `file_patterns` is non-empty.
- the custom graph is created by `build_custom(dag)`;
- `show_results` records the final detected path and optional browser URL.
- `finalize_cosidag_state` records success only after the required chain has
  succeeded; on failure it releases the claim for a later retry.

The automatic retrigger task starts the next watcher run before the current run
enters the scientific graph. `max_active_runs`, `max_active_tasks`, and
`max_retrig_runs` should therefore be chosen deliberately.

## Minimal folder-driven example

```python
from datetime import datetime

from airflow.operators.python import ExternalPythonOperator
from cosidag import COSIDAG


def build_custom(dag):
    run_dir = "{{ ti.xcom_pull(task_ids='check_new_file', key='detected_folder') }}"
    response = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='response_file') }}"

    def compute(run_dir: str, response_file: str):
        print("Running analysis", run_dir, response_file)

    ExternalPythonOperator(
        task_id="compute_step",
        python="/home/gamma/envs/myenv/bin/python",
        python_callable=compute,
        op_kwargs={"run_dir": run_dir, "response_file": response},
        dag=dag,
    )


with COSIDAG(
    dag_id="example_cosidag",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    monitoring_folders=["/home/gamma/workspace/data/incoming"],
    policy="folder-driven",
    level=3,
    only_basename="products",
    idle_seconds=5,
    file_patterns={
        # Glob syntax is the default.
        "response_file": "*.h5",
        # Prefix a pattern with regex: to match basenames as a regular expression.
        "orientation_file": r"regex:^(?!.*GRB).*\.(?:fits|ori)$",
    },
    select_policy="latest_mtime",
    build_custom=build_custom,
    tags=["example"],
) as dag:
    pass
```

## Constructor parameters

`monitoring_folders` is the only COSIDAG-specific positional argument without a
default. Standard Airflow `DAG` arguments such as `dag_id`, `start_date`,
`schedule_interval`, `catchup`, `description`, `max_active_runs`, and
`max_active_tasks` are passed through `*args` and `**kwargs`.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `monitoring_folders` | required | Root paths watched by the sensor; use `[]` for a manual-only DAG |
| `level` | `1` | Maximum child-directory depth in folder-driven mode |
| `date` | `None` | Exact `YYYYMMDD` or `YYYY-MM-DD` filter |
| `date_queries` | `None` | One comparison or a list, for example `>=2026-01-01` |
| `build_custom` | `None` | Callable that attaches scientific tasks |
| `sensor_poke_seconds` | `30` | Sensor polling interval |
| `sensor_timeout_seconds` | six hours | Sensor timeout |
| `home_env_var` | `COSIFLOW_HOME_URL` | Environment variable used by `show_results` |
| `idle_seconds` | `20` | Minimum age since the latest write |
| `min_files` | `1` | Minimum recursive file count in folder-driven mode |
| `ready_marker` | `None` | Required marker filename in folder-driven mode |
| `only_basename` | `None` | Exact candidate folder or file basename |
| `prefer_deepest` | `True` | Prefer deeper folder candidates |
| `file_patterns` | `None` | XCom-key to glob/`regex:` pattern mapping |
| `select_policy` | `first` | `first` or `latest_mtime` for multiple file matches |
| `policy` | `folder-driven` | `folder-driven` or `file-driven` |
| `tags` | `None` | Additional Airflow tags |
| `default_args_extra` | `None` | Overrides/extensions for task default arguments |
| `auto_retrig` | `True` | Create the self-retrigger task |
| `max_retrig_runs` | unlimited | Maximum number of automatic successor runs |
| `claim_stale_seconds` | `86400` | Minimum age before an orphaned claim may be recovered after its owning DagRun is no longer active |

The detected XCom keys and transactional state schema are fixed by the current
implementation; there are no `processed_variable`, `xcom_detected_key`, or
`builder_fn` aliases.

## Monitoring policies

### Folder-driven

For every monitoring root, the sensor scans child directories up to `level`,
then applies date, basename, processed-state, depth, marker, minimum-file, and
stability checks.

On success it publishes:

```text
check_new_file.detected_path
check_new_file.detected_folder
check_new_file.monitoring_policy = folder-driven
```

The accepted directory is claimed transactionally. It becomes processed only
after `finalize_cosidag_state` confirms that `show_results` and its required
upstream chain succeeded.

### File-driven

File-driven mode checks only direct child files of each monitoring root.
`level`, `prefer_deepest`, `ready_marker`, and `min_files` do not apply.

```python
with COSIDAG(
    dag_id="example_file_cosidag",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    monitoring_folders=["/home/gamma/workspace/data/incoming"],
    policy="file-driven",
    idle_seconds=20,
    build_custom=build_custom,
) as dag:
    pass
```

On success it publishes:

```text
check_new_file.detected_path
check_new_file.detected_file
check_new_file.detected_folder
check_new_file.monitoring_policy = file-driven
```

The accepted file, not its parent directory, is stored in the transactional
state table after successful finalization.

## Input resolution

`file_patterns` maps XCom keys to recursive file searches under the detected
folder:

```python
file_patterns={
    "source_file": "*[Gg][Rr][Bb]*.fits*",
    "response_file": "regex:^(?!.*(?:GRB|BG)).*\\.h5$",
}
```

- ordinary values use recursive glob syntax;
- values beginning with `regex:` are matched against each basename with
  `re.match`;
- `first` selects the lexicographically first path;
- `latest_mtime` selects the path with the newest modification time;
- a missing required match fails `resolve_inputs`;
- before publishing XComs, the task waits until every selected file can be
  opened.

The selected paths are published under the supplied mapping keys. The detected
folder is also republished as `resolve_inputs.run_dir`.

## Runtime configuration in the Airflow UI

The Trigger DAG form exposes COSIDAG parameters, but only values explicitly read
from `dag_run.conf` can change behavior at runtime.

Runtime overrides currently supported by the sensor are:

- `monitoring_folders`;
- `level`;
- `date` and `date_queries`;
- `idle_seconds`, `min_files`, and `ready_marker`;
- `only_basename` and `prefer_deepest`;
- `policy` or `monitoring_policy`.

`automatic_retrig` also reads `auto_retrig` and `max_retrig_runs` from
`dag_run.conf`. The sensor also reads `claim_stale_seconds`.

The current `resolve_inputs` task uses the `file_patterns` and `select_policy`
captured when the DAG is parsed. `home_env_var` and Airflow concurrency limits
are likewise parse-time settings. Change these in the DAG source rather than in
the Trigger form.

## Manual-only DAGs

To remove the sensor from the graph, construct the DAG with:

```python
monitoring_folders=[]
```

When `file_patterns` is configured, trigger the DAG with a valid directory:

```json
{
  "detected_folder": "/home/gamma/workspace/data/manual/products",
  "auto_retrig": false
}
```

When no monitoring task exists, `show_results` can also read `detected_path`,
`detected_file`, or `detected_folder` from the run configuration.

## Transactional input state

COSIDAG stores one row per `(dag_id, path)` in
`cosiflow_cosidag_state`. The primary key lets PostgreSQL arbitrate concurrent
claims instead of relying on a shared JSON document.

| State | Meaning | Eligible for a new claim |
| --- | --- | --- |
| `claimed` | A specific DagRun owns the input | No, except for an idempotent retry by the same run |
| `succeeded` | The required scientific chain completed | No |
| `failed` | The owning run did not complete successfully | Yes |

The sensor first checks readiness and then atomically claims the path. It writes
XCom only after the claim succeeds. `finalize_cosidag_state` changes the owning
claim to `succeeded` after `show_results` succeeds. If the upstream chain fails,
the finalizer marks the claim `failed` and fails itself so that the DagRun keeps
the correct failure status.

Stale claims are recovered only when both conditions hold:

1. the claim is older than `claim_stale_seconds`;
2. its owning DagRun is no longer `queued` or `running`.

The Airflow menu **Develop Tools → Reset Cosidag** lists only `succeeded` rows.
A reset or selective deletion removes successful history but never steals an
active claim. Disabling automatic retriggering does not disable this filtering.

### Legacy Variable migration

During `airflow-init`, after `airflow db migrate`, COSIflow applies
`env/migrations/001_cosidag_state.sql` and imports every valid
`COSIDAG_PROCESSED::<dag_id>` JSON list as `succeeded` rows. Each DAG migration
is recorded in `cosiflow_cosidag_state_migration`, making repeated init runs
idempotent.

Legacy Variables are retained for rollback evidence but are no longer read or
written at runtime. Invalid JSON or values that are not lists of strings stop
initialization instead of silently discarding state.

## Automatic retrigger idempotency

`automatic_retrig` derives a UUID v5 during task execution from the target DAG,
source `run_id`, and task ID. Different source runs therefore receive different
successor IDs, while retrying the same task produces the same ID. If the
successor was created before an ambiguous worker failure, a retry treats
`DagRunAlreadyExists` as an idempotent success and continues the current
scientific chain.

Successor `conf` is constructed as a Python dictionary at runtime. The helper
increments `retrig_run_count` without mutating the source configuration and can
read the JSON-string form produced by older serialized DAGs during migration.

## Final result

`show_results` publishes a dictionary under:

```text
show_results.cosidag_result
```

The dictionary contains `path`, `folder`, `file`, `policy`, and `url`. The URL is
available when `COSIFLOW_HOME_URL` (or the configured `home_env_var`) is present.
It points to the detected folder in Data Explorer, uses `COSI_DATA_DIR` as the
filesystem root, and safely URL-encodes folder names.

## Failures and MailHog

COSIDAG tasks inherit the shared failure callback and Airflow SMTP settings.
The local Compose stack routes generated email to MailHog. Open it from
**Develop Tools → Mailhog**; the plugin provides a menu redirect and does not add
per-task email links.
