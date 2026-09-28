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
  succeeded; the first failure returns the input to the queue after a bounded
  backoff, while the second failure leaves it in `failed` for operator review.

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

COSIDAG-specific arguments are keyword-only. Airflow `DAG` arguments continue
to pass through: `dag_id` may be the first positional argument, while
`start_date`, `schedule_interval`, `catchup`, `description`,
`max_active_runs`, and `max_active_tasks` can be supplied by keyword as usual.
`monitoring_folders` is required as a keyword; use an empty list for a
manual-only DAG.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `monitoring_folders` | required | Root paths watched by the sensor; use `[]` for a manual-only DAG |
| `level` | `1` | Maximum child-directory depth in folder-driven mode |
| `date` | `None` | Legacy source-level exact date filter; invalid values fail closed |
| `date_queries` | `None` | Legacy source-level comparisons; invalid values fail closed |
| `build_custom` | `None` | Callable that attaches scientific tasks |
| `sensor_poke_seconds` | `30` | Sensor polling interval |
| `sensor_timeout_seconds` | six hours | Sensor timeout |
| `input_poke_seconds` | `120` | Input-pattern readiness polling interval |
| `input_timeout_seconds` | 30 minutes | Input-pattern readiness timeout |
| `home_env_var` | `COSIFLOW_HOME_URL` | Environment variable used by `show_results` |
| `idle_seconds` | `20` | Minimum time for size and mtime metadata to remain unchanged |
| `min_files` | `1` | Minimum recursive file count in folder-driven mode |
| `ready_marker` | `None` | Optional relative marker path in folder-driven mode; no marker is required by default |
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
| `refill_threshold` | `20` | Refill the persistent queue when eligible queued rows fall below this value |
| `discovery_batch_size` | `100` | Maximum candidates inserted during one refill cycle |
| `retry_backoff_seconds` | `300` | Delay before the single automatic retry becomes claimable |

### Constructor migration and deprecation

The keyword-first form is the supported constructor API:

```python
COSIDAG(
    "cosipipe_example",
    monitoring_folders=["/data/incoming"],
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
)
```

Using `dag_id="cosipipe_example"` is also supported. Existing DAGs that pass
only `monitoring_folders` positionally and `dag_id` by keyword continue to load
temporarily:

```python
# Deprecated compatibility form
COSIDAG(["/data/incoming"], dag_id="cosipipe_example", ...)
```

The compatibility form emits `DeprecationWarning` and will be removed after a
deprecation window. Migrate by adding the `monitoring_folders=` keyword. Other
COSIDAG options must be passed by keyword; the compatibility shim does not
accept additional positional COSIDAG arguments. Supplying both positional and
keyword monitoring configuration fails immediately with `TypeError`.

The Trigger UI exposes the preferred structured `date_filters` form: a list of
objects containing `operator` (`<`, `<=`, `==`, `>=`, or `>`) and an ISO
`YYYY-MM-DD` date. Legacy date arguments remain accepted for existing DAG
source files, but are not presented as new UI fields.

The detected XCom keys and transactional state schema are fixed by the current
implementation; there are no `processed_variable`, `xcom_detected_key`, or
`builder_fn` aliases.

## Monitoring policies

### Folder-driven

For every monitoring root, the sensor scans child directories up to `level`,
then applies date, basename, processed-state, depth, optional marker,
minimum-file, and stability checks. The sensor runs in `reschedule` mode, so it
releases its worker slot between checks.

Stability requires two or more observations of the same directory snapshot.
The recursive file count, total size, latest nanosecond mtime, and a digest of
relative paths, sizes, and mtimes must remain unchanged for at least
`idle_seconds`. Observations are stored transactionally in
`cosiflow_cosidag_stability`, rather than in task XCom, because Airflow clears
task-scoped XCom when a sensor resumes after `UP_FOR_RESCHEDULE`. Consumed
observations are removed, while abandoned observations expire after 24 hours.
The optional `ready_marker` remains an additional producer
contract only when explicitly configured. It is not needed or required by
default and does not replace the metadata stability window. A configured
marker must be relative to the candidate folder. Absolute paths, parent
traversal, and paths that resolve outside the candidate through a symlink are
rejected.

COSIflow inventories candidates only when the persistent queue falls below
`refill_threshold`, and inserts at most `discovery_batch_size` new canonical
paths per refill. Candidates are evaluated in the configured priority order, but each candidate
keeps an independent stability history. A higher-priority folder that is still
changing, lacks enough files, or does not satisfy an optional marker therefore
does not prevent a later ready folder from being selected.

Each monitoring root is inventoried with one filesystem walk per poke. Every
regular file is stat'ed once, and candidate snapshots are aggregated bottom-up
from shared child metadata. Nested candidates therefore do not recursively
rescan the same files. Per-candidate readiness diagnostics are emitted at debug
level; normal logs retain cycle summaries and the selected path.

On success it publishes:

```text
check_new_file.detected_path
check_new_file.detected_folder
check_new_file.monitoring_policy = folder-driven
```

The accepted directory is first queued and then claimed transactionally. Its
filesystem type, confinement, basename/date filters, marker, file count, and
stability snapshot are checked again after the claim and before any XCom is
published. It becomes processed only after `finalize_cosidag_state` confirms
that `show_results` and its required upstream chain succeeded.

### File-driven

File-driven mode checks only direct child files of each monitoring root.
`level`, `prefer_deepest`, `ready_marker`, and `min_files` do not apply.
Readiness requires the selected file's size and nanosecond mtime to remain
unchanged for at least `idle_seconds`. The rescheduling sensor releases its
worker slot between observations. Its stability timestamp uses the same
transactional observation table and therefore survives each reschedule.

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

The accepted file, not its parent directory, is queued by canonical real path
and stored in the transactional state table after successful finalization.

## Input resolution

`file_patterns` maps XCom keys to recursive file searches under the detected
folder:

```python
file_patterns={
    "source_file": "*[Gg][Rr][Bb]*.fits*",
    "response_file": "regex:^(?!.*(?:GRB|BG)).*\\.h5$",
}
```

- ordinary values use recursive filename glob syntax, or relative-path glob
  syntax when the pattern contains a path separator;
- values beginning with `regex:` are matched against each basename with
  `re.match`;
- `first` selects the lexicographically first path;
- `latest_mtime` selects the path with the newest modification time;
- a missing required match fails `resolve_inputs`;
- one recursive inventory is shared by every configured pattern;
- the selected files' size and nanosecond mtime must remain unchanged for at
  least `idle_seconds` before XComs are published;
- the selected-set stability observation is persisted outside task XCom and
  therefore survives every sensor reschedule;
- readiness uses a `PythonSensor` in `reschedule` mode, so no Python operator
  sleeps while holding a worker slot.

The compatibility method `find_file_by_pattern(pattern, detected_folder)`
accepts a raw regular expression without the `regex:` prefix. It delegates to
the same file-only inventory and basename `re.match` logic, and returns the
lexicographically first matching path. It therefore has the same anchored and
deterministic behavior as a declarative `regex:` pattern with `select_policy`
set to `first`.

The selected paths are published under the supplied mapping keys. The detected
folder is also republished as `resolve_inputs.run_dir`.

## Runtime configuration in the Airflow UI

The Trigger DAG form exposes COSIDAG parameters, but only values explicitly read
from `dag_run.conf` can change behavior at runtime.

Runtime overrides currently supported by the sensor are:

- `monitoring_folders`;
- `level`;
- structured `date_filters`; legacy `date` and `date_queries` remain accepted by
  the backend for compatibility;
- `idle_seconds`, `min_files`, and `ready_marker`;
- `only_basename` and `prefer_deepest`;
- `policy` or `monitoring_policy`.

`automatic_retrig` also reads `auto_retrig` and `max_retrig_runs` from
`dag_run.conf` when that task exists. The corresponding UI fields are omitted
when the DAG is built with `auto_retrig=False`. The sensor also reads
`claim_stale_seconds`.

Runtime configuration is validated before filesystem scanning, stale-claim
recovery, or a new claim. `prefer_deepest` and `auto_retrig` accept native
booleans, integer `1`/`0`, or case-insensitive string values
`true`/`false`, `yes`/`no`, `on`/`off`, and `1`/`0`. Other values are rejected
instead of using Python truthiness. Monitoring policy accepts only
`folder-driven` or `file-driven`; input selection policy accepts only `first`
or `latest_mtime` and is validated when the DAG is constructed, before an
input inventory can run.

When `resolve_inputs` exists, it accepts validated runtime `file_patterns` and
`select_policy` overrides. Pattern keys must be safe XCom keys, patterns must be
non-empty relative glob or `regex:` expressions, and invalid regular
expressions fail before inventory. These fields are omitted when no resolver
task exists. `input_poke_seconds`, `input_timeout_seconds`, `home_env_var`, and
Airflow concurrency limits remain source-only parse-time settings.

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
| `queued` | Discovered, validated candidate waiting for a worker | Yes, after `next_attempt_at` |
| `claimed` | A specific DagRun owns the input | No, except for an idempotent retry by the same run |
| `succeeded` | The required scientific chain completed | No |
| `failed` | Two processing attempts failed | No; an operator must retry it explicitly |
| `discarded` | Post-claim validation found a permanent mismatch | No |

Refill uses `INSERT ... ON CONFLICT DO NOTHING`, so concurrent discovery cannot
duplicate queue rows. Claiming uses a row-locked PostgreSQL CTE with
`FOR UPDATE SKIP LOCKED`; concurrent DagRuns therefore receive different
inputs without loading the full processed history into memory. Paths are keyed
by canonical real path and must remain confined to one configured monitoring
root, including across symlinks.

The sensor writes XCom only after the claimed path passes a second validation
against the stored discovery snapshot. `finalize_cosidag_state` changes the
owning claim to `succeeded` after `show_results` succeeds. On a first processing
failure it increments the attempt count and requeues the input after
`retry_backoff_seconds`; a second failure produces `failed` and stops automatic
selection. Successful rows are retained indefinitely, with no TTL or automatic
cap, so an accepted path is never silently made eligible again.

Stale claims are recovered only when both conditions hold:

1. the claim is older than `claim_stale_seconds`;
2. its owning DagRun is no longer `queued` or `running`.

The Airflow menu **Develop Tools → Reset Cosidag** keeps successful-history
reset separate from failed-input retry. Manual retry is limited to `failed`
rows, requires confirmation and an audit reason, records the author and time,
and validates any runtime override before returning the row to `queued`. It
cannot steal `claimed` rows or requeue `succeeded` rows. A reset or selective
deletion still removes successful history only.

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
