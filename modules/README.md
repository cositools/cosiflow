# CosiDAG

A high-level reactive DAG template for filesystem-driven scientific workflows

## Overview

`COSIDAG` is a convenience subclass of Airflow’s `DAG` designed to simplify the creation of **file-driven scientific pipelines**.
It encapsulates a standard five-step workflow pattern:

1. **check_new_file**
   Monitors one or more folders, searching for new subdirectories.
   Handles date filtering, basename filtering, depth traversal, and stability checks.
   Pushes the detected folder path into XCom and tracks processed folders via Airflow Variables.

2. **automatic_retrig**
   Immediately triggers a new run of the same DAG, so the sensor keeps watching for fresh data.
   This enables near-real-time reactive pipelines.

3. **resolve_inputs** *(optional)*
   If `file_patterns` is provided, the module automatically scans the detected folder, resolves filenames, and pushes the results to XCom using user-defined keys.

4. **custom tasks**
   A user-provided `build_custom(dag)` function can attach any processing tasks (analysis, binning, model execution, visualization).
   These tasks use the values produced by steps 1–3 via `xcom_pull`.

5. **show_results**
   Logs a homepage URL (read from an environment variable) and optionally builds a deep link referencing the detected folder.

This structure removes 80–90% of the boilerplate typically involved in writing dynamic pipelines while ensuring consistency across future COSI workflows (e.g. **TSMap**, **Light Curve**).

---

## Why use CosiDAG?

CosiDAG solves common problems in scientific workflows:

* **Dynamic file discovery** — your pipeline reacts automatically to new folders dropped on disk.
* **No hard-coded filenames** — files are resolved automatically using regex patterns.
* **Unified behavior** — TSMap, Light Curve, and other pipelines share the same structure.
* **Clean separation of infrastructure vs science code** — COSIDAG handles monitoring, deduplication, and XCom logic; the user only implements the scientific tasks.
* **Consistent task orchestration** — every pipeline follows the same five-step DAG layout.

Additionally, CosiDAG integrates seamlessly with the **MailHog link plugin**, which captures and exposes exception emails directly in the Airflow UI.
This means that mailbox-based alerting and debugging works **out-of-the-box** with all CosiDAG-derived workflows, without requiring extra configuration.

---

## Minimal Example

```python
from datetime import datetime
from cosidag import COSIDAG
from airflow.operators.python import ExternalPythonOperator

def build_custom(dag):

    # Pull runtime-discovered folder and file paths
    RUN_DIR = "{{ ti.xcom_pull('check_new_file', key='detected_folder') }}"
    RESPONSE = "{{ ti.xcom_pull('resolve_inputs', key='response_file') }}"

    def compute(run_dir: str, response_file: str):
        print("Running analysis...", run_dir, response_file)

    compute_task = ExternalPythonOperator(
        task_id="compute_step",
        python="/path/to/external/env/bin/python",
        python_callable=compute,
        op_kwargs={"run_dir": RUN_DIR, "response_file": RESPONSE},
        dag=dag,
    )

    return [compute_task]

with COSIDAG(
    dag_id="example_cosidag",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    monitoring_folders=["/data/incoming"],
    file_patterns={
        "response_file": r"Response.*\.h5"
    },
    select_policy="latest_mtime",
    only_basename="products",
    prefer_deepest=True,
    idle_seconds=5,
    build_custom=build_custom,
    tags=["example"],
):
    pass
```

---

## How CosiDAG Passes Data Between Tasks

CosiDAG uses **XCom** to pass runtime-discovered paths to the user-defined tasks.

* `check_new_file` pushes the detected folder:

  ```
  key="detected_folder"
  ```
* `resolve_inputs` pushes files matched by regex patterns, using the corresponding keys:

  ```
  "response_file": "/path/to/Response_003.h5"
  ```

User tasks retrieve them via:

```python
"{{ ti.xcom_pull('check_new_file', key='detected_folder') }}"
"{{ ti.xcom_pull('resolve_inputs', key='response_file') }}"
```

This allows pipelines to be fully dynamic and independent of hard-coded paths.

---

## Configuration Parameters

| Parameter                     | Type                        | Description                                                  |
| ----------------------------- | --------------------------- | ------------------------------------------------------------ |
| `monitoring_folders`          | list[str]                   | Folders to scan for new data.                                |
| `level`                       | int                         | Directory depth to scan.                                     |
| `date`                        | str/int                     | Accept only folders with this date.                          |
| `date_queries`                | str                         | Query expression for date filtering (e.g. `==20251119`).     |
| `only_basename`               | str                         | Accept only folders with this basename (e.g., `"products"`). |
| `prefer_deepest`              | bool                        | Selects deepest matching subfolder.                          |
| `min_files`                   | int                         | Minimum number of files required before accepting a folder.  |
| `idle_seconds`                | int                         | Seconds to wait for the folder to “settle” (no live writes). |
| `ready_marker`                | str                         | Marker file required for folder acceptance.                  |
| `home_env_var`                | str                         | ENV var containing the UI base URL.                          |
| `file_patterns`               | dict[str, str]              | Mapping XCom key → regex pattern for auto file resolution.   |
| `select_policy`               | `"latest_mtime"`, `"first"` | Strategy for resolving multiple matches.                     |
| `default_args_extra`          | dict                        | Additional default args for tasks.                           |
| `tags`                        | list[str]                   | Airflow UI tags.                                             |
| `auto_retrig`                 | bool                        | Enables real-time monitoring.                                |
| `processed_variable`          | str                         | Name of the Airflow Variable storing processed paths.        |
| `builder_fn` / `build_custom` | callable                    | Function that attaches user-defined tasks.                   |
| `xcom_detected_key`           | str                         | XCom key for detected folder path.                           |

---

Ecco una **sezione aggiuntiva** da integrare nel README.md, in inglese, perfettamente coerente con il resto della documentazione.
Include tutti i punti richiesti: configurazione tramite UI, gestione della Airflow Variable, comandi utili, e la possibilità di disattivare il retrigger automatico.

---

## Configuring and Running a CosiDAG from the Airflow UI

Once a CosiDAG script is defined, **you do not need to modify the Python file** to run the pipeline on different datasets.
Instead, Airflow’s Trigger UI allows you to dynamically set:

* monitoring folders
* date filters
* file patterns
* selection policy
* any custom configuration values defined in your DAG parameters

This makes CosiDAG pipelines fully reusable: **the same code can be triggered dozens of times with different inputs**, without editing the script.

To run a CosiDAG on new data:

1. Open the DAG in the Airflow Web UI
2. Click **Trigger DAG**
3. Fill in the configuration form (folder paths, patterns, etc.)
4. Click **Trigger**

Every run will process a different dataset with identical logic.

---

## Processed Folder Tracking (Airflow Variable)

Every CosiDAG keeps track of previously processed folders using an Airflow Variable named:

```
COSIDAG_PROCESSED::<dag_id>
```

This prevents the pipeline from reprocessing the same folder unless explicitly requested.

### Viewing the stored folders

From the CLI:

```bash
airflow variables get COSIDAG_PROCESSED::<dag_id>
```

### Clearing the list (e.g. to reprocess everything)

```bash
airflow variables set COSIDAG_PROCESSED::<dag_id> "[]"
```

### Removing the variable entirely

```bash
airflow variables delete COSIDAG_PROCESSED::<dag_id>
```

These commands allow you to “reset” the monitoring history at any time.

---

## Disabling Automatic Retrigger

By default, CosiDAG enables **automatic retriggering** (`automatic_retrig=True`), meaning the DAG keeps running in a loop to continuously watch for new folders.

You can disable this behavior by setting:

```python
auto_retrig=False
```

or by exposing it as a configurable parameter and turning it off in the Airflow UI.

When retriggering is disabled:

* The DAG will **not** restart automatically
* You can manually rerun the pipeline on a folder that was already processed
* This is useful for **re-analysis**, debugging, or running multiple configurations on the same dataset

Disabling retriggering + clearing the processed-variable list lets you fully reprocess any folder without modifying the DAG code.

---

## MailHog Link Plugin (Exception Visibility)

CosiDAG integrates cleanly with the Airflow **MailHog link plugin**, a small extension that:

* intercepts exception emails generated by Airflow,
* displays a direct link to the captured email next to the failed task in the Airflow UI.

While you don’t need to configure anything manually, it is useful to know that:

* When a CosiDAG-based pipeline fails, any email alerts triggered by Airflow’s email backend will appear in the MailHog UI.
* COSIFLOW’s environment already includes MailHog and the plugin, so notifications are automatically routed and linked.
* This provides faster debugging and lowers the cost of diagnosing failed tasks.

You do **not** need to interact with MailHog directly, CosiDAG DAGs just benefit from it.

---

## When to Use CosiDAG

CosiDAG is ideal when your workflow:

* **should react to new data** appearing in a filesystem,
* requires robust **folder validation**,
* should **run analysis scripts in external Python environments**,
* must be: 
  - **easy to maintain** 
  - **extend**
  - **reuse across different scientific pipelines**

Examples include:

* TSMap pipeline
* Light Curve pipeline
* SimData ingestion
* Any workflow triggered by incoming instrument data

---

## Summary

* CosiDAG pipelines are reusable and configurable directly from the Airflow UI
* No need to modify the DAG script to process new datasets
* Processed folders are stored in `COSIDAG_PROCESSED::<dag_id>`
* CLI commands allow viewing, clearing, or deleting this history
* The automatic retrigger step can be disabled to purposely re-run old folders