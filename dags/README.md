# DAG & COSIDAG Catalog

This document describes all **DAGs** and **COSIDAGs** available in this repository, including:

* DAG title
* workflow type (DAG vs COSIDAG)
* purpose
* inputs / outputs
* number of tasks (and task layout)
* operator types
* XCom usage (inter-task communication)

---

## COSIDAG framework (module)

### File

`cosidag.py`

### What it is

This is **not a DAG**.
It defines the **`COSIDAG`** convenience class used by multiple pipelines.

### Standard COSIDAG layout

A COSIDAG wires a common pattern:

1. `check_new_file` *(optional)*
2. `automatic_retrig` *(optional)*
3. `resolve_inputs` *(optional)*
4. `[custom tasks]`
5. `show_results` *(always)*

### Operators used internally

* `PythonOperator`
* `EmptyOperator`
* `TriggerDagRunOperator`
* (also sensors / utilities internally, depending on configuration)

### XCom contract (key design)

* `detected_folder` is pushed by `check_new_file` (or by a fallback setter task when monitoring is disabled)
* resolved inputs are pushed by `resolve_inputs` using keys from `file_patterns`
* `show_results` reads `detected_folder` from XCom and prints a results URL (if configured)

✅ This file is the **contract** that all COSIDAG-based pipelines rely on.

---

# Entry-point DAGs

## `init_pipelines`

### File

`cosipipe_simdata.py` (header indicates: `# dags/init_pipelines.py`)

### Type

**Standard DAG** (entry-point / initializer)

### Workflow purpose

Single entry point that:

* prepares/stages raw inputs
* resolves configuration
* creates the run/products directory
* optionally performs a background cut
* creates symlinks to standardized locations

This is the DAG you trigger from the Airflow UI to bootstrap a pipeline run.

### Inputs

Airflow UI params / configuration (conceptually):

* destination selector (where to save products)
* paths for source/background/orientation/response (or folders from which they can be resolved)
* optional “date/selection policy” style filters

### Outputs

* standardized run folder (products directory)
* staged inputs (symlinked or copied, depending on logic)
* background cut output (if enabled)

### Number of tasks

**6 tasks**

* `prepare_raw_dirs`
* `resolve_config`
* `stage_all_files`
* `create_products_dir`
* `background_cut`
* `create_symlinks`

### Operators used

* `PythonOperator`
* `ExternalPythonOperator` (used for scientific steps executed in the `cosipy` conda env)

### XCom usage

✅ **Yes**
Used to propagate resolved configuration and output paths between tasks.

---

# Scientific COSIDAG pipelines

## `cosidag_tsmap`

### File

`cosidag_tsmap.py`

### Type

**COSIDAG**

### Workflow purpose

TS Map computation pipeline (binned GRB + background → TS map products).

### Inputs

Resolved via COSIDAG `file_patterns` (search under detected folder):

* `grb_file`: `GRB*_unbinned_*.fits*`
* `background_file`: `Total_BG*_unbinned_*.fits*`
* `orientation_file`: `*.ori`
* `response_file`: `Response*.h5`

Plus COSIDAG parameters (monitoring folders, date queries, selection policy, etc.).

### Outputs

* binned GRB file
* binned background file
* TS map products (standard and multi-resolution)
* results stored in the run/products directory used by COSIDAG

### Number of tasks

**~8 total** (COSIDAG base tasks + custom tasks)

**Base COSIDAG tasks (created by framework):**

* `check_new_file`
* `automatic_retrig` *(depends on config)*
* `resolve_inputs`
* `show_results`

**Custom tasks in this pipeline:**

* `bin_grb_source`
* `bin_background`
* `ts_map_computation`
* `ts_map_mulres_computation`

### Operators used

* `ExternalPythonOperator` (science steps)
* plus COSIDAG internal operators (see `cosidag.py`)

### XCom usage

✅ **Yes**

* COSIDAG publishes resolved input paths (`resolve_inputs`)
* custom tasks read paths using templated `ti.xcom_pull(...)` (return_value pattern is used)

---

## `cosidag_lcurve`

### File

`cosidag_lcurve.py`

### Type

**COSIDAG**

### Workflow purpose

Light Curve plotting pipeline (binned GRB + background → light curve products).

### Inputs

Resolved via COSIDAG `file_patterns`:

* `grb_file`: `GRB*_unbinned_*.fits*`
* `background_file`: `Total_BG*_unbinned_*.fits*`
* `orientation_file`: `*.ori`
* `response_file`: `Response*.h5`

### Outputs

* binned GRB file
* binned background file
* light curve plot(s) / products saved into the pipeline output folder

### Number of tasks

**~7 total** (COSIDAG base tasks + custom tasks)

**Base COSIDAG tasks:**

* `check_new_file`
* `automatic_retrig` *(enabled in config in this file)*
* `resolve_inputs`
* `show_results`

**Custom tasks in this pipeline:**

* `bin_grb_source`
* `bin_background`
* `plot_lightcurve`

### Operators used

* `ExternalPythonOperator`
* plus COSIDAG internal operators

### XCom usage

✅ **Yes**
Uses XCom for:

* detected folder
* resolved inputs
* passing file paths between binning and plotting

---

# Tutorial / example COSIDAGs

## `cosidag_example`

### File

`cosidag_example.py`

### Type

**COSIDAG (example)**

### Workflow purpose

Demonstrates how to attach a custom task to a COSIDAG and how to:

* consume `detected_folder` via XCom
* search files inside the detected folder via helper `dag.find_file_by_pattern(...)`

### Inputs

* `monitoring_folders` points to a sample location (example uses `/home/gamma/workspace/data/tsmap`)
* COSIDAG detection parameters (depth, date queries, etc.)

### Outputs

* logs + demonstration of resolved file path (printed)
* depends on your custom implementation

### Number of tasks

**~5–6 total** (COSIDAG base tasks + 1 custom task)
Custom task:

* `custom_process`

### Operators used

* `PythonOperator`
* plus COSIDAG internal operators

### XCom usage

✅ **Yes** (pulls `detected_folder`)

---

## `cosidag_helloworld`

### File

`cosidag_helloworld.py`

### Type

**COSIDAG (minimal tutorial)**

### Workflow purpose

Minimal “hello world” COSIDAG showing:

* how to define `build_custom(dag)`
* how to chain a single custom task
* how to run even with `monitoring_folders=None` (no folder detection)

### Inputs

None (demo-style). In this file `monitoring_folders=None` and `auto_retrig=False`.

### Outputs

Logs only.

### Number of tasks

**~2–3 total**

* COSIDAG “detected folder setter” task may exist when monitoring is disabled (implementation detail)
* custom task: `hello_world`

### Operators used

* `PythonOperator`
* `BashOperator`
* plus COSIDAG internal operators

### XCom usage

✅ **Yes** (used to keep the COSIDAG contract consistent, even in no-monitoring mode)

---

## `cosidag_tutorial_a_svd`

### File

`cosidag_a.py`

### Type

**COSIDAG (tutorial A)**

### Workflow purpose

Tutorial A:

* build a binary text matrix
* factorize via SVD
* save factor outputs and diagnostic plots

### Inputs

* demo parameters (TEXT, SIZE, FONT_SIZE, RANK)
* output base directory: `/home/gamma/workspace/data/tutorials/a_b_factor`

### Outputs

In `BASE_DIR`:

* `factors.pkl`
* `factor_L.png`
* `factor_R.png`
  (and any other artifacts generated by the tutorial)

### Number of tasks

**~3–4 total**

* COSIDAG “set detected folder” step (since this tutorial doesn’t rely on monitoring)
* custom task:

  * `a_factorize_text_matrix`

### Operators used

* `PythonOperator` (setup / set folder)
* `ExternalPythonOperator` (SVD + plots in external env)

### XCom usage

✅ **Yes**
Uses XCom to propagate base folder / run folder into the external step.

---

## `cosidag_tutorial_b_reconstruct`

### File

`cosidag_b.py`

### Type

**COSIDAG (tutorial B)**

### Workflow purpose

Tutorial B:

* load SVD factors generated in tutorial A
* reconstruct the matrix (float + binarized)
* save reconstruction plots

### Inputs

From `BASE_DIR`:

* `factors.pkl`
  Parameters:
* `bin_thr` threshold

### Outputs

In `BASE_DIR`:

* `reconstruction_float.png`
* `reconstruction_binary.png`

### Number of tasks

**~3–4 total**

* COSIDAG “set detected folder” step (no monitoring)
* custom task:

  * `b_reconstruct_and_plot`

### Operators used

* `PythonOperator`
* `ExternalPythonOperator`

### XCom usage

✅ **Yes**
Folder/path handoff via XCom.

---

# Test / utility DAGs

## `dag_parallel_test_1`

### File

`dag_parallel_test_1.py`

### Type

**Standard DAG (test)**

### Workflow purpose

Simple parallelism test: two independent sleep tasks.

### Inputs / Outputs

None (logs only).

### Number of tasks

**2**

* `sleep_a`
* `sleep_b`

### Operators used

* `BashOperator`

### XCom usage

❌ No

---

## `dag_parallel_test_2`

### File

`dag_parallel_test_2.py`

### Type

**Standard DAG (test)**

### Workflow purpose

Second parallelism test DAG.

### Inputs / Outputs

None (logs only).

### Number of tasks

**2**

* `sleep_c`
* `sleep_d`

### Operators used

* `BashOperator`

### XCom usage

❌ No

---

## `dag_with_email_alert`

### File

`fail_task.py`

### Type

**Standard DAG (test / failure path)**

### Workflow purpose

Intentional failure DAG used to test:

* failure handling
* alerting / callbacks (depending on Airflow configuration)

### Inputs / Outputs

None (it fails on purpose).

### Number of tasks

**1**

* `failing_task`

### Operators used

* `PythonOperator`

### XCom usage

❌ No

---

# Summary table

| DAG ID / Name                    | Type    | Purpose (short)                         | Tasks (approx) | Operators (main)                        | XCom |
| -------------------------------- | ------- | --------------------------------------- | -------------- | --------------------------------------- | ---- |
| `init_pipelines`                 | DAG     | stage/init run + background cut + links | 6              | PythonOperator, ExternalPythonOperator  | Yes  |
| `cosidag_tsmap`                  | COSIDAG | TS map products                         | ~8             | ExternalPythonOperator (+ COSIDAG core) | Yes  |
| `cosidag_lcurve`                 | COSIDAG | light curve products                    | ~7             | ExternalPythonOperator (+ COSIDAG core) | Yes  |
| `cosidag_example`                | COSIDAG | example: detected_folder + file search  | ~5–6           | PythonOperator (+ COSIDAG core)         | Yes  |
| `cosidag_helloworld`             | COSIDAG | minimal tutorial                        | ~2–3           | PythonOperator, BashOperator            | Yes  |
| `cosidag_tutorial_a_svd`         | COSIDAG | SVD factorization tutorial              | ~3–4           | PythonOperator, ExternalPythonOperator  | Yes  |
| `cosidag_tutorial_b_reconstruct` | COSIDAG | reconstruction tutorial                 | ~3–4           | PythonOperator, ExternalPythonOperator  | Yes  |
| `dag_parallel_test_1`            | DAG     | parallelism test                        | 2              | BashOperator                            | No   |
| `dag_parallel_test_2`            | DAG     | parallelism test                        | 2              | BashOperator                            | No   |
| `dag_with_email_alert`           | DAG     | intentional failure / alert test        | 1              | PythonOperator                          | No   |
