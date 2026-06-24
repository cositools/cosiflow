# Core DAG Reference

The core `cosiflow` repository does not currently ship production DAG Python files in `dags/`.
This directory is kept as the Airflow DAG mount point and as the place where module DAG symlinks are exposed after installation.

Scientific workflows are supplied by external Cosiflow modules, such as `fast-transient-analysis-pipeline`, and are hot-loaded into the running Airflow environment with `env/hot_load_module.sh`.

## What Lives In Core Cosiflow

| Area | File or directory | Purpose |
| --- | --- | --- |
| COSIDAG framework | `modules/cosidag.py` | Reusable Airflow DAG subclass for filesystem-driven scientific workflows |
| Date helpers | `modules/date_helper.py` | Date parsing and filtering support for COSIDAG monitoring |
| Failure callback | `callbacks/on_failure_callback.py` | Shared callback support for task failures |
| Airflow plugins | `plugins/` | UI helpers such as DAG refresh, data browsing, MailHog access, and COSIDAG reset |
| Module loader | `env/hot_load_module.sh` | Installs, updates, and removes external modules |

## COSIDAG Contract

`modules/cosidag.py` defines the `COSIDAG` convenience class used by module pipelines.
A COSIDAG can wire the following task pattern:

1. `check_new_file`, when `monitoring_folders` is configured.
2. `automatic_retrig`, when `auto_retrig=True`.
3. `resolve_inputs`, when `file_patterns` is configured.
4. Custom scientific tasks provided by `build_custom`.
5. `show_results`.

The shared XCom contract is:

| Key | Produced by | Meaning |
| --- | --- | --- |
| `detected_path` | `check_new_file` | The accepted folder or file path |
| `detected_folder` | `check_new_file` | The accepted folder, or the parent folder for file-driven runs |
| `detected_file` | `check_new_file` | The accepted file in file-driven runs |
| Custom `file_patterns` keys | `resolve_inputs` | Paths resolved inside the detected folder |

## Module DAG Catalogs

Module DAG catalogs should be maintained in the module repository, next to the DAG files they describe.

For the Fast Transient Analysis Pipeline, see:

```text
../fast-transient-analysis-pipeline/src/dags/README.md
```

This separation keeps the core framework documentation stable while allowing each scientific module to document its own DAG IDs, inputs, outputs, operators, and runtime assumptions.
