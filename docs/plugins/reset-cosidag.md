# Reset Cosidag plugin

This plugin adds an authorized Airflow UI page under
**Develop Tools → Reset Cosidag** for managing COSIDAG processed state.

## What It Manages

COSIDAG stores input lifecycle state in the PostgreSQL table
`cosiflow_cosidag_state`. The plugin exposes only rows whose status is
`succeeded`.

The content depends on the COSIDAG monitoring policy:

| Policy | Stored values |
| --- | --- |
| `folder-driven` | Processed directory paths |
| `file-driven` | Processed file paths |

## Features

- Select a COSIDAG ID from active DAGs.
- View the current processed path list.
- Reset all successfully processed paths for one DAG.
- Select individual entries with checkboxes.
- Select all entries with the header checkbox.
- Delete only the selected entries with the trash button.

## Usage

1. Open the Airflow web UI.
2. Navigate to **Develop Tools → Reset Cosidag**.
3. Select a COSIDAG ID.
4. Use **Reset Processed State** to clear successful history, or select specific rows and click the trash button to remove only those paths.

Reset operations do not delete active `claimed` rows. This prevents an operator
action from transferring an in-flight input to a second run. Failed paths are
already retryable and are not shown in the processed-path list.

## Authorization

- GET `/reset_cosidag/` and GET processed paths require `can_read` on
  `COSIflow COSIDAG State`.
- POST `/reset_cosidag/reset` and selective deletion require `can_edit` on the
  same resource.
- Scientist can inspect state but cannot mutate it. Operator and Admin can read
  and edit. Viewer receives neither permission.

Every `dag_id` is checked against active `DagModel` rows before state is read or
written. POST requests retain Airflow CSRF protection. Audit events record the
mutation and row count but do not include processed paths.

## Migration and rollback

`airflow-init` creates the state schema and imports valid legacy
`COSIDAG_PROCESSED::<dag_id>` Variables exactly once. The Variables remain
unchanged as rollback evidence, but the plugin does not use them after the
migration. Invalid legacy JSON fails initialization visibly.

## Structure

```text
reset_cosidag_link/
├── reset_cosidag_plugin.py
└── templates/
    └── reset_cosidag.html
```
