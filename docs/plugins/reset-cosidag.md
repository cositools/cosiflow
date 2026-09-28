# Reset Cosidag plugin

This plugin adds an authorized Airflow UI page under
**Develop Tools → Reset Cosidag** for managing COSIDAG processed state and
operator-controlled retries.

## What It Manages

COSIDAG stores input lifecycle state in the PostgreSQL table
`cosiflow_cosidag_state`. The processed-path panel exposes `succeeded` rows; a
separate failed-path panel exposes `failed` rows without mixing the two actions.

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
- Review failed inputs, their attempt count, and their last processing error.
- Requeue one failed input after explicit confirmation and a required reason.
- Optionally supply validated runtime overrides for that retry.

## Usage

1. Open the Airflow web UI.
2. Navigate to **Develop Tools → Reset Cosidag**.
3. Select a COSIDAG ID.
4. Use **Reset Processed State** to clear successful history, or select specific rows and click the trash button to remove only those paths.
5. For a failed input, choose **Retry**, provide an audit reason, optionally
   provide JSON runtime overrides, and confirm the action.

Reset operations do not delete active `claimed` rows. This prevents an operator
action from transferring an in-flight input to a second run. Failed paths are
not selected automatically: only the dedicated retry action may return them to
`queued`. The action records author, reason, and time, and cannot target
`claimed` or `succeeded` rows.

## Authorization

- GET `/reset_cosidag/`, GET processed paths, and GET failed paths require `can_read` on
  `COSIflow COSIDAG State`.
- POST `/reset_cosidag/reset`, selective deletion, and POST manual retry require
  `can_edit` on the same resource.
- Scientist can inspect state but cannot mutate it. Operator and Admin can read
  and edit. Viewer receives neither permission.

Every `dag_id` is checked against active `DagModel` rows before state is read or
written. POST requests retain Airflow CSRF protection. Reset audit events avoid
logging processed paths; retry audit data is stored with the queue row and
includes path, author, reason, time, and validated overrides.

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
