# Reset Cosidag Plugin

This plugin adds an Airflow UI page under **Develop tools -> Reset Cosidag** for managing the processed state of COSIDAG pipelines.

## What It Manages

COSIDAG stores processed paths in an Airflow Variable named:

```text
COSIDAG_PROCESSED::<dag_id>
```

The content depends on the COSIDAG monitoring policy:

| Policy | Stored values |
| --- | --- |
| `folder-driven` | Processed directory paths |
| `file-driven` | Processed file paths |

## Features

- Select a COSIDAG ID from active DAGs.
- View the current processed path list.
- Reset the whole variable to an empty list.
- Select individual entries with checkboxes.
- Select all entries with the header checkbox.
- Delete only the selected entries with the trash button.

## Usage

1. Open the Airflow web UI.
2. Navigate to **Develop tools -> Reset Cosidag**.
3. Select a COSIDAG ID.
4. Use **Reset Variable** to clear the whole history, or select specific rows and click the trash button to remove only those paths.

## Structure

```text
reset_cosidag_link/
├── reset_cosidag_plugin.py
├── templates/
│   └── reset_cosidag.html
└── README.md
```
