# Refresh DAGs List Plugin

This plugin adds an authorized menu action under **Develop Tools**. It runs
`airflow dags list`, rebuilds the DAG bag, and synchronizes parsed DAGs with the
Airflow metadata database.

## Features

- Executes the `airflow dags list` command
- Forces a refresh of the DAG bag to update the list in the UI
- Adds **Develop Tools → Refresh DAGs List** to the Airflow menu

## Installation

The plugin is automatically loaded by Airflow when placed in the `plugins/` folder.

## Usage

1. Navigate to the Airflow web UI
2. Open **Develop Tools → Refresh DAGs List**
4. Confirm the POST action on the displayed page.
5. The DAG list is refreshed and the result appears after a 303 redirect.

## How It Works

When you confirm "Refresh DAGs List":

1. The plugin executes `airflow dags list` command
2. It creates a new `DagBag` instance and forces parsing of all DAGs
3. It syncs the DAG bag with the database to update the UI
4. You are redirected to the confirmation page with a status message

## Structure

```
refresh_dags_list/
├── refresh_dags_plugin.py    # Main plugin file
├── templates/refresh_dags.html
└── README.md                  # This file
```

## Notes

- The action route is POST-only and requires `can_edit` on
  `COSIflow DAG Catalog`; GET on that route returns 405.
- Only Operator and Admin receive the permission and menu entry.
- Authorization runs before `subprocess.run`, DAG parsing and `sync_to_db`.
- The form retains Airflow CSRF protection.
- The `airflow dags list` command is executed in the context of the Airflow container
- The DAG bag refresh may take a few seconds to complete
- If there are any errors, they will be displayed as a warning message
