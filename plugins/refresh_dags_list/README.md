# Refresh DAGs List Plugin

This plugin adds an authenticated menu action under **Develop Tools**. It runs
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
4. The command will execute and you will be redirected to the home page with a success message
5. The DAGs list will be automatically refreshed

## How It Works

When you click on "Refresh DAGs List" in the menu:

1. The plugin executes `airflow dags list` command
2. It creates a new `DagBag` instance and forces parsing of all DAGs
3. It syncs the DAG bag with the database to update the UI
4. You are redirected to the home page with a success message

## Structure

```
refresh_dags_list/
├── refresh_dags_plugin.py    # Main plugin file
└── README.md                  # This file
```

## Notes

- The route is protected with `login_required`
- The `airflow dags list` command is executed in the context of the Airflow container
- The DAG bag refresh may take a few seconds to complete
- If there are any errors, they will be displayed as a warning message
