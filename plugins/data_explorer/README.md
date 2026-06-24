# Data Explorer Plugin

This plugin adds an Airflow UI page for browsing files under the configured COSI data directory.

## Menu Entry

The plugin registers the following Airflow menu item:

```text
Results Browser -> heasarc Browser
```

The view is served from:

```text
/heasarcbrowser
```

## Data Root

The browser reads from:

```text
COSI_DATA_DIR
```

If the environment variable is not set, it falls back to:

```text
/home/gamma/workspace/data
```

## Features

* Lists folders and files under the data root.
* Lets authenticated users navigate subdirectories.
* Supports file download.
* Supports preview metadata and content handling for common text, image, FITS, HDF5, CSV, YAML, JSON, and archive extensions.
* Prevents navigation outside the configured data root.

## Structure

```text
data_explorer/
├── data_explorer_plugin.py
├── templates/
│   └── explorer.html
└── README.md
```

## Notes

The plugin is loaded automatically by Airflow when the `plugins/` directory is mounted in the Airflow environment.
