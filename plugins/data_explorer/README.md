# Data Explorer Plugin

This plugin adds an Airflow UI page for browsing files under the configured COSI data directory.

## Menu Entry

The plugin registers this top-level Airflow menu item:

```text
HEASARC Explorer
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
* Requires `can_read` on `COSIflow Scientific Data` for browsing, preview,
  inline images and download. Scientist, Operator and Admin receive this
  permission; Viewer does not.
* Supports file download.
* Provides zoom controls for image previews and opens a full-size image in a
  separate tab on double-click.
* Supports preview metadata and content handling for common text, image, FITS, HDF5, CSV, YAML, JSON, and archive extensions.
* Prevents navigation outside the configured data root.

Authorization runs before every filesystem operation. `_resolve_data_path`
then independently resolves symlinks and rejects traversal outside
`COSI_DATA_DIR` with HTTP 403.

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
