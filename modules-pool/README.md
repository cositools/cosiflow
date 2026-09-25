# COSIflow module pool

Place trusted COSIflow module directories here before using
`env/hot_load_module.sh`. The base Compose stack mounts this directory read-only
at `/home/gamma/airflow/modules_pool` and does not expose the parent workspace.

Do not store credentials, `.env` files, database dumps, or unrelated projects
in this directory.
