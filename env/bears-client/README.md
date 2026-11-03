# BEARS client placeholder

Compose mounts this directory read-only at `/home/gamma/airflow/bears-client` when
`COSIFLOW_BEARS_CLIENT_HOST_DIR` is not set. It intentionally contains no client,
so BEARS task messages are a logged no-op.

To enable BEARS, set `COSIFLOW_BEARS_CLIENT_HOST_DIR` in `env/.env` to a checkout of SSL's `bears.py` repository (the directory containing `bears.py`) and set the `COSI_BEARS_*` values. See `docs/getting-started/configuration.md`.
