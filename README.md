# COSIflow

COSIflow is an Apache Airflow environment for orchestrating and monitoring COSI
scientific pipelines. This repository owns the core runtime, the `COSIDAG`
framework, callbacks, Airflow UI plugins, and the GCN inbox/outbox client.

## Documentation

- [COSIflow documentation](docs/index.md)
- [Installation](docs/getting-started/installation.md)
- [Configuration and credential handling](docs/getting-started/configuration.md)
- [COSIDAG reference](docs/reference/cosidag.md)
- [FasTP repository and documentation](https://github.com/cositools/fast-transient-analysis-pipeline)

## Quick start

Install Docker Engine or Docker Desktop with Docker Compose v2, then prepare the
local-only environment file:

```bash
cd env
cp .env.example .env
python3 bootstrap-secrets.py
chmod 600 .env
```

Add the required `GCN_CLIENT_ID` and `GCN_CLIENT_SECRET` values to `.env`, then
start the stack:

```bash
docker compose build
docker compose up -d
docker compose ps
```

Airflow is published only on loopback at `http://127.0.0.1:8080` by default.
See the installation guide before exposing COSIflow outside a development host.

## Documentation checks

Documentation dependencies are separate from application dependencies:

```bash
python3 -m pip install -r requirements-docs.txt
sh scripts/check-docs.sh
```

The check performs a strict MkDocs build and validates internal and external
links. The workflow validates documentation changes but does not publish a
website. Public hosting requires separate project approval.

## License

See [LICENSE](LICENSE).
