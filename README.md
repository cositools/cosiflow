# COSIflow

COSIflow is an Apache Airflow environment for orchestrating and monitoring COSI
scientific pipelines. This repository provides the core runtime, the `COSIDAG`
framework, callbacks, UI plugins, and the GCN inbox/outbox client. Scientific
pipelines are installed as adjacent modules, such as
`fast-transient-analysis-pipeline` (FasTP).

For a faster COSIflow setup, or for an example that automates the installation
of both COSIflow and the FasTP module, follow the
[FasTP installation guide](https://github.com/cositools/fast-transient-analysis-pipeline/tree/dev#installation).

## Quick start

### Prerequisites

* Install Docker Engine or Docker Desktop with Docker Compose v2
* Clone this repository into your home directory that can also contain pipeline modules:

```text
~/cosi/
├── cosiflow/
└── fast-transient-analysis-pipeline/   # optional module
```

Run all Compose commands from `cosiflow/env`.

### 1. Configure the host identity and public endpoints

Edit `env/docker-compose.yaml`.

Set the build arguments under `x-build-args` to the UID and GID returned by:

```bash
id -u
id -g
```

For example:

```yaml
x-build-args: &build-args
  UID: ${UID:-501}
  GID: ${GID:-20}
```

Review these endpoint values:

```yaml
x-host-ip: &host-ip "127.0.0.1"

x-common-env: &common-env
  MAILHOG_WEBUI_PORT: ${MAILHOG_WEBUI_PORT:-8025}
  AIRFLOW_WEBUI_PORT: ${AIRFLOW_WEBUI_PORT:-8080}
```

`AIRFLOW_WEBUI_PORT` and `MAILHOG_WEBUI_PORT` control the host-side Docker port
mapping as well as the URLs displayed by COSIflow. The services continue to
listen on ports 8080 and 8025 inside their containers.

### 2. Store secrets in `.env`

Non-sensitive defaults live in `docker-compose.yaml`. Put local passwords and
GCN Kafka credentials in `cosiflow/env/.env`, which is excluded by `.gitignore`:

```dotenv
AIRFLOW_ADMIN_PASSWORD=<airflow-admin-password>
POSTGRES_PASSWORD=<airflow-database-password>
GCN_DB_PASSWORD=<gcn-database-password>
GCN_MYSQL_ROOT_PASSWORD=<gcn-root-password>

# Optional: required only for authenticated GCN Kafka consumption.
GCN_CLIENT_ID=<your-gcn-client-id>
GCN_CLIENT_SECRET=<your-gcn-client-secret>
```

Restrict access to the file:

```bash
cd cosiflow/env
chmod 600 .env
```

Never commit `.env` or copy credentials into `docker-compose.yaml`. Without GCN
credentials, the local database, manual injection, and dry-run outbox remain
usable; the Kafka receiver stays idle.

### 3. Prepare persistent directories

From the `cosiflow` directory:

```bash
mkdir -p data/postgres_data data/gcn_mysql_data data/heasarc data/logs
```

These are bind-mounted host directories. They remain on disk after containers
are stopped or removed.

### 4. Build and start COSIflow

```bash
cd env
docker compose build
docker compose up -d
docker compose ps
```

Follow the Airflow logs with:

```bash
docker compose logs -f airflow
```

Open:

- Airflow: `http://<HOST_IP>:<AIRFLOW_WEBUI_PORT>/home`
- MailHog: `http://<HOST_IP>:<MAILHOG_WEBUI_PORT>`

The default Airflow username is `admin`; the password is the value of
`AIRFLOW_ADMIN_PASSWORD`.

Open a shell in the Airflow container with:

```bash
docker compose exec airflow bash
```

### 5. Stop the stack

Stop and remove containers and the Compose network:

```bash
docker compose down
```

`docker compose down -v` also removes Compose-managed named volumes, but it does
not delete the bind-mounted directories under `cosiflow/data`.

## Main configuration

| Variable or anchor | Purpose |
| --- | --- |
| `UID`, `GID` | Identity used to build the non-root `gamma` user |
| `x-host-ip` / `HOST_IP` | Host address used for published service URLs |
| `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_EMAIL`, `AIRFLOW_ADMIN_PASSWORD` | Initial Airflow administrator |
| `AIRFLOW_WEBUI_PORT` | Host port published for the Airflow UI |
| `MAILHOG_WEBUI_PORT` | Host port published for the MailHog UI |
| `POSTGRES_USER`, `POSTGRES_DB`, `POSTGRES_PASSWORD` | Airflow metadata database |
| `HOST_WORKSPACE_PATH` | Host workspace mounted into `DockerOperator` containers |
| `HOST_DATA_PATH` | Host equivalent of the shared COSI data root |
| `COSI_DATA_DIR`, `COSI_INPUT_DIR`, `COSI_LOG_DIR` | Main container data paths |
| `COSI_OBS_DIR`, `COSI_TRANSIENT_DIR`, `COSI_TRIGGER_DIR`, `COSI_MAPS_DIR`, `COSI_SOURCE_DIR` | Canonical COSI data domains |
| `GCN_DB_*` | MySQL connection for the GCN inbox/outbox |
| `GCN_CLIENT_ID`, `GCN_CLIENT_SECRET`, `GCN_CONSUMER_*` | GCN Kafka consumer |
| `GCN_PRODUCER_ENABLED`, `GCN_DRY_RUN`, `GCN_TOPIC_ALLOWLIST` | Outbound safety controls |

The checked-in producer defaults are deliberately conservative:
`GCN_PRODUCER_ENABLED=false`, `GCN_DRY_RUN=true`, and test topics only.

## Installing scientific modules

Modules live beside `cosiflow/` and are linked into the running Airflow
container by `env/hot_load_module.sh`:

```bash
cd cosiflow/env
./hot_load_module.sh <module-directory> install
```

Use `update` after changing dependencies or runtime configuration and `remove`
to unload a module. The complete module format and lifecycle are documented in
[env/README.md](env/README.md).

## COSIDAG

`COSIDAG` is an Airflow `DAG` subclass for filesystem-driven scientific
workflows. It can monitor new folders or files, reject already processed paths,
resolve input files, run a custom task graph, retrigger itself, and publish a
final result link.

The implementation and developer contract are documented in
[modules/README.md](modules/README.md).

## DAGs

The core repository keeps `dags/` as the Airflow mount point but does not ship
production scientific DAG Python files. Installed modules expose their DAG
directories there through `.cfmodule` symlinks.

See [dags/README.md](dags/README.md) for the core contract and the module's own
DAG catalog for its current workflow IDs.

## GCN client

The `gcn-client` and `gcn-mysql` services provide a durable inbound notice
database and outbound queue. Airflow tasks interact with MySQL rather than
opening Kafka connections directly.

Architecture, schema, commands, dry-run behavior, and local test procedures are
documented in [gcn-client/README.md](gcn-client/README.md).

## Tests

The configurable COSIDAG benchmark and chart generator are documented in
[test/README.md](test/README.md).
