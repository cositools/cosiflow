# Installation

## Prerequisites

- Docker Engine or Docker Desktop with Docker Compose v2.
- A Unix-like host shell for the checked-in helper scripts.
- GCN Kafka client credentials for the configured consumer. Compose requires
  these values before creating the services.

Clone the repository into a writable workspace. No fixed home-directory path
is required for the core stack.

## Configure the environment

Create the ignored environment file from its non-sensitive template:

```bash
cd cosiflow/env
cp .env.example .env
python3 bootstrap-secrets.py
chmod 600 .env
```

`bootstrap-secrets.py` generates local Airflow and database values without
printing them. Add the assigned GCN credentials to the two remaining fields:

```dotenv
GCN_CLIENT_ID=<your-gcn-client-id>
GCN_CLIENT_SECRET=<your-gcn-client-secret>
```

See [Configuration](configuration.md) for the complete contract and credential
rotation guidance.

## Prepare persistent directories

From the repository root:

```bash
mkdir -p data/postgres_data data/gcn_mysql_data data/heasarc data/logs
```

These directories are bind-mounted into the containers. They remain on disk
after `docker compose down` and are not removed by `docker compose down -v`.

## Build and start

Run Compose from `cosiflow/env`:

```bash
docker compose build
docker compose up -d
docker compose ps
```

Startup is ordered as follows:

1. PostgreSQL and GCN MySQL pass their health checks.
2. `airflow-init` validates required runtime values, migrates the Airflow
   database, re-encrypts supported Airflow secrets, synchronizes permissions,
   reconciles the administrator, and provisions COSIflow roles.
3. The Airflow webserver and scheduler start only after initialization succeeds.
4. The GCN client starts after its MySQL database is healthy.

Follow Airflow logs with:

```bash
docker compose logs -f airflow
```

The default local endpoints are:

- Airflow: `http://127.0.0.1:8080/home`
- MailHog: `http://127.0.0.1:8025`

The initial Airflow username defaults to `admin`; its password is the value of
`AIRFLOW_ADMIN_PASSWORD` in the ignored `.env` file.

Open a shell in the Airflow container with:

```bash
docker compose exec airflow bash
```

## Stop the stack

```bash
docker compose down
```

The checked-in Compose definition is for local development. It binds user
interfaces to loopback and does not publish either database. Shared deployments
must put an authenticated HTTPS reverse proxy in front of the UI and provide
equivalent configuration through a managed secret system.

## Next step

Read [COSIflow modules](../architecture/modules.md) before installing FasTP or
another scientific pipeline.
