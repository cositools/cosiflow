# Configuration

The executable configuration sources are
[`env/.env.example`](https://github.com/cositools/cosiflow/blob/dev-review/env/.env.example),
[`env/docker-compose.yaml`](https://github.com/cositools/cosiflow/blob/dev-review/env/docker-compose.yaml),
and the
[`Airflow entrypoint`](https://github.com/cositools/cosiflow/blob/dev-review/env/entrypoint-airflow.sh).
The ignored `env/.env` supplies local values to Compose; it must never be
committed.

## Required secrets

The template contains nine blank secret values:

| Variable | Consumer |
| --- | --- |
| `AIRFLOW_ADMIN_PASSWORD` | Initial/synchronized Airflow administrator |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Airflow web sessions |
| `AIRFLOW__CORE__INTERNAL_API_SECRET_KEY` | Airflow internal API |
| `AIRFLOW__CORE__FERNET_KEY` | Airflow connection and variable encryption |
| `POSTGRES_PASSWORD` | Airflow metadata database |
| `GCN_DB_PASSWORD` | GCN application database user |
| `GCN_MYSQL_ROOT_PASSWORD` | GCN MySQL initialization and administration |
| `GCN_CLIENT_ID` | GCN Kafka authentication |
| `GCN_CLIENT_SECRET` | GCN Kafka authentication |

Compose uses required-value interpolation, so blank values fail before the
affected containers are created. The Airflow entrypoint independently validates
its runtime secrets and never prints them. Airflow web, internal API, and
Fernet keys must be distinct.

## Local ports

| Variable | Default | Binding |
| --- | --- | --- |
| `AIRFLOW_WEBUI_PORT` | `8080` | `127.0.0.1` host to Airflow `8080` |
| `MAILHOG_WEBUI_PORT` | `8025` | `127.0.0.1` host to MailHog `8025` |

MailHog SMTP is also bound to `127.0.0.1:1025`. PostgreSQL and GCN MySQL have
no published host port. Changing `HOST_IP` does not change the Compose bind
address; the checked-in value is used to construct UI links inside the runtime.

## Data paths

| Variable | Container default | Purpose |
| --- | --- | --- |
| `COSI_DATA_DIR` | `/home/gamma/workspace/data` | Root presented by Data Explorer |
| `COSI_INPUT_DIR` | `/home/gamma/workspace/data/input` | General input domain |
| `COSI_LOG_DIR` | `/home/gamma/workspace/log` | Pipeline-configurable log path |
| `COSI_OBS_DIR` | `/home/gamma/workspace/data/obs` | Observation data |
| `COSI_TRANSIENT_DIR` | `/home/gamma/workspace/data/transient` | Transient products |
| `COSI_TRIGGER_DIR` | `/home/gamma/workspace/data/tdrss` | Trigger data |
| `COSI_MAPS_DIR` | `/home/gamma/workspace/data/maps` | Map products |
| `COSI_SOURCE_DIR` | `/home/gamma/workspace/data/source` | Source products |

Compose bind-mounts host `data/heasarc` at the `COSI_DATA_DIR` default and host
`data/logs` at Airflow's `/home/gamma/airflow/logs`. The similarly named
`COSI_LOG_DIR` is a separate environment contract and is not that Airflow log
mount.

## GCN safety defaults

The checked-in defaults consume the configured inbound topics but keep outbound
publication conservative:

```text
GCN_PRODUCER_ENABLED=false
GCN_DRY_RUN=true
GCN_REQUIRE_TEST_TOPICS=true
```

The allowlist contains only COSI test topics. Do not enable real publication
without the producer authorization and review described in the
[GCN client reference](../reference/gcn-client.md#publishing-outside-dry-run-mode).

## Credential rotation

Do not copy internal security-review files into public documentation. The
public rotation contract is the checked-in
[`env/rotate-local-databases.sh`](https://github.com/cositools/cosiflow/blob/dev-review/env/rotate-local-databases.sh)
script:

1. Create and verify restorable backups of both database directories.
2. Stop the Compose stack.
3. Put newly generated database credentials in the protected `env/.env` file.
4. Supply the previous PostgreSQL, GCN application, and GCN root passwords as
   `OLD_POSTGRES_PASSWORD`, `OLD_GCN_DB_PASSWORD`, and
   `OLD_GCN_MYSQL_ROOT_PASSWORD` through a protected shell or secret runner.
5. From `cosiflow/env`, run `./rotate-local-databases.sh --backup-confirmed`.
6. Restart the stack and verify service health and application access.

The script refuses to run without explicit backup confirmation. It updates all
three database principals, checks that each new credential works, and checks
that each old credential is rejected. It does not create or verify the backup
for you. Rotate Airflow session, internal API, and Fernet keys under a separate
maintenance plan because changing them can invalidate sessions or require
re-encryption of stored data.
