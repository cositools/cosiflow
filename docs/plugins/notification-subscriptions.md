# Notification subscriptions

The **Develop Tools → Notification Subscriptions** page manages COSIflow email
routing without a tracked recipient file or an image rebuild. Subscriptions
reference existing Airflow users and are stored in a separate COSIflow table in
the Airflow PostgreSQL database; Airflow-managed user tables are not extended.

## Access and interface

Operator and Admin users can list and change subscriptions. Every route checks
the shared COSIflow authorization manifest; menu visibility alone is not an
authorization control. Save, enable/disable, and delete operations use POST,
CSRF protection, and mutation audit logging.

Only active users with a valid single email address can be selected. Deleting
an Airflow user cascades to that user's subscriptions.

## Events and filters

The supported events are:

- `task_failure`, `task_retry`, and `task_success`;
- `dag_failure` and `dag_success`.

Each subscription has optional DAG, task, and operator glob patterns. An empty
pattern is normalized to `*`; all three patterns must match. Matching is
case-sensitive and recipient addresses are deterministically deduplicated.

Active Admin users receive task- and DAG-failure subscriptions during initial
reconciliation. Retry and success delivery is opt-in. In particular, success
callbacks are installed but send nothing until an administrator enables a
matching subscription. Scheduling, queued, and started notifications are out
of scope.

## Failure-safe delivery

Task emails use Airflow's current `TaskInstance.log_url`, `try_number`, and
`map_index`. The log preview is resolved through Airflow's file task handler,
confined below `base_log_folder`, and read from the end with byte and line
limits. Invalid or unavailable URLs and logs degrade to plain explanatory text.
All dynamic subject and HTML values are escaped.

The callback catches context, database, log, rendering, and SMTP errors so a
notification failure cannot mask the original task error. When PostgreSQL
recipient lookup fails, `COSIFLOW_ALERT_FALLBACK_RECIPIENTS` may supply a
deployment-only fallback. If that list is empty or invalid, delivery is safely
skipped. Recipient addresses are not written to callback logs.

## Deployment settings

Set `AIRFLOW_PUBLIC_BASE_URL` to an `http` or `https` URL reachable by email
recipients. The Compose default is local-only and must be overridden for a
shared deployment. Tune previews with `COSIFLOW_ALERT_LOG_TAIL_LINES` and
`COSIFLOW_ALERT_LOG_TAIL_BYTES`; the callback enforces built-in upper bounds.

The entrypoint applies `env/migrations/002_notification_subscriptions.sql` with
a checksum ledger, then idempotently seeds Admin failure subscriptions after
Airflow RBAC initialization.
