# Explore Notices Airflow Plugin

The Explore Notices plugin adds an Airflow AppBuilder page for inspecting GCN notices stored by the COSIflow GCN client in MySQL.

It is registered as the authenticated top-level menu item
**GCN Notices Explorer** and exposes inbox/outbox list and detail routes:

- `/explore-notices/?tab=inbox`: paginated notice inbox with filters and client heartbeat status.
- `/explore-notices/?tab=outbox`: paginated outbound notice outbox with delivery status and attempt summary.
- `/explore-notices/notice/<id>`: full inbox notice detail view with raw payload, JSON payload, validation errors, and derived summary fields.
- `/explore-notices/outbox/<id>`: full outbound notice detail view with payload JSON, validation errors, source DAG metadata, and delivery attempts.

All exposed routes require an authenticated Airflow session.

## Data Source

The plugin reads from the GCN client tables:

- `gcn_inbound_notices`
- `gcn_outbound_notices`
- `gcn_delivery_attempts`
- `gcn_client_heartbeats`

Database connection settings are read from the same environment variables used by the GCN client:

- `GCN_DB_HOST`
- `GCN_DB_PORT`
- `GCN_DB_NAME`
- `GCN_DB_USER`
- `GCN_DB_PASSWORD`
- `GCN_DB_CONNECT_TIMEOUT`

## Inbox Notice List

The list view supports:

- topic filtering with a chip/tag selector;
- validation status filtering;
- content type filtering;
- configurable page size through the `limit` query parameter;
- server-side pagination through the `page` query parameter.

Selected topics are serialized as repeated query parameters:

```text
/explore-notices/?topic=gcn.classic.text.FERMI_GBM_POS_TEST&topic=gcn.classic.text.SWIFT_BAT_GRB_POS_TEST&limit=15&page=1
```

The backend preserves all active filters when building pagination links.

## Derived Display Fields

Some classic GCN text notices are stored as `raw_only` because they are not JSON and may fail VOEvent parsing. For those notices, important table columns would otherwise be empty.

To keep the UI useful without requiring a database backfill, the plugin derives display-only fields from `raw_payload` at read time:

- `Mission` and `Instrument` from the topic suffix, for example `FERMI_GBM`.
- `Event / Trigger` from `NOTICE_TYPE` and `TRIGGER_NUM`.
- `Time` from `GRB_DATE` and `GRB_TIME`.
- `Position` from `GRB_RA` and `GRB_DEC`.

These derived values are only used for display. They do not update rows in `gcn_inbound_notices`.

## Outbox Notice List

The outbox tab shows notices written by COSIflow DAG tasks before they are handed to the GCN producer. It highlights the fields used for operational checks:

- `status`: `queued`, `locked`, `dry_run_published`, `published`, `failed`, `invalid`, or `cancelled`.
- `attempts_count` / `max_attempts`: how many publish attempts have been made.
- `last_error`: the latest validation, safety, or producer failure. Empty means no current failure.
- `published_at`: when the worker finished a dry-run or real publish attempt.
- `dag_run_id`, `created_by_dag_id`, and `task_id`: the Airflow source of the outbound notice.

For local prototype runs, `dry_run_published` is the expected success state when `GCN_DRY_RUN=true` or `GCN_PRODUCER_ENABLED=false`.

## Files

- `explore_notices_plugin.py`: Flask/AppBuilder view, SQL queries, pagination, and notice display enrichment.
- `templates/explore_notices.html`: inbox/outbox tabbed list view, filters, topic chip selector, pagination controls.
- `templates/explore_notice_detail.html`: detail view for a single notice.
- `templates/explore_outbox_detail.html`: detail view for an outbound notice and its delivery attempts.

## Development Notes

When changing SQL filters, keep `_notice_where()` as the single source of truth so the list query and count query stay consistent.

When changing topic filter behavior, keep the hidden `topic` inputs in sync with selected chips; the server expects repeated `topic` query parameters.

When changing classic text parsing, treat the extracted values as best-effort display metadata. The raw payload remains the authoritative source.
