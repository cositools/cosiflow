# Explore Notices Airflow Plugin

The Explore Notices plugin adds an Airflow AppBuilder page for inspecting GCN notices stored by the COSIflow GCN client in MySQL.

It is registered under `Results Browser > Explore Notices` and exposes two views:

- `/explore-notices/`: paginated notice inbox with filters and client heartbeat status.
- `/explore-notices/notice/<id>`: full notice detail view with raw payload, JSON payload, validation errors, and derived summary fields.

## Data Source

The plugin reads from the GCN client tables:

- `gcn_inbound_notices`
- `gcn_client_heartbeats`

Database connection settings are read from the same environment variables used by the GCN client:

- `GCN_DB_HOST`
- `GCN_DB_PORT`
- `GCN_DB_NAME`
- `GCN_DB_USER`
- `GCN_DB_PASSWORD`
- `GCN_DB_CONNECT_TIMEOUT`

## Notice List

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

## Files

- `explore_notices_plugin.py`: Flask/AppBuilder view, SQL queries, pagination, and notice display enrichment.
- `templates/explore_notices.html`: list view, filters, topic chip selector, pagination controls.
- `templates/explore_notice_detail.html`: detail view for a single notice.

## Development Notes

When changing SQL filters, keep `_notice_where()` as the single source of truth so the list query and count query stay consistent.

When changing topic filter behavior, keep the hidden `topic` inputs in sync with selected chips; the server expects repeated `topic` query parameters.

When changing classic text parsing, treat the extracted values as best-effort display metadata. The raw payload remains the authoritative source.
