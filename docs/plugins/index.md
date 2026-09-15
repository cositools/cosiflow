# Plugins and access control

COSIflow installs its Airflow plugins from the repository `plugins/` directory.
Every filesystem, database, state, or process action is protected by an explicit
Airflow Auth Manager custom-view permission. Menu visibility is supplementary;
the route check is authoritative.

## Plugin catalog

| Plugin | Menu | Main route | Required capability |
| --- | --- | --- | --- |
| [Data Explorer](data-explorer.md) | **HEASARC Explorer** | `/heasarcbrowser/` | `can_read / COSIflow Scientific Data` |
| [Explore Notices](explore-notices.md) | **GCN Notices Explorer** | `/explore-notices/` | read or create capability for the requested action |
| [MailHog link](mailhog.md) | **Develop Tools → Mailhog** | `/mailhog/` | `can_read / COSIflow Mail Sandbox` |
| [Refresh DAGs](refresh-dags.md) | **Develop Tools → Refresh DAGs List** | `/refresh_dags/` | `can_edit / COSIflow DAG Catalog` |
| [Reset COSIDAG](reset-cosidag.md) | **Develop Tools → Reset Cosidag** | `/reset_cosidag/` | read or edit `COSIflow COSIDAG State` |

## Managed roles

| Permission | Viewer | Scientist | Operator | Admin |
| --- | :---: | :---: | :---: | :---: |
| Read GCN notices | No | Yes | Yes | FAB Admin |
| Inject GCN inbox/outbox records | No | No | Yes | FAB Admin |
| Browse scientific data | No | Yes | Yes | FAB Admin |
| Read COSIDAG state | No | Yes | Yes | FAB Admin |
| Edit COSIDAG state | No | No | Yes | FAB Admin |
| Refresh the DAG catalog | No | No | Yes | FAB Admin |
| Open the MailHog link | No | No | Yes | FAB Admin |

During `airflow-init`,
[`env/configure_rbac.py`](https://github.com/cositools/cosiflow/blob/dev-review/env/configure_rbac.py)
extends the current Airflow Viewer permissions for Scientist and the current Op
permissions for Operator. It reconciles only managed `COSIflow *` permissions
and menu entries; it does not rewrite unrelated custom permissions. A mismatch
fails initialization.

The canonical permission and route manifest is
[`plugins/shared_auth/__init__.py`](https://github.com/cositools/cosiflow/blob/dev-review/plugins/shared_auth/__init__.py).
