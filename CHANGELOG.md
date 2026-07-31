# Changelog

This document records the significant changes to COSIflow.

The dates shown are the dates of the annotated tags. The history before
`v0.1.0`, which started in 2024 but did not contain version tags, is consolidated
into the first release. The **Unreleased** section describes the current `dev`
branch after `v0.2.0`; these changes are not part of a release yet.

## [Unreleased]

### Added

- Bundled a reproducible copy of the COSI and Core schemas in the GCN client
  image. A dedicated Docker build stage performs a sparse checkout of the
  `cositools/gcn-schema` repository at a pinned revision, removing the need to
  manually mount an external schema repository.
- Added shared Jinja/CSS resources used by the Airflow plugins for consistent
  page layout and styling.
- Added Graphviz to the Airflow image for optional benchmark DAG graph exports.

### Changed

- Python environment creation through `hot_load_module.sh` now always removes
  and recreates the target virtual environment. Updates therefore no longer
  reuse stale editable checkouts or outdated dependencies and produce an
  installation that is consistent with the module's current requirements.
- The Docker Compose configuration now exposes `HOST_IP` and the GCN MySQL port
  as YAML anchors that can be edited in one place; local database passwords can
  be supplied through the ignored `.env` file.
- The Airflow data volume now points to `data/heasarc`, while persistent logs
  are stored separately under `data/logs`.
- The default benchmark configuration no longer generates Airflow graphs,
  reducing additional work during performance measurements. Graph generation
  remains available in the test runner.
- Plugin menu labels and ordering now consistently expose **HEASARC Explorer**,
  **GCN Notices Explorer**, and **Develop Tools**.
- The Compose configuration now exposes `HOST_DATA_PATH` for DockerOperator
  mounts and publishes the Airflow and MailHog UIs through their configured host
  port variables.
- Local passwords and GCN Kafka credentials are documented as `.env` values;
  tracked Compose files retain only non-personal defaults.
- Reworked the main, module-development, and COSIDAG guides against the current
  implementation, including the exact glob/regex and runtime-override contracts.

### Fixed

- The final COSIDAG `show_results` task now uses `ALL_SUCCESS`. It runs only
  when all upstream dependencies have succeeded and no longer presents a
  pipeline containing failed tasks as completed.
- The MailHog redirect and DAG refresh plugin routes now explicitly require an
  authenticated Airflow session.

## [v0.2.0] - 2026-07-14

_Annotated tag: “Add GCN client with MySQL database + plugin”._

This release introduces the first end-to-end prototype for receiving, storing,
inspecting, and publishing GCN notices, together with a benchmarking system for
COSIDAG workflows.

### Added

- Added a new `gcn-client` service with a GCN Kafka consumer and producer. The
  client receives notices from the configured topics, stores their payloads and
  metadata, and maintains separate heartbeats for the inbound and outbox
  components.
- Added a MySQL 8.4 database, independent of the PostgreSQL database used by
  Airflow, with dedicated tables for inbound notices, the outbound queue,
  delivery attempts, and client status. The schema includes indexes, checksums,
  idempotency keys, and constraints that prevent duplicate Kafka messages.
- Added parsing and normalization for JSON, VOEvent, and textual/raw notices,
  including extraction of the main scientific metadata and validation against
  the COSI schema.
- Added an outbox workflow with queueing, locking, priority handling, a maximum
  attempt count, and detailed records for every publication attempt.
- Added conservative producer safeguards: publishing is disabled and dry-run
  mode is enabled by default, test topics are required, and an allowlist can be
  configured.
- Added the Airflow **Explore Notices** plugin to the results browser, providing:

  - separate inbox and outbox views;
  - multi-topic, validation-status, and content-type filters;
  - server-side pagination and automatic refresh;
  - detail pages for raw/JSON payloads, validation errors, DAG metadata, and
    delivery attempts;
  - manual injection of inbound and outbound notices;
  - GCN client service status and derived summary fields for textual GCN
    Classic notices.

- Added a configurable COSIDAG performance test suite. The runner can clean
  datasets while preserving selected files, reset processing variables,
  unpause and trigger multiple DAGs, track tasks and execution times, sample
  container resources, and save results as CSV.
- Added SVG charts for task Gantt timelines, elapsed times, memory, system load,
  CPU, and disk usage, with optional Airflow graph exports. Dedicated modes are
  also available for finalization only and chart regeneration only.

### Changed

- Extended `docker-compose.yaml` with the `gcn-mysql` and `gcn-client` services,
  their health checks, persistent storage, and the configuration variables for
  the consumer, producer, database, and schemas.
- Airflow now waits for the GCN database to become available before starting,
  allowing the Explore Notices plugin to query an initialized schema.
- Added `PyMySQL` to the Airflow environment dependencies so that the plugin can
  access the GCN database.
- Excluded generated performance test results from Git tracking.

## [v0.1.1] - 2026-07-01

_Annotated tag: “Bug fix and clean up hot-load script logs”._

This corrective release focuses on the usability and reliability of the
dynamic module-loading system.

### Changed

- Reorganized `hot_load_module.sh` into clearer sections and standardized its
  output with color-coded messages for normal operations, warnings, and errors.
- Clarified the installation, update, and removal steps for links, Python
  environments, and Docker images, including their summaries.
- Reorganized `Dockerfile.airflow`: removed the global configuration that
  redirected `python3` and `python` to Python 3.11, kept the Airflow environment
  explicitly based on Python 3.9, and documented a possible Python 3.10 build
  for reference.

### Fixed

- Centralized fatal error handling in the hot-loading script. Missing
  configuration, environment creation, dependency installation, module linking,
  and Docker build failures now terminate with an explicit error and a non-zero
  exit code.

## [v0.1.0] - 2026-06-30

_Annotated tag: “v0.1.0: Cosiflow module runtime baseline”._

The first versioned baseline of the COSIflow environment. This tag consolidates
the project's earlier history and defines the modular runtime used as the
foundation for subsequent releases.

### Added

- Added an orchestration environment based on Apache Airflow 2.10.3 and Docker
  Compose, with PostgreSQL for metadata, MailHog for local notifications, and a
  Docker socket proxy for controlled container execution.
- Added an Oracle Linux 8 Airflow image with a Python 3.9 runtime and Python 3.11
  and 3.12 interpreters for external module environments, designed for both
  `amd64` and `arm64`.
- Added the `COSIDAG` framework for reactive scientific pipelines, including:

  - folder- or file-based monitoring and input stability checks;
  - time filters, readiness markers, and candidate selection;
  - optional automatic retriggering with a configurable run limit;
  - pattern-based input resolution and XCom value propagation;
  - tracking of processed paths through Airflow Variables;
  - custom tasks and final result linking.

- Added the `hot_load_module.sh` manager for installing, updating, and removing
  modules without rebuilding the entire environment. It supports dynamic DAG
  and pipeline links, Docker images, one or more virtual environments, and YAML
  configurations with `container`, `environment`, `both`, and `none` modes.
- Added bootstrapping of `pip`, `setuptools`, and `wheel` when creating virtual
  environments, including Python 3.12 support and additional requirement files
  installed with `--no-deps`.
- Added Airflow plugins for browsing COSI data, opening MailHog messages from
  task errors, refreshing the DAG list, and selectively resetting paths already
  processed by COSIDAG.
- Added failure-notification callbacks and local SMTP configuration through
  MailHog.
- Added documentation for configuring the environment, developing COSIDAGs,
  and creating, installing, or updating scientific modules.

### Changed

- Separated the orchestration runtime from scientific pipeline code. The core
  repository provides the environment, framework, callbacks, and plugins, while
  production DAGs are distributed through adjacent module repositories such as
  `fast-transient-analysis-pipeline`.
- Centralized user, port, credential, COSI directory, and service configuration
  in `docker-compose.yaml`; this baseline does not require a `.env` file.

[Unreleased]: https://github.com/cositools/cosiflow/compare/v0.2.0...dev
[v0.2.0]: https://github.com/cositools/cosiflow/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/cositools/cosiflow/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/cositools/cosiflow/tree/v0.1.0
