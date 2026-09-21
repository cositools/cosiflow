from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class ConfigurationError(ValueError):
    """Raised before any network operation when required configuration is absent."""


def _required_env(name: str) -> str:
    value = os.getenv(name, "")
    if not value:
        raise ConfigurationError(f"{name} is required and must not be empty")
    return value


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _csv_env(name: str, default: str = "") -> list[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _positive_float_env(name: str, default: str) -> float:
    value = float(os.getenv(name, default))
    if value <= 0:
        raise ConfigurationError(f"{name} must be positive")
    return value


def _positive_int_env(name: str, default: str) -> int:
    value = int(os.getenv(name, default))
    if value <= 0:
        raise ConfigurationError(f"{name} must be positive")
    return value


def _jitter_env(name: str, default: str) -> float:
    value = float(os.getenv(name, default))
    if not 0.0 <= value <= 1.0:
        raise ConfigurationError(f"{name} must be between 0 and 1")
    return value


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_connect_timeout: int

    client_id: str
    client_secret: str
    gcn_domain: str | None
    consumer_group_id: str
    consumer_topics: list[str]
    consumer_enabled: bool
    consumer_poll_timeout: float
    consumer_commit: bool

    producer_enabled: bool
    dry_run: bool
    require_test_topics: bool
    outbound_topic_default: str
    topic_allowlist: list[str]
    publish_poll_seconds: float
    publish_batch_size: int
    max_attempts: int
    producer_client_label: str

    schema_root: Path
    cosi_alert_schema: Path
    init_db_on_start: bool
    log_level: str

    worker_backoff_initial_seconds: float
    worker_backoff_max_seconds: float
    worker_backoff_jitter_ratio: float
    worker_failure_budget: int
    outbox_retry_initial_seconds: float
    outbox_retry_max_seconds: float
    outbox_lock_timeout_seconds: int
    heartbeat_degraded_seconds: float
    heartbeat_offline_seconds: float
    watchdog_interval_seconds: float
    watchdog_start_grace_seconds: float


def load_settings() -> Settings:
    schema_root = Path(os.getenv("GCN_SCHEMA_ROOT", "/app/gcn-schema"))
    cosi_alert_schema = Path(
        os.getenv(
            "GCN_COSI_ALERT_SCHEMA",
            str(schema_root / "gcn/notices/cosi/alert.schema.json"),
        )
    )

    consumer_enabled = _bool_env("GCN_CONSUMER_ENABLED", True)
    producer_enabled = _bool_env("GCN_PRODUCER_ENABLED", False)
    dry_run = _bool_env("GCN_DRY_RUN", True)
    client_id = os.getenv("GCN_CLIENT_ID", "")
    client_secret = os.getenv("GCN_CLIENT_SECRET", "")
    if consumer_enabled or (producer_enabled and not dry_run):
        client_id = _required_env("GCN_CLIENT_ID")
        client_secret = _required_env("GCN_CLIENT_SECRET")

    worker_backoff_initial_seconds = _positive_float_env(
        "GCN_WORKER_BACKOFF_INITIAL_SECONDS", "1"
    )
    worker_backoff_max_seconds = _positive_float_env(
        "GCN_WORKER_BACKOFF_MAX_SECONDS", "30"
    )
    if worker_backoff_max_seconds < worker_backoff_initial_seconds:
        raise ConfigurationError(
            "GCN_WORKER_BACKOFF_MAX_SECONDS must be greater than or equal to "
            "GCN_WORKER_BACKOFF_INITIAL_SECONDS"
        )

    outbox_retry_initial_seconds = _positive_float_env(
        "GCN_OUTBOX_RETRY_INITIAL_SECONDS", "5"
    )
    outbox_retry_max_seconds = _positive_float_env(
        "GCN_OUTBOX_RETRY_MAX_SECONDS", "300"
    )
    if outbox_retry_max_seconds < outbox_retry_initial_seconds:
        raise ConfigurationError(
            "GCN_OUTBOX_RETRY_MAX_SECONDS must be greater than or equal to "
            "GCN_OUTBOX_RETRY_INITIAL_SECONDS"
        )

    heartbeat_degraded_seconds = _positive_float_env(
        "GCN_HEARTBEAT_DEGRADED_SECONDS", "30"
    )
    heartbeat_offline_seconds = _positive_float_env(
        "GCN_HEARTBEAT_OFFLINE_SECONDS", "90"
    )
    if heartbeat_offline_seconds <= heartbeat_degraded_seconds:
        raise ConfigurationError(
            "GCN_HEARTBEAT_OFFLINE_SECONDS must be greater than "
            "GCN_HEARTBEAT_DEGRADED_SECONDS"
        )

    return Settings(
        db_host=os.getenv("GCN_DB_HOST", "gcn-mysql"),
        db_port=int(os.getenv("GCN_DB_PORT", "3306")),
        db_name=os.getenv("GCN_DB_NAME", "gcn"),
        db_user=os.getenv("GCN_DB_USER", "gcn_user"),
        db_password=_required_env("GCN_DB_PASSWORD"),
        db_connect_timeout=int(os.getenv("GCN_DB_CONNECT_TIMEOUT", "10")),
        client_id=client_id,
        client_secret=client_secret,
        gcn_domain=os.getenv("GCN_DOMAIN") or None,
        consumer_group_id=os.getenv("GCN_CONSUMER_GROUP_ID", "cosiflow-gcn-client"),
        consumer_topics=_csv_env("GCN_CONSUMER_TOPICS", ""),
        consumer_enabled=consumer_enabled,
        consumer_poll_timeout=float(os.getenv("GCN_CONSUMER_POLL_TIMEOUT", "1.0")),
        consumer_commit=_bool_env("GCN_CONSUMER_COMMIT", True),
        producer_enabled=producer_enabled,
        dry_run=dry_run,
        require_test_topics=_bool_env("GCN_REQUIRE_TEST_TOPICS", True),
        outbound_topic_default=os.getenv(
            "GCN_OUTBOUND_TOPIC_DEFAULT",
            "gcn.notices.cosi.test.alert",
        ),
        topic_allowlist=_csv_env(
            "GCN_TOPIC_ALLOWLIST",
            "gcn.notices.cosi.test.alert,gcn.notices.cosi.bgo.test.alert,gcn.notices.cosi.ged.test.alert",
        ),
        publish_poll_seconds=float(os.getenv("GCN_PUBLISH_POLL_SECONDS", "5")),
        publish_batch_size=int(os.getenv("GCN_PUBLISH_BATCH_SIZE", "10")),
        max_attempts=int(os.getenv("GCN_MAX_ATTEMPTS", "3")),
        producer_client_label=os.getenv("GCN_PRODUCER_CLIENT_LABEL", "cosiflow-gcn-client"),
        schema_root=schema_root,
        cosi_alert_schema=cosi_alert_schema,
        init_db_on_start=_bool_env("GCN_INIT_DB_ON_START", True),
        log_level=os.getenv("GCN_LOG_LEVEL", "INFO"),
        worker_backoff_initial_seconds=worker_backoff_initial_seconds,
        worker_backoff_max_seconds=worker_backoff_max_seconds,
        worker_backoff_jitter_ratio=_jitter_env(
            "GCN_WORKER_BACKOFF_JITTER_RATIO", "0.2"
        ),
        worker_failure_budget=_positive_int_env("GCN_WORKER_FAILURE_BUDGET", "5"),
        outbox_retry_initial_seconds=outbox_retry_initial_seconds,
        outbox_retry_max_seconds=outbox_retry_max_seconds,
        outbox_lock_timeout_seconds=_positive_int_env(
            "GCN_OUTBOX_LOCK_TIMEOUT_SECONDS", "300"
        ),
        heartbeat_degraded_seconds=heartbeat_degraded_seconds,
        heartbeat_offline_seconds=heartbeat_offline_seconds,
        watchdog_interval_seconds=_positive_float_env(
            "GCN_WATCHDOG_INTERVAL_SECONDS", "5"
        ),
        watchdog_start_grace_seconds=_positive_float_env(
            "GCN_WATCHDOG_START_GRACE_SECONDS", "30"
        ),
    )
