from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _csv_env(name: str, default: str = "") -> list[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


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


def load_settings() -> Settings:
    schema_root = Path(os.getenv("GCN_SCHEMA_ROOT", "/app/gcn-schema"))
    cosi_alert_schema = Path(
        os.getenv(
            "GCN_COSI_ALERT_SCHEMA",
            str(schema_root / "gcn/notices/cosi/alert.schema.json"),
        )
    )

    return Settings(
        db_host=os.getenv("GCN_DB_HOST", "gcn-mysql"),
        db_port=int(os.getenv("GCN_DB_PORT", "3306")),
        db_name=os.getenv("GCN_DB_NAME", "gcn"),
        db_user=os.getenv("GCN_DB_USER", "gcn_user"),
        db_password=os.getenv("GCN_DB_PASSWORD", "gcn_password"),
        db_connect_timeout=int(os.getenv("GCN_DB_CONNECT_TIMEOUT", "10")),
        client_id=os.getenv("GCN_CLIENT_ID", ""),
        client_secret=os.getenv("GCN_CLIENT_SECRET", ""),
        gcn_domain=os.getenv("GCN_DOMAIN") or None,
        consumer_group_id=os.getenv("GCN_CONSUMER_GROUP_ID", "cosiflow-gcn-client"),
        consumer_topics=_csv_env("GCN_CONSUMER_TOPICS", ""),
        consumer_enabled=_bool_env("GCN_CONSUMER_ENABLED", True),
        consumer_poll_timeout=float(os.getenv("GCN_CONSUMER_POLL_TIMEOUT", "1.0")),
        consumer_commit=_bool_env("GCN_CONSUMER_COMMIT", True),
        producer_enabled=_bool_env("GCN_PRODUCER_ENABLED", False),
        dry_run=_bool_env("GCN_DRY_RUN", True),
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
    )
