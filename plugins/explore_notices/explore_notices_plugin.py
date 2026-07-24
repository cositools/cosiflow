"""Airflow plugin for browsing GCN notices stored by the COSIflow GCN client."""

import hashlib
import json
import os
import re
import socket
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.parse import urlparse
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import pymysql
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, flash, redirect, request, url_for
from flask_appbuilder import BaseView, expose
from flask_login import current_user, login_required


plugin_folder = os.path.dirname(os.path.abspath(__file__))

explore_notices_bp = Blueprint(
    "explore_notices_bp",
    __name__,
    template_folder=os.path.join(plugin_folder, "templates"),
    url_prefix="/explore-notices",
)


def _connect():
    return pymysql.connect(
        host=os.environ.get("GCN_DB_HOST", "gcn-mysql"),
        port=int(os.environ.get("GCN_DB_PORT", "3306")),
        database=os.environ.get("GCN_DB_NAME", "gcn"),
        user=os.environ.get("GCN_DB_USER", "gcn_user"),
        password=os.environ.get("GCN_DB_PASSWORD", "gcn_password"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=int(os.environ.get("GCN_DB_CONNECT_TIMEOUT", "10")),
    )


def _json_pretty(value):
    if value in (None, ""):
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, sort_keys=True)
    try:
        return json.dumps(json.loads(value), indent=2, sort_keys=True)
    except Exception:
        return str(value)


def _payload_sha256(value):
    if isinstance(value, str):
        value = value.encode("utf-8")
    return hashlib.sha256(value).hexdigest()


def _canonical_json(payload):
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _json_or_none(value):
    if value in (None, ""):
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _schema_version(schema_url):
    if not schema_url:
        return None
    text = str(schema_url)
    marker = "/schema/"
    if marker not in text:
        return None
    tail = text.split(marker, 1)[1]
    return tail.split("/", 1)[0] if "/" in tail else tail


def _parse_iso_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="microseconds")
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.isoformat(sep=" ", timespec="microseconds")


def _string_or_json(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return _canonical_json(value)


def _normalize_json_notice(payload):
    event_ids = payload.get("id")
    if event_ids is not None and not isinstance(event_ids, list):
        event_ids = [event_ids]
    return {
        "schema_url": payload.get("$schema"),
        "schema_version": _schema_version(payload.get("$schema")),
        "mission": payload.get("mission"),
        "instrument": payload.get("instrument"),
        "alert_type": payload.get("alert_type"),
        "alert_tense": payload.get("alert_tense"),
        "event_name": _string_or_json(payload.get("event_name")),
        "event_ids": event_ids,
        "trigger_time": _parse_iso_datetime(payload.get("trigger_time")),
        "alert_datetime": _parse_iso_datetime(payload.get("alert_datetime")),
        "ra_deg": payload.get("ra"),
        "dec_deg": payload.get("dec"),
        "ra_dec_error_json": payload.get("ra_dec_error"),
        "healpix_url": payload.get("healpix_url"),
        "classification_json": payload.get("classification"),
        "record_number": payload.get("record_number"),
    }


def _normalize_topics(values):
    topics = []
    seen = set()
    for value in values:
        for topic in value.split(","):
            topic = topic.strip()
            if topic and topic not in seen:
                topics.append(topic)
                seen.add(topic)
    return topics


def _csv_env(name, default=""):
    raw = os.environ.get(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _configured_consumer_topics():
    return _csv_env("GCN_CONSUMER_TOPICS")


def _subscribed_topics(heartbeats):
    topics = []
    seen = set()
    for topic in _configured_consumer_topics():
        topics.append(topic)
        seen.add(topic)
    for heartbeat in heartbeats:
        details = heartbeat.get("details_json")
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except Exception:
                details = {}
        if not isinstance(details, dict):
            continue
        for topic in details.get("topics") or []:
            if topic and topic not in seen:
                topics.append(topic)
                seen.add(topic)
    return topics


def _bounded_int(value, default, minimum, maximum):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return min(max(number, minimum), maximum)


def _notice_where(topics, validation_status, content_type):
    """Build the shared WHERE clause used by both count and list queries."""
    where = []
    params = []
    if topics:
        topic_clauses = []
        for topic in topics:
            topic_clauses.append("topic = %s")
            params.append(topic)
        where.append(f"({' OR '.join(topic_clauses)})")
    if validation_status:
        where.append("validation_status = %s")
        params.append(validation_status)
    if content_type:
        where.append("content_type = %s")
        params.append(content_type)
    return f"WHERE {' AND '.join(where)}" if where else "", params


def _outbox_where(status, dag_id, task_id, topic):
    """Build the WHERE clause for outbound notice filters."""
    where = []
    params = []
    if status:
        where.append("status = %s")
        params.append(status)
    if dag_id:
        where.append("created_by_dag_id = %s")
        params.append(dag_id)
    if task_id:
        where.append("task_id = %s")
        params.append(task_id)
    if topic:
        where.append("topic = %s")
        params.append(topic)
    return f"WHERE {' AND '.join(where)}" if where else "", params


def _fetch_notice_count(topics, validation_status, content_type):
    where_sql, params = _notice_where(topics, validation_status, content_type)
    sql = f"SELECT COUNT(*) AS total FROM gcn_inbound_notices {where_sql}"
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row["total"] or 0)


def _fetch_notices(limit, offset, topics, validation_status, content_type):
    where_sql, params = _notice_where(topics, validation_status, content_type)
    sql = f"""
        SELECT
          id, received_at, source, topic, kafka_offset, content_type,
          parse_status, validation_status, mission, instrument,
          alert_type, alert_tense, event_name, trig_id, isotime,
          trigger_time, ra_deg, dec_deg, raw_payload
        FROM gcn_inbound_notices
        {where_sql}
        ORDER BY received_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            notices = cur.fetchall()
    for notice in notices:
        _enrich_notice_display(notice)
    return notices


def _fetch_outbound_notice_count(status, dag_id, task_id, topic):
    where_sql, params = _outbox_where(status, dag_id, task_id, topic)
    sql = f"SELECT COUNT(*) AS total FROM gcn_outbound_notices {where_sql}"
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row["total"] or 0)


def _fetch_outbound_notices(limit, offset, status, dag_id, task_id, topic):
    where_sql, params = _outbox_where(status, dag_id, task_id, topic)
    sql = f"""
        SELECT
          id, created_at, updated_at, status, topic, topic_kind,
          validation_status, mission, instrument, alert_type, alert_tense,
          record_number, event_name, trigger_time, alert_datetime,
          created_by_dag_id, dag_run_id, task_id, source_pipeline,
          attempts_count, max_attempts, last_error, published_at,
          payload_sha256, idempotency_key
        FROM gcn_outbound_notices
        {where_sql}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def _fetch_outbound_topics():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT topic, COUNT(*) AS notice_count
                FROM gcn_outbound_notices
                GROUP BY topic
                ORDER BY topic
                """
            )
            return cur.fetchall()


def _fetch_outbound_dags():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT created_by_dag_id AS dag_id, COUNT(*) AS notice_count
                FROM gcn_outbound_notices
                WHERE created_by_dag_id IS NOT NULL
                GROUP BY created_by_dag_id
                ORDER BY created_by_dag_id
                """
            )
            return cur.fetchall()


def _parse_classic_text(raw_payload):
    """Extract simple KEY: value fields from classic GCN text notices."""
    fields = {}
    for line in str(raw_payload or "").splitlines():
        match = re.match(r"^\s*([A-Z0-9_]+):\s*(.*?)\s*$", line)
        if match and match.group(1) not in fields:
            fields[match.group(1)] = match.group(2)
    return fields


def _topic_parts(topic):
    """Infer mission and instrument from topic suffixes such as FERMI_GBM_POS_TEST."""
    suffix = str(topic or "").rsplit(".", 1)[-1]
    parts = suffix.split("_")
    if len(parts) < 2:
        return None, None
    return parts[0], parts[1]


def _parse_degrees(raw_payload, field):
    match = re.search(rf"^\s*{re.escape(field)}:\s*([+-]?\d+(?:\.\d+)?)d", str(raw_payload or ""), re.MULTILINE)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _parse_classic_trigger_time(fields):
    date_match = re.search(r"\b(\d{2})/(\d{2})/(\d{2})\b", fields.get("GRB_DATE", ""))
    time_match = re.search(r"\{(\d{2}:\d{2}:\d{2}(?:\.\d+)?)\}", fields.get("GRB_TIME", ""))
    if not date_match or not time_match:
        return None
    year, month, day = date_match.groups()
    return f"20{year}-{month}-{day} {time_match.group(1)}"


def _clean_trigger_id(value):
    if value in (None, ""):
        return None
    return str(value).split(",", 1)[0].strip()


def _format_position(ra_deg, dec_deg):
    if ra_deg is None or dec_deg is None:
        return ""
    return f"{float(ra_deg):.4f}, {float(dec_deg):.4f}"


def _enrich_notice_display(notice):
    """Add display-only fields without mutating persisted notice data."""
    fields = _parse_classic_text(notice.get("raw_payload"))
    topic_mission, topic_instrument = _topic_parts(notice.get("topic"))
    notice_type = fields.get("NOTICE_TYPE")
    trigger_num = _clean_trigger_id(fields.get("TRIGGER_NUM") or notice.get("trig_id"))
    inferred_ra = _parse_degrees(notice.get("raw_payload"), "GRB_RA")
    inferred_dec = _parse_degrees(notice.get("raw_payload"), "GRB_DEC")

    if not notice.get("mission"):
        notice["mission"] = topic_mission
    if not notice.get("instrument"):
        notice["instrument"] = topic_instrument
    if not notice.get("event_name") and notice_type:
        notice["event_name"] = notice_type
    if trigger_num:
        notice["trig_id"] = trigger_num
    if notice.get("ra_deg") is None and inferred_ra is not None:
        notice["ra_deg"] = inferred_ra
    if notice.get("dec_deg") is None and inferred_dec is not None:
        notice["dec_deg"] = inferred_dec

    event_parts = [value for value in [notice.get("event_name"), notice.get("trig_id")] if value]
    notice["event_display"] = " / ".join(str(value) for value in event_parts)
    notice["time_display"] = notice.get("trigger_time") or notice.get("isotime") or _parse_classic_trigger_time(fields) or ""
    notice["position_display"] = _format_position(notice.get("ra_deg"), notice.get("dec_deg"))


def _fetch_topics():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT topic, COUNT(*) AS notice_count
                FROM gcn_inbound_notices
                GROUP BY topic
                ORDER BY topic
                """
            )
            return cur.fetchall()


def _fetch_notice(notice_id):
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM gcn_inbound_notices WHERE id = %s", (notice_id,))
            notice = cur.fetchone()
    if notice:
        _enrich_notice_display(notice)
    return notice


def _fetch_outbound_notice(notice_id):
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM gcn_outbound_notices WHERE id = %s", (notice_id,))
            notice = cur.fetchone()
    return notice


def _fetch_outbound_attempts(notice_id):
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  id, attempt_no, started_at, finished_at, status, dry_run,
                  topic, producer_client_label, kafka_partition, kafka_offset,
                  kafka_metadata_json, error_class, error_message, payload_sha256
                FROM gcn_delivery_attempts
                WHERE outbound_notice_id = %s
                ORDER BY attempt_no DESC, id DESC
                """,
                (notice_id,),
            )
            attempts = cur.fetchall()
    for attempt in attempts:
        attempt["kafka_metadata_pretty"] = _json_pretty(attempt.get("kafka_metadata_json"))
    return attempts


def _fetch_heartbeats(service_status=None):
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT component, updated_at, status, details_json
                FROM gcn_client_heartbeats
                ORDER BY component
                """
            )
            rows = cur.fetchall()
    service_status = service_status or _fetch_gcn_service_status()
    existing = {row.get("component") for row in rows}
    for component in ["inbound", "outbox"]:
        if component not in existing:
            rows.append(
                {
                    "component": component,
                    "updated_at": None,
                    "status": "off",
                    "details_json": {"reason": "no heartbeat row"},
                }
            )
    for row in rows:
        if service_status.get("status_class") == "off":
            row["status"] = "off"
            details = row.get("details_json")
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except Exception:
                    details = {"raw_details": details}
            if not isinstance(details, dict):
                details = {}
            details["service_state"] = service_status.get("raw_status")
            row["details_json"] = details
        row["details_pretty"] = _json_pretty(row.get("details_json"))
        row["status_class"] = _heartbeat_status_class(row.get("status"))
    return rows


def _heartbeat_status_class(status):
    status = str(status or "").strip().lower()
    if status == "running":
        return "running"
    if status in {"warning", "warn", "idle", "degraded", "starting", "locked"}:
        return "warning"
    if status in {"off", "offline", "stopped", "down", "failed", "error", "dead"}:
        return "off"
    return "unknown"


def _docker_base_url():
    docker_host = os.environ.get("DOCKER_HOST", "tcp://docker-proxy:2375")
    parsed = urlparse(docker_host)
    if parsed.scheme == "tcp":
        return f"http://{parsed.netloc}"
    if parsed.scheme in {"http", "https"}:
        return docker_host.rstrip("/")
    return "http://docker-proxy:2375"


def _fetch_gcn_service_status():
    container = os.environ.get("GCN_CLIENT_CONTAINER", "cosi_gcn_client")
    url = f"{_docker_base_url()}/containers/{container}/json"
    status = {
        "name": container,
        "status": "unknown",
        "status_class": "unknown",
        "raw_status": "unknown",
        "started_at": None,
        "finished_at": None,
    }
    request = Request(url, method="GET")
    try:
        with urlopen(request, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code == 404:
            status.update(
                {
                    "status": "off",
                    "status_class": "off",
                    "raw_status": "not_found",
                }
            )
            return status
        status.update(
            {
                "status": "warning",
                "status_class": "warning",
                "raw_status": f"docker_status_http_{exc.code}",
            }
        )
        return status
    except Exception as exc:
        status.update(
            {
                "status": "warning",
                "status_class": "warning",
                "raw_status": f"docker_status_unavailable: {exc}",
            }
        )
        return status

    state = payload.get("State") or {}
    running = bool(state.get("Running"))
    raw_status = state.get("Status") or ("running" if running else "stopped")
    status.update(
        {
            "status": "running" if running else "off",
            "status_class": "running" if running else "off",
            "raw_status": raw_status,
            "started_at": state.get("StartedAt"),
            "finished_at": state.get("FinishedAt"),
        }
    )
    return status


def _control_gcn_container(action, component):
    allowed = {"start", "stop", "restart"}
    action = str(action or "").strip().lower()
    component = str(component or "gcn-client").strip() or "gcn-client"
    if action not in allowed:
        raise ValueError(f"Unsupported control action: {action}")
    container = os.environ.get("GCN_CLIENT_CONTAINER", "cosi_gcn_client")
    query = "?t=2" if action in {"stop", "restart"} else ""
    url = f"{_docker_base_url()}/containers/{container}/{action}{query}"
    request = Request(url, data=b"", method="POST", headers={"Content-Length": "0"})
    timed_out = False
    try:
        with urlopen(request, timeout=45) as response:
            response.read()
    except HTTPError as exc:
        if exc.code not in {204, 304}:
            raise
    except (TimeoutError, socket.timeout):
        if action not in {"stop", "restart"}:
            raise
        timed_out = True
    return {
        "action": action,
        "component": component,
        "container": container,
        "timed_out": timed_out,
    }


def _inject_inbound_notice(raw_payload, topic, source):
    raw_payload = str(raw_payload or "")
    topic = str(topic or "").strip()
    source = str(source or "manual").strip() or "manual"
    if not topic:
        raise ValueError("Inbox topic is required.")
    subscribed_topics = _configured_consumer_topics()
    if subscribed_topics and topic not in subscribed_topics:
        raise ValueError(f"Inbox topic {topic!r} is not among configured GCN_CONSUMER_TOPICS.")
    if not subscribed_topics:
        raise ValueError("No GCN_CONSUMER_TOPICS configured; manual inbox injection is disabled.")
    if not raw_payload.strip():
        raise ValueError("Inbox payload is required.")

    payload_json = None
    normalized = {}
    content_type = "unknown"
    parse_status = "raw_only"
    validation_status = "not_applicable"
    validation_errors = None
    try:
        parsed = json.loads(raw_payload)
        content_type = "json"
        if not isinstance(parsed, dict):
            parse_status = "failed"
            validation_status = "invalid"
            validation_errors = [{"message": "JSON notice payload must be an object"}]
        else:
            payload_json = parsed
            normalized = _normalize_json_notice(parsed)
            parse_status = "parsed"
            validation_status = "not_checked"
    except json.JSONDecodeError as exc:
        validation_errors = [{"message": str(exc), "parser": "manual_json"}]
        content_type = "text" if topic.startswith("gcn.classic.text.") else "unknown"

    row = {
        "notice_uuid": str(uuid4()),
        "source": source,
        "topic": topic,
        "content_type": content_type,
        "payload_sha256": _payload_sha256(raw_payload),
        "raw_payload": raw_payload,
        "payload_json": payload_json,
        "parse_status": parse_status,
        "validation_status": validation_status,
        "validation_errors": validation_errors,
        **normalized,
    }
    columns = [
        "notice_uuid",
        "source",
        "topic",
        "content_type",
        "payload_sha256",
        "raw_payload",
        "payload_json",
        "parse_status",
        "validation_status",
        "validation_errors",
        "schema_url",
        "schema_version",
        "mission",
        "instrument",
        "alert_type",
        "alert_tense",
        "event_name",
        "event_ids",
        "trigger_time",
        "alert_datetime",
        "ra_deg",
        "dec_deg",
        "ra_dec_error_json",
        "healpix_url",
        "classification_json",
    ]
    for json_column in ["payload_json", "validation_errors", "event_ids", "ra_dec_error_json", "classification_json"]:
        row[json_column] = _json_or_none(row.get(json_column))
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO gcn_inbound_notices ({', '.join(columns)}) VALUES ({placeholders})"
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [row.get(column) for column in columns])
            conn.commit()
            return int(cur.lastrowid)


def _queue_manual_outbound_notice(raw_payload, topic, idempotency_key=None):
    raw_payload = str(raw_payload or "")
    topic = str(topic or "").strip()
    if not topic:
        raise ValueError("Outbox topic is required.")
    if not raw_payload.strip():
        raise ValueError("Outbox JSON payload is required.")
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Outbox payload must be valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("Outbox payload must be a JSON object.")

    canonical = _canonical_json(payload)
    payload_hash = _payload_sha256(canonical)
    normalized = _normalize_json_notice(payload)
    key = str(idempotency_key or "").strip() or f"manual:{topic}:{payload_hash}"
    row = {
        "outbox_uuid": str(uuid4()),
        "status": "queued",
        "topic": topic,
        "topic_kind": "test" if "test" in topic.lower() else "manual",
        "payload_json": canonical,
        "payload_sha256": payload_hash,
        "validation_status": "not_checked",
        "validation_errors": None,
        "created_by_dag_id": "manual",
        "dag_run_id": f"manual__{datetime.utcnow().isoformat(timespec='seconds')}",
        "task_id": "manual_injection",
        "source_pipeline": "manual",
        "priority": 0,
        "max_attempts": int(os.environ.get("GCN_MAX_ATTEMPTS", "3")),
        "idempotency_key": key,
        **normalized,
    }
    columns = [
        "outbox_uuid",
        "status",
        "topic",
        "topic_kind",
        "payload_json",
        "payload_sha256",
        "schema_url",
        "schema_version",
        "validation_status",
        "validation_errors",
        "mission",
        "instrument",
        "alert_type",
        "alert_tense",
        "record_number",
        "event_name",
        "event_ids",
        "trigger_time",
        "alert_datetime",
        "created_by_dag_id",
        "dag_run_id",
        "task_id",
        "source_pipeline",
        "priority",
        "max_attempts",
        "idempotency_key",
    ]
    for json_column in ["payload_json", "validation_errors", "event_ids"]:
        row[json_column] = _json_or_none(row.get(json_column))
    placeholders = ", ".join(["%s"] * len(columns))
    update_columns = [
        "updated_at = CURRENT_TIMESTAMP(6)",
        "id = LAST_INSERT_ID(id)",
        "payload_json = VALUES(payload_json)",
        "payload_sha256 = VALUES(payload_sha256)",
        "validation_status = VALUES(validation_status)",
        "validation_errors = VALUES(validation_errors)",
        "last_error = NULL",
    ]
    sql = f"""
        INSERT INTO gcn_outbound_notices ({", ".join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {", ".join(update_columns)}
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [row.get(column) for column in columns])
            conn.commit()
            return int(cur.lastrowid)


def _page_url(page, filters):
    """Build pagination URLs while preserving active filters."""
    query = [("tab", filters["tab"])]
    for topic in filters["topics"]:
        query.append(("topic", topic))
    if filters["validation_status"]:
        query.append(("validation_status", filters["validation_status"]))
    if filters["content_type"]:
        query.append(("content_type", filters["content_type"]))
    if filters["outbox_status"]:
        query.append(("outbox_status", filters["outbox_status"]))
    if filters["dag_id"]:
        query.append(("dag_id", filters["dag_id"]))
    if filters["task_id"]:
        query.append(("task_id", filters["task_id"]))
    if filters["outbox_topic"]:
        query.append(("outbox_topic", filters["outbox_topic"]))
    query.extend(
        [
            ("limit", filters["limit"]),
            ("page", page),
        ]
    )
    return f"?{urlencode(query)}"


def _pagination(page, limit, total_count, filters):
    total_pages = max(1, (total_count + limit - 1) // limit)
    page = min(max(page, 1), total_pages)
    start = ((page - 1) * limit) + 1 if total_count else 0
    end = min(page * limit, total_count)
    return {
        "page": page,
        "limit": limit,
        "total_count": total_count,
        "total_pages": total_pages,
        "start": start,
        "end": end,
        "first_url": _page_url(1, filters),
        "prev_url": _page_url(max(page - 1, 1), filters),
        "next_url": _page_url(min(page + 1, total_pages), filters),
        "last_url": _page_url(total_pages, filters),
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }


class ExploreNoticesView(BaseView):
    default_view = "index"
    route_base = "/explore-notices"

    @expose("/inject-inbox", methods=["POST"])
    @login_required
    def inject_inbox(self):
        if not current_user.is_authenticated:
            return redirect("/login/?next=/explore-notices/")
        try:
            notice_id = _inject_inbound_notice(
                request.form.get("payload", ""),
                request.form.get("topic", ""),
                request.form.get("source", "manual"),
            )
            flash(f"Manual inbox notice inserted with ID {notice_id}.", "success")
        except Exception as exc:
            flash(f"Manual inbox injection failed: {exc}", "error")
        return redirect(url_for("ExploreNoticesView.index", tab="inbox"))

    @expose("/inject-outbox", methods=["POST"])
    @login_required
    def inject_outbox(self):
        if not current_user.is_authenticated:
            return redirect("/login/?next=/explore-notices/")
        try:
            notice_id = _queue_manual_outbound_notice(
                request.form.get("payload", ""),
                request.form.get("topic", ""),
                request.form.get("idempotency_key", ""),
            )
            flash(f"Manual outbox notice queued with ID {notice_id}.", "success")
        except Exception as exc:
            flash(f"Manual outbox injection failed: {exc}", "error")
        return redirect(url_for("ExploreNoticesView.index", tab="outbox"))

    @expose("/control-component", methods=["POST"])
    @login_required
    def control_component(self):
        if not current_user.is_authenticated:
            return redirect("/login/?next=/explore-notices/")
        tab = request.form.get("tab", "inbox")
        try:
            result = _control_gcn_container(
                request.form.get("action", ""),
                request.form.get("component", ""),
            )
            suffix = " The Docker API did not return before timeout, but the request was sent." if result.get("timed_out") else ""
            flash(
                f"Requested {result['action']} for {result['component']} via container {result['container']}.{suffix}",
                "success",
            )
        except Exception as exc:
            flash(f"Component control failed: {exc}", "error")
        return redirect(url_for("ExploreNoticesView.index", tab=tab))

    @expose("/")
    @login_required
    def index(self):
        if not current_user.is_authenticated:
            return redirect("/login/?next=/explore-notices/")
        tab = request.args.get("tab", "inbox").strip().lower()
        if tab not in {"inbox", "outbox"}:
            tab = "inbox"
        limit = _bounded_int(request.args.get("limit"), 15, 1, 500)
        page = _bounded_int(request.args.get("page"), 1, 1, 1000000)
        topics = _normalize_topics(request.args.getlist("topic"))
        validation_status = request.args.get("validation_status", "").strip()
        content_type = request.args.get("content_type", "").strip()
        outbox_status = request.args.get("outbox_status", "").strip()
        dag_id = request.args.get("dag_id", "").strip()
        task_id = request.args.get("task_id", "").strip()
        outbox_topic = request.args.get("outbox_topic", "").strip()
        filters = {
            "tab": tab,
            "limit": limit,
            "topics": topics,
            "validation_status": validation_status,
            "content_type": content_type,
            "outbox_status": outbox_status,
            "dag_id": dag_id,
            "task_id": task_id,
            "outbox_topic": outbox_topic,
        }
        error = None
        notices = []
        outbound_notices = []
        heartbeats = []
        available_topics = []
        outbound_topics = []
        outbound_dags = []
        subscribed_topics = _configured_consumer_topics()
        service_status = _fetch_gcn_service_status()
        pagination = _pagination(1, limit, 0, filters)
        try:
            if tab == "outbox":
                total_count = _fetch_outbound_notice_count(outbox_status, dag_id, task_id, outbox_topic)
            else:
                total_count = _fetch_notice_count(topics, validation_status, content_type)
            pagination = _pagination(page, limit, total_count, filters)
            offset = (pagination["page"] - 1) * limit
            if tab == "outbox":
                outbound_notices = _fetch_outbound_notices(
                    limit,
                    offset,
                    outbox_status,
                    dag_id,
                    task_id,
                    outbox_topic,
                )
            else:
                notices = _fetch_notices(limit, offset, topics, validation_status, content_type)
            service_status = _fetch_gcn_service_status()
            heartbeats = _fetch_heartbeats(service_status)
            subscribed_topics = _subscribed_topics(heartbeats)
            available_topics = _fetch_topics()
            outbound_topics = _fetch_outbound_topics()
            outbound_dags = _fetch_outbound_dags()
        except Exception as exc:
            error = str(exc)
        return self.render_template(
            "explore_notices.html",
            notices=notices,
            outbound_notices=outbound_notices,
            heartbeats=heartbeats,
            available_topics=available_topics,
            outbound_topics=outbound_topics,
            outbound_dags=outbound_dags,
            subscribed_topics=subscribed_topics,
            service_status=service_status,
            pagination=pagination,
            error=error,
            filters=filters,
            now=datetime.utcnow(),
        )

    @expose("/notice/<int:notice_id>")
    @login_required
    def notice_detail(self, notice_id):
        error = None
        notice = None
        try:
            notice = _fetch_notice(notice_id)
            if notice:
                notice["payload_json_pretty"] = _json_pretty(notice.get("payload_json"))
                notice["validation_errors_pretty"] = _json_pretty(notice.get("validation_errors"))
                notice["event_ids_pretty"] = _json_pretty(notice.get("event_ids"))
                notice["classification_pretty"] = _json_pretty(notice.get("classification_json"))
        except Exception as exc:
            error = str(exc)
        return self.render_template(
            "explore_notice_detail.html",
            notice=notice,
            error=error,
        )

    @expose("/outbox/<int:notice_id>")
    @login_required
    def outbox_detail(self, notice_id):
        error = None
        notice = None
        attempts = []
        try:
            notice = _fetch_outbound_notice(notice_id)
            attempts = _fetch_outbound_attempts(notice_id)
            if notice:
                notice["payload_json_pretty"] = _json_pretty(notice.get("payload_json"))
                notice["validation_errors_pretty"] = _json_pretty(notice.get("validation_errors"))
                notice["event_ids_pretty"] = _json_pretty(notice.get("event_ids"))
        except Exception as exc:
            error = str(exc)
        return self.render_template(
            "explore_outbox_detail.html",
            notice=notice,
            attempts=attempts,
            error=error,
        )


class ExploreNoticesPlugin(AirflowPlugin):
    name = "explore_notices_plugin"
    flask_blueprints = [explore_notices_bp]
    appbuilder_views = [
        {
            # The name of the view, which will be displayed in the menu
            "name": "GCN Notices Explorer",
            # Which Category to put the link in, if you don't want one, set to an empty string
            "category": "",
            "view": ExploreNoticesView(),
        }
    ]
