from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def payload_sha256(raw_payload: str | bytes) -> str:
    if isinstance(raw_payload, str):
        raw_payload = raw_payload.encode("utf-8")
    return hashlib.sha256(raw_payload).hexdigest()


def parse_iso_datetime(value: Any) -> str | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.isoformat(sep=" ", timespec="microseconds")


def normalize_json_notice(payload: dict[str, Any]) -> dict[str, Any]:
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
        "trigger_time": parse_iso_datetime(payload.get("trigger_time")),
        "alert_datetime": parse_iso_datetime(payload.get("alert_datetime")),
        "ra_deg": payload.get("ra"),
        "dec_deg": payload.get("dec"),
        "ra_dec_error_json": payload.get("ra_dec_error"),
        "healpix_url": payload.get("healpix_url"),
        "classification_json": payload.get("classification"),
        "record_number": payload.get("record_number"),
    }


def canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _schema_version(schema_url: Any) -> str | None:
    if not schema_url:
        return None
    text = str(schema_url)
    marker = "/schema/"
    if marker not in text:
        return None
    tail = text.split(marker, 1)[1]
    return tail.split("/", 1)[0] if "/" in tail else tail


def _string_or_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
