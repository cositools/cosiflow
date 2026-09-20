"""Pure runtime helpers shared by COSIDAG orchestration and its tests."""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from typing import Any


def normalize_run_conf(value: Any) -> dict[str, Any]:
    """Return a detached mapping and reject unsupported DAG-run configuration."""
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("dag_run.conf must be a JSON object") from exc
        if isinstance(decoded, Mapping):
            return dict(decoded)
    raise ValueError("dag_run.conf must be a mapping")


def build_successor_conf(value: Any) -> dict[str, Any]:
    """Copy source configuration and increment the automatic-retrigger count."""
    conf = normalize_run_conf(value)
    raw_count = conf.get("retrig_run_count", 0)
    try:
        run_count = int(raw_count)
    except (TypeError, ValueError) as exc:
        raise ValueError("retrig_run_count must be an integer") from exc
    if run_count < 0:
        raise ValueError("retrig_run_count cannot be negative")
    conf["retrig_run_count"] = run_count + 1
    return conf


def automatic_retrigger_run_id(
    dag_id: str,
    source_run_id: str,
    task_id: str = "automatic_retrig",
) -> str:
    """Build a compact ID that is unique per source run and stable on retries."""
    identity = f"cosiflow:{dag_id}:{source_run_id}:{task_id}"
    token = uuid.uuid5(uuid.NAMESPACE_URL, identity).hex
    return f"auto__{token}"
