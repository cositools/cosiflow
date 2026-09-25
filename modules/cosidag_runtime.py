"""Pure runtime helpers shared by COSIDAG orchestration and its tests."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Mapping
from pathlib import PurePath, PureWindowsPath
from typing import Any, NamedTuple


_TRUE_STRINGS = frozenset({"1", "true", "yes", "on"})
_FALSE_STRINGS = frozenset({"0", "false", "no", "off"})
_MONITORING_POLICIES = frozenset({"folder-driven", "file-driven"})
_SELECT_POLICIES = frozenset({"first", "latest_mtime"})


class SensorRuntimeConfig(NamedTuple):
    """Validated values consumed by the COSIDAG discovery sensor."""

    raw: dict[str, Any]
    monitoring_folders: Any
    level: int
    date_queries: Any
    idle_seconds: int
    min_files: int
    ready_marker: str | None
    only_basename: Any
    prefer_deepest: bool
    policy: str
    claim_stale_seconds: int


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


def parse_runtime_bool(value: Any, field_name: str) -> bool:
    """Parse an explicit boolean representation and reject ambiguous values."""
    if isinstance(value, bool):
        return value
    if type(value) is int and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUE_STRINGS:
            return True
        if normalized in _FALSE_STRINGS:
            return False
    accepted = "true/false, yes/no, on/off, or 1/0"
    raise ValueError(f"{field_name} must be one of {accepted}; got {value!r}")


def parse_runtime_int(value: Any, field_name: str, minimum: int | None = None) -> int:
    """Parse an integer without accepting booleans or fractional values."""
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer, got {value!r}")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer, got {value!r}") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{field_name} must be an integer, got {value!r}")
    if isinstance(value, str) and value.strip() != str(parsed):
        raise ValueError(f"{field_name} must be an integer, got {value!r}")
    if minimum is not None and parsed < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}, got {value!r}")
    return parsed


def normalize_monitoring_policy(value: Any) -> str:
    """Normalize and validate folder/file monitoring policy."""
    normalized = str(value or "folder-driven").strip().lower()
    if normalized not in _MONITORING_POLICIES:
        expected = " or ".join(repr(item) for item in sorted(_MONITORING_POLICIES))
        raise ValueError(f"Unsupported COSIDAG monitoring policy {value!r}; expected {expected}")
    return normalized


def normalize_select_policy(value: Any) -> str:
    """Normalize and validate input selection policy before filesystem work."""
    normalized = str(value).strip().lower()
    if normalized not in _SELECT_POLICIES:
        expected = " or ".join(repr(item) for item in sorted(_SELECT_POLICIES))
        raise ValueError(f"Unsupported select_policy {value!r}; expected {expected}")
    return normalized


def normalize_relative_runtime_path(value: Any, field_name: str) -> str | None:
    """Validate a configured relative path before resolving it under a root."""
    if value is None or value == "":
        return None
    if not isinstance(value, (str, os.PathLike)):
        raise ValueError(f"{field_name} must be a relative path, got {value!r}")
    path = os.fspath(value)
    if not path or "\x00" in path:
        raise ValueError(f"{field_name} must be a non-empty relative path")
    if os.path.isabs(path) or PureWindowsPath(path).is_absolute():
        raise ValueError(f"{field_name} must be relative, got {value!r}")
    if ".." in PurePath(path).parts or ".." in PureWindowsPath(path).parts:
        raise ValueError(f"{field_name} must not contain parent traversal, got {value!r}")
    normalized = os.path.normpath(path)
    if normalized in {"", os.curdir}:
        raise ValueError(f"{field_name} must identify a path below the candidate folder")
    return normalized


def validate_sensor_runtime_config(value: Any, defaults: Mapping[str, Any]) -> SensorRuntimeConfig:
    """Validate discovery overrides before filesystem access or state mutation."""
    conf = normalize_run_conf(value)
    date_queries = conf.get("date_queries")
    if date_queries is None:
        date_value = conf.get("date", defaults.get("date"))
        date_queries = f"=={date_value}" if date_value else defaults.get("date_queries")
    policy_value = conf.get(
        "monitoring_policy",
        conf.get("policy", defaults.get("policy", "folder-driven")),
    )
    return SensorRuntimeConfig(
        raw=conf,
        monitoring_folders=conf.get("monitoring_folders", defaults.get("monitoring_folders")),
        level=parse_runtime_int(conf.get("level", defaults.get("level", 1)), "level", minimum=1),
        date_queries=date_queries,
        idle_seconds=parse_runtime_int(
            conf.get("idle_seconds", defaults.get("idle_seconds", 20)),
            "idle_seconds",
            minimum=0,
        ),
        min_files=parse_runtime_int(
            conf.get("min_files", defaults.get("min_files", 1)),
            "min_files",
            minimum=0,
        ),
        ready_marker=normalize_relative_runtime_path(
            conf.get("ready_marker", defaults.get("ready_marker")),
            "ready_marker",
        ),
        only_basename=conf.get("only_basename", defaults.get("only_basename")),
        prefer_deepest=parse_runtime_bool(
            conf.get("prefer_deepest", defaults.get("prefer_deepest", True)),
            "prefer_deepest",
        ),
        policy=normalize_monitoring_policy(policy_value),
        claim_stale_seconds=parse_runtime_int(
            conf.get("claim_stale_seconds", defaults.get("claim_stale_seconds", 86400)),
            "claim_stale_seconds",
            minimum=1,
        ),
    )


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
