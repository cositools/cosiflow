"""Fail-safe email callbacks for COSIflow task and DAG notifications."""

from __future__ import annotations

import html
import logging
import os
import sys
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from airflow.configuration import conf
from airflow.utils.email import send_email
from airflow.utils.log.file_task_handler import FileTaskHandler


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
MODULES_PATH = os.path.join(AIRFLOW_HOME, "modules")
if MODULES_PATH not in sys.path:
    sys.path.append(MODULES_PATH)

from notification_subscriptions import (  # type: ignore  # noqa: E402
    resolve_notification_recipients,
    valid_email_address,
)


LOGGER = logging.getLogger(__name__)
DEFAULT_LOG_TAIL_LINES = 30
DEFAULT_LOG_TAIL_BYTES = 64 * 1024
MAX_LOG_TAIL_LINES = 500
MAX_LOG_TAIL_BYTES = 1024 * 1024
MAX_EXCEPTION_CHARS = 4000


def _bounded_int_env(name: str, default: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        LOGGER.warning("cosiflow_notification_invalid_limit name=%s", name)
        return default
    if value <= 0 or value > maximum:
        LOGGER.warning("cosiflow_notification_invalid_limit name=%s", name)
        return default
    return value


def _fallback_recipients() -> list[str]:
    recipients = {
        item.strip()
        for item in os.environ.get("COSIFLOW_ALERT_FALLBACK_RECIPIENTS", "").split(",")
        if valid_email_address(item.strip())
    }
    return sorted(recipients, key=str.casefold)


def _context_value(context: Mapping[str, Any], key: str, default: Any = "") -> Any:
    value = context.get(key, default)
    return default if value is None else value


def _context_snapshot(context: Mapping[str, Any]) -> dict[str, Any]:
    ti = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    task = context.get("task") or getattr(ti, "task", None)
    dag_id = (
        getattr(ti, "dag_id", None)
        or getattr(dag_run, "dag_id", None)
        or getattr(dag, "dag_id", None)
        or "unknown"
    )
    task_id = getattr(ti, "task_id", None) or getattr(task, "task_id", None) or ""
    run_id = getattr(ti, "run_id", None) or getattr(dag_run, "run_id", None) or "unknown"
    logical_date = (
        _context_value(context, "logical_date", None)
        or getattr(dag_run, "logical_date", None)
        or getattr(ti, "execution_date", None)
    )
    task_type = getattr(task, "task_type", None) if task is not None else None
    task_class = task.__class__.__name__ if task is not None else ""
    operator = getattr(ti, "operator", None) or task_type or task_class
    return {
        "task_instance": ti,
        "dag_id": str(dag_id),
        "task_id": str(task_id),
        "run_id": str(run_id),
        "logical_date": "" if logical_date is None else str(logical_date),
        "operator": str(operator or ""),
        "try_number": getattr(ti, "try_number", None),
        "map_index": getattr(ti, "map_index", None),
        "exception": str(_context_value(context, "exception", "") or "")[
            :MAX_EXCEPTION_CHARS
        ],
    }


def _safe_log_url(task_instance: Any) -> str | None:
    if task_instance is None:
        return None
    try:
        value = str(task_instance.log_url)
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("log URL must be absolute HTTP(S) without credentials")
        return value
    except Exception:
        LOGGER.exception("cosiflow_notification_log_url_unavailable")
        return None


def _task_log_path(task_instance: Any) -> Path:
    base = Path(conf.get("logging", "base_log_folder")).resolve()
    template = conf.get("logging", "log_filename_template")
    handler = FileTaskHandler(str(base), template)
    relative = handler._render_filename(task_instance, int(task_instance.try_number))
    candidate = (base / relative).resolve()
    if candidate == base or base not in candidate.parents:
        raise ValueError("rendered task log path escapes the configured log root")
    return candidate


def _tail_text(path: Path, max_lines: int, max_bytes: int) -> str:
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        start = max(0, size - max_bytes)
        handle.seek(start)
        payload = handle.read(max_bytes)
    if start:
        first_newline = payload.find(b"\n")
        payload = payload[first_newline + 1 :] if first_newline >= 0 else b""
    lines = payload.decode("utf-8", errors="replace").splitlines()
    tail = "\n".join(lines[-max_lines:])
    if start:
        tail = "[earlier log content omitted]\n" + tail
    return tail or "Log file is empty."


def _log_preview(task_instance: Any) -> str:
    if task_instance is None:
        return "Task log preview is not available for this event."
    max_lines = _bounded_int_env(
        "COSIFLOW_ALERT_LOG_TAIL_LINES", DEFAULT_LOG_TAIL_LINES, MAX_LOG_TAIL_LINES
    )
    max_bytes = _bounded_int_env(
        "COSIFLOW_ALERT_LOG_TAIL_BYTES", DEFAULT_LOG_TAIL_BYTES, MAX_LOG_TAIL_BYTES
    )
    try:
        return _tail_text(_task_log_path(task_instance), max_lines, max_bytes)
    except FileNotFoundError:
        return "Task log file is not available on this Airflow component."
    except Exception:
        LOGGER.exception("cosiflow_notification_log_preview_unavailable")
        return "Task log preview is unavailable. Use the Airflow log link when present."


def _subject_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split())


def _event_label(event_type: str) -> str:
    return event_type.replace("_", " ").title()


def _render_email(snapshot: Mapping[str, Any], event_type: str) -> tuple[str, str]:
    log_url = _safe_log_url(snapshot.get("task_instance"))
    preview = _log_preview(snapshot.get("task_instance"))
    label = _event_label(event_type)
    subject_target = snapshot.get("task_id") or snapshot.get("dag_id")
    subject = (
        f"[COSIflow] {label}: {_subject_text(subject_target)} "
        f"({_subject_text(snapshot.get('dag_id'))})"
    )

    def escaped(key: str) -> str:
        return html.escape(str(snapshot.get(key, "") or ""), quote=True)

    link_row = ""
    if log_url:
        escaped_url = html.escape(log_url, quote=True)
        link_row = (
            "<tr><td><strong>Log URL:</strong></td>"
            f'<td><a href="{escaped_url}">{escaped_url}</a></td></tr>'
        )
    task_row = ""
    if snapshot.get("task_id"):
        task_row = f"<tr><td><strong>Task:</strong></td><td>{escaped('task_id')}</td></tr>"
    attempt_row = ""
    if snapshot.get("try_number") is not None:
        attempt_row = (
            "<tr><td><strong>Attempt:</strong></td>"
            f"<td>{escaped('try_number')}</td></tr>"
        )
    map_row = ""
    if snapshot.get("map_index") is not None and int(snapshot["map_index"]) >= 0:
        map_row = (
            "<tr><td><strong>Map index:</strong></td>"
            f"<td>{escaped('map_index')}</td></tr>"
        )
    exception_block = ""
    if snapshot.get("exception"):
        exception_block = (
            '<h3 style="margin-top:20px;">Exception</h3>'
            '<pre style="background-color:#f5f5f5; padding:10px; '
            'border:1px solid #ccc; overflow:auto;">'
            f"{escaped('exception')}</pre>"
        )
    html_content = f"""
    <html>
      <body style="font-family:Arial, sans-serif; font-size:14px; color:#333;">
        <h2 style="color:#c0392b;">{html.escape(label, quote=True)}</h2>
        <table style="border-collapse:collapse;">
          <tr><td><strong>DAG:</strong></td><td>{escaped('dag_id')}</td></tr>
          {task_row}
          <tr><td><strong>Run:</strong></td><td>{escaped('run_id')}</td></tr>
          <tr><td><strong>Logical date:</strong></td><td>{escaped('logical_date')}</td></tr>
          {attempt_row}
          {map_row}
          {link_row}
        </table>
        {exception_block}
        <h3 style="margin-top:20px;">Log preview</h3>
        <pre style="background-color:#f5f5f5; padding:10px; border:1px solid #ccc; max-height:300px; overflow:auto;">{html.escape(preview, quote=True)}</pre>
      </body>
    </html>
    """
    return subject, html_content


def _notify(context: Mapping[str, Any], event_type: str) -> None:
    snapshot = _context_snapshot(context)
    routing_context = {
        "dag_id": snapshot["dag_id"],
        "task_id": snapshot["task_id"],
        "operator": snapshot["operator"],
    }
    try:
        recipients = resolve_notification_recipients(event_type, routing_context)
    except Exception:
        LOGGER.exception(
            "cosiflow_notification_subscription_lookup_failed event=%s dag_id=%s task_id=%s",
            event_type,
            snapshot["dag_id"],
            snapshot["task_id"],
        )
        recipients = _fallback_recipients()
    if not recipients:
        LOGGER.info(
            "cosiflow_notification_skipped event=%s dag_id=%s task_id=%s reason=no_recipients",
            event_type,
            snapshot["dag_id"],
            snapshot["task_id"],
        )
        return
    subject, html_content = _render_email(snapshot, event_type)
    try:
        send_email(to=recipients, subject=subject, html_content=html_content)
    except Exception:
        LOGGER.exception(
            "cosiflow_notification_delivery_failed event=%s dag_id=%s task_id=%s recipient_count=%s",
            event_type,
            snapshot["dag_id"],
            snapshot["task_id"],
            len(recipients),
        )
        return
    LOGGER.info(
        "cosiflow_notification_delivered event=%s dag_id=%s task_id=%s recipient_count=%s",
        event_type,
        snapshot["dag_id"],
        snapshot["task_id"],
        len(recipients),
    )


def _safe_notify(context: Mapping[str, Any], event_type: str) -> None:
    try:
        _notify(context or {}, event_type)
    except Exception:
        LOGGER.exception("cosiflow_notification_callback_failed event=%s", event_type)
    return None


def notify_email(context):
    """Compatibility entry point for task failure notifications."""
    return _safe_notify(context, "task_failure")


def notify_retry(context):
    return _safe_notify(context, "task_retry")


def notify_success(context):
    return _safe_notify(context, "task_success")


def notify_dag_failure(context):
    return _safe_notify(context, "dag_failure")


def notify_dag_success(context):
    return _safe_notify(context, "dag_success")
