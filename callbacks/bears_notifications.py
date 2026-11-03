#############################################################################
# File    : bears_notifications.py
# Author  : WingYeung Ma
# Version : 2026-11-02 - Initial version.
# Notes   : BEARS messages for COSIflow.
#
#         - bears_task_failure: generic failure alert. COSIDAG attaches it
#           to every task, beside the email callback, so any final task
#           failure reports which step failed.
#         - send_bears_message: public helper for pipelines to send their
#           own messages (e.g. backend-pipeline's BACKEND_COMPLETE).
#
#############################################################################

#############################################################################
# Imports
#############################################################################

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
from pathlib import Path
from typing import Any, Mapping, NamedTuple, Optional
from urllib.parse import urlsplit

#############################################################################
# Globals
#############################################################################

LOGGER = logging.getLogger(__name__)

SOURCE = "COSIFLOW"
DEFAULT_CLIENT_DIR = "/home/gamma/airflow/bears-client"
LEVELS = ("INFO", "WARNING", "ERROR")


class BearsSettings(NamedTuple):
    host: str
    port: int
    client_id: str
    facility: str


#############################################################################
# Public Helper
#############################################################################

# Sends "[LEVEL]:[SOURCE]:EVENT: message" to BEARS. 
# level is INFO, WARNING, or ERROR;
# event and source are names such as BACKEND_COMPLETE and BACKEND.
def send_bears_message(level: str, event: str, message: str, source: str = SOURCE) -> bool:
    try:
        if level not in LEVELS:
            raise ValueError(f"level must be one of {', '.join(LEVELS)}")

        settings = load_settings()
        if settings is None:
            return False
        client_class = load_client_class()
        if client_class is None:
            LOGGER.warning("cosiflow_bears_client_missing dir=%s", DEFAULT_CLIENT_DIR)
            return False

        text = _single_line(f"[{level}]:[{source}]:{event}: {message}")
        return _deliver(client_class, settings, source, text)
    except Exception:
        LOGGER.exception("cosiflow_bears_send_failed source=%s event=%s", source, event)
        return False


#############################################################################
# Airflow Task Callback
#############################################################################

# Generic failure alert. 
def bears_task_failure(context):
    try:
        send_bears_message("ERROR", "TASK_FAILED", failure_details(context or {}))
    except Exception:
        LOGGER.exception("cosiflow_bears_callback_failed")
    return None


# e.g. dag=d; task=t; run=r; try=2; reason=RuntimeError: ...; log=.....
def failure_details(context: Mapping[str, Any]) -> str:
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
    task_id = getattr(ti, "task_id", None) or getattr(task, "task_id", None) or "unknown"
    map_index = getattr(ti, "map_index", None)
    if isinstance(map_index, int) and map_index >= 0:
        task_id = f"{task_id}[{map_index}]"
    run_id = getattr(ti, "run_id", None) or getattr(dag_run, "run_id", None) or "unknown"

    parts = [f"dag={dag_id}", f"task={task_id}", f"run={run_id}"]

    try_number = getattr(ti, "try_number", None)
    if isinstance(try_number, int) and try_number > 0:
        parts.append(f"try={try_number}")

    exception = context.get("exception")
    if exception is not None:
        reason = (
            f"{type(exception).__name__}: {exception}"
            if isinstance(exception, BaseException)
            else str(exception)
        )
        parts.append(f"reason={reason}")

    log_url = _safe_log_url(ti)
    if log_url:
        parts.append(f"log={log_url}")

    return "; ".join(_single_line(p) for p in parts)


#############################################################################
# Configuration
#############################################################################

# Returns settings, None when disabled, or raises ValueError when invalid.
def load_settings(environ: Optional[Mapping[str, str]] = None) -> Optional[BearsSettings]:
    env = os.environ if environ is None else environ
    enabled = env.get("COSI_BEARS_ENABLED", "false").strip().lower()
    if enabled == "false":
        return None

    required = {}
    for name in ("HOST", "CLIENT_ID", "FACILITY", "PORT"):
        value = env.get(f"COSI_BEARS_{name}", "").strip()
        if not value:
            raise ValueError(f"COSI_BEARS_{name} is required")
        required[name] = value

    return BearsSettings(
        host=required["HOST"],
        port=int(required["PORT"]),
        client_id=required["CLIENT_ID"],
        facility=required["FACILITY"],
    )


#############################################################################
# Message Helpers
#############################################################################

def _single_line(value: Any) -> str:
    return " ".join(str(value).split())


# Absolute http(s) URL without credentials, otherwise None.
def _safe_log_url(task_instance: Any) -> Optional[str]:
    if task_instance is None:
        return None
    try:
        value = str(task_instance.log_url)
    except Exception:
        return None
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    return value


#############################################################################
# BEARS Client
#############################################################################

class _DeliveryLog:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def debug(self, msg, *args, **kwargs):
        LOGGER.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        LOGGER.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        LOGGER.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.errors.append(str(msg))
        LOGGER.error(msg, *args, **kwargs)


_client_class_cache: Any = None


# SSL's BEARSClient class, or None when the client is not available.
def load_client_class(client_dir: Optional[str] = None):
    global _client_class_cache
    if _client_class_cache is not None:
        return _client_class_cache

    try:
        module = importlib.import_module("bears")
    except ModuleNotFoundError as exc:
        if exc.name != "bears":
            raise
        path = Path(client_dir or DEFAULT_CLIENT_DIR) / "bears.py"
        if not path.is_file():
            return None
        spec = importlib.util.spec_from_file_location("bears", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

    _client_class_cache = module.BEARSClient
    return _client_class_cache


# One connection per message, using BEARSClient's default timeout (5 s).
def _deliver(client_class, settings: BearsSettings, source: str, text: str) -> bool:
    delivery = _DeliveryLog()
    client = client_class(
        host=settings.host,
        port=settings.port,
        client_id=settings.client_id,
        facility=settings.facility,
        source=source,
        reconnect_attempts=None,
        logger=delivery,
    )
    
    client.one_shot(text)

    if delivery.errors:
        LOGGER.error(
            "cosiflow_bears_delivery_failed source=%s errors=%s",
            source,
            ",".join(delivery.errors),
        )
        return False
    LOGGER.info("cosiflow_bears_delivered source=%s message=%s", source, text)
    return True
