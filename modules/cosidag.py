"""
COSIDAG — a convenience DAG subclass that wires a standard layout:

  1) check_new_file -> 2) automatic_retrig -> 3) resolve_inputs ->
  4) [custom tasks] -> 5) show_results -> 6) finalize_cosidag_state

This implementation supports disabling optional steps:

- check_new_file is NOT created if monitoring_folders is empty / None.
- automatic_retrig is NOT created if auto_retrig is False.

The chaining logic adapts automatically depending on which tasks exist.

Notes
------
* Requires Airflow 2.x.
* Environment: define COSIFLOW_HOME_URL (in your .env) to point to the web UI homepage.
* State: a transactional PostgreSQL table tracks claimed, failed, and successful
  paths across runs. In folder-driven mode it stores folder paths; in file-driven
  mode it stores file paths. Legacy ``COSIDAG_PROCESSED::<dag_id>`` Variables are
  imported during ``airflow-init`` and retained only as rollback evidence.
* Date queries: use date_queries (e.g. '>=2025-11-01' or ['>=2025-11-01','<=2025-11-05']).
* Only basename: if only_basename is provided, it only accepts candidate paths with the given basename.
* Prefer deepest: if prefer_deepest is True, it prefers the deepest subfolder.
* File patterns: if file_patterns is provided, it searches for files matching the given patterns
  using glob recursion and selects according to select_policy.
* Path helper: if available, the module cosiflow.modules.path is used to parse/build
  URL fragments from detected folders. The code degrades gracefully if not found.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from datetime import datetime
from typing import Callable, Iterable, Optional, Sequence
from urllib.parse import quote, urlsplit, urlunsplit

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException, DagRunAlreadyExists
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ---- Optional path utils --------------------------------------------------------
try:
    from cosiflow.modules.path import PathInfo, build_url_fragment  # type: ignore
except Exception:
    PathInfo = None  # type: ignore
    build_url_fragment = None  # type: ignore

# ---- Import on-failure callback -------------------------------------------------
import sys

airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
sys.path.append(os.path.join(airflow_home, "callbacks"))
sys.path.append(os.path.join(airflow_home, "modules"))
from on_failure_callback import notify_email  # type: ignore

from date_helper import _looks_like_date_folder, _parse_date_string, _apply_date_queries  # type: ignore
from cosidag_runtime import (  # type: ignore
    automatic_retrigger_run_id,
    build_successor_conf,
    normalize_monitoring_policy,
    normalize_relative_runtime_path,
    normalize_run_conf,
    normalize_select_policy,
    parse_runtime_bool,
    validate_sensor_runtime_config,
)
from cosidag_filesystem import (  # type: ignore
    DirectorySnapshot,
    file_snapshot,
    resolve_patterns,
    resolve_confined_relative_path,
    scan_directory_snapshots,
    scan_file_inventory,
    stability_observation,
)
from cosidag_state import (  # type: ignore
    claim_path,
    list_unavailable_paths,
    mark_path_failed,
    mark_path_succeeded,
    release_orphaned_claims,
)

_BASE_DEFAULT_ARGS = {
    "owner": "cosiflow",
    "email_on_failure": True,
    "on_failure_callback": notify_email,  # from callbacks/on_failure_callback.py
}

LOGGER = logging.getLogger(__name__)

# --- Public config helpers (Airflow Variable -> ENV -> default) -----------------
try:
    from airflow.models import Variable as _AFVariable
except Exception:
    _AFVariable = None


def cfg(key: str, default=None):
    """Read config from Airflow Variable, then ENV, else default."""
    val = None
    if _AFVariable is not None:
        try:
            val = _AFVariable.get(key)
        except Exception:
            val = None
    if val is None:
        val = os.environ.get(key, default)
    return val


def cfg_int(key: str, default: int) -> int:
    v = cfg(key, default)
    try:
        return int(v)
    except Exception:
        return default


def cfg_float(key: str, default: float) -> float:
    v = cfg(key, default)
    try:
        return float(v)
    except Exception:
        return default


def cfg_bool(key: str, default: bool = False) -> bool:
    v = cfg(key, None)
    if v is None:
        return default
    return parse_runtime_bool(v, key)


def _param(default, description: str) -> Param:
    """Create an Airflow Param with a UI-facing description."""
    return Param(default, description=description)


# ----- Helper functions (MUST stay at module top-level) --------------------------


def _stability_ready(ti, key: str, identity: str, snapshot, idle_seconds: int) -> bool:
    """Persist and compare a filesystem snapshot across sensor reschedules."""
    previous = ti.xcom_pull(task_ids=ti.task_id, key=key)
    ready, current = stability_observation(
        previous=previous,
        identity=identity,
        snapshot=snapshot,
        observed_at=time.time(),
        idle_seconds=idle_seconds,
    )
    ti.xcom_push(key=key, value=current)
    return ready


def _candidate_stability_key(policy: str, path: str) -> str:
    """Return a bounded XCom key for one candidate's stability history."""
    normalized = os.path.abspath(os.path.expanduser(path))
    digest = hashlib.sha256(os.fsencode(normalized)).hexdigest()
    return f"candidate_stability_{policy}_{digest}"


def _normalize_folders(monitoring_folders: Iterable[str]) -> Sequence[str]:
    """Return absolute existing directories; ignore non-existing."""
    if isinstance(monitoring_folders, (str, os.PathLike)):
        candidates = [str(monitoring_folders)]
    else:
        candidates = [str(p) for p in monitoring_folders]
    out = []
    for p in candidates:
        ap = os.path.abspath(os.path.expanduser(p))
        if os.path.isdir(ap):
            out.append(ap)
    return out


def _iter_direct_files(root: str) -> Iterable[str]:
    """Yield regular files directly under root."""
    try:
        with os.scandir(root) as entries:
            for entry in entries:
                try:
                    if entry.is_file():
                        yield entry.path
                except FileNotFoundError:
                    continue
    except FileNotFoundError:
        return


def _date_filter_ok(path: str, date_queries) -> bool:
    """
    Accept path if its 'reference date' (folder name or mtime) satisfies ALL queries.

    date_queries can be:
      - None  -> always True
      - string like '>=2025-11-01'
      - list of strings ['>=2025-11-01', '<=2025-11-05']
    """
    LOGGER.debug("COSIDAG date filter: path=%s, date_queries=%s", path, date_queries)
    if not date_queries:
        return True

    last = os.path.basename(os.path.normpath(path))

    # 1) Try parsing date from folder name (YYYYMMDD[_...] or YYYY-MM-DD[_...])
    ref_date = None
    if _looks_like_date_folder(last):
        ds = last.split("_")[0]
        try:
            ref_date = _parse_date_string(ds)
        except Exception as e:
            LOGGER.warning("COSIDAG failed to parse folder date %r: %s", ds, e)

    # 2) Fallback to mtime date
    if ref_date is None:
        try:
            ref_date = datetime.fromtimestamp(os.stat(path).st_mtime).date()
        except Exception as e:
            LOGGER.warning("COSIDAG failed to get mtime for %s: %s", path, e)
            # If we cannot determine a reference date, do not filter out for safety.
            return True

    return _apply_date_queries(ref_date, date_queries)


def _find_new_folder(
    monitoring_folders: Iterable[str],
    level: int,
    dag_id: str,
    date_queries: Optional[str | list[str]] = None,
    only_basename: Optional[str] = None,
    prefer_deepest: bool = True,
    owner_run_id: Optional[str] = None,
    candidate_ready: Optional[Callable[[str, DirectorySnapshot], bool]] = None,
) -> Optional[str]:
    """Return the first available, ready folder across filtered candidates."""
    print(
        "[COSIDAG] _find_new_folder: searching for new folders "
        f"(dag_id={dag_id}, level={level}, date_queries={date_queries}, only_basename={only_basename})"
    )
    roots = _normalize_folders(monitoring_folders)
    if not roots:
        print("[COSIDAG] _find_new_folder: no valid monitoring folders found")
        return None

    print(f"[COSIDAG] _find_new_folder: monitoring {len(roots)} root folder(s): {', '.join(roots)}")
    unavailable = set(list_unavailable_paths(dag_id, owner_run_id=owner_run_id))
    print(f"[COSIDAG] _find_new_folder: loaded {len(unavailable)} unavailable folder(s)")

    candidate_snapshots: dict[str, DirectorySnapshot] = {}
    for root in sorted(roots):
        snapshots = scan_directory_snapshots(root, max_candidate_depth=level)
        print(f"[COSIDAG] _find_new_folder: found {len(snapshots)} subfolder(s) in {root} (max_depth={level})")
        for sub, snapshot in snapshots.items():
            if only_basename and os.path.basename(sub) != only_basename:
                continue
            if _date_filter_ok(sub, date_queries):
                candidate_snapshots[sub] = snapshot

    candidates = list(candidate_snapshots)

    if not candidates:
        print("[COSIDAG] _find_new_folder: no candidates found after filtering")
        return None

    print(f"[COSIDAG] _find_new_folder: {len(candidates)} candidate folder(s) after filtering")

    # Prefer deeper paths first
    if prefer_deepest:
        candidates.sort(key=lambda p: (p.count(os.sep), p), reverse=True)
        print("[COSIDAG] _find_new_folder: sorted candidates by depth (deepest first)")
    else:
        candidates.sort()
        print("[COSIDAG] _find_new_folder: sorted candidates alphabetically")

    available = 0
    for path in candidates:
        if path in unavailable:
            continue
        available += 1
        if candidate_ready is not None and not candidate_ready(path, candidate_snapshots[path]):
            LOGGER.debug("COSIDAG folder candidate is not ready: %s", path)
            continue
        print(f"[COSIDAG] _find_new_folder: found new folder: {path}")
        return path

    print(
        "[COSIDAG] _find_new_folder: no ready candidate found "
        f"({available} available, {len(candidates)} total)"
    )
    return None


def _find_new_file(
    monitoring_folders: Iterable[str],
    dag_id: str,
    date_queries: Optional[str | list[str]] = None,
    only_basename: Optional[str] = None,
    owner_run_id: Optional[str] = None,
) -> Optional[str]:
    """Return the first new direct child file across roots."""
    print(
        "[COSIDAG] _find_new_file: searching for new direct child files "
        f"(dag_id={dag_id}, date_queries={date_queries}, only_basename={only_basename})"
    )
    roots = _normalize_folders(monitoring_folders)
    if not roots:
        print("[COSIDAG] _find_new_file: no valid monitoring folders found")
        return None

    print(f"[COSIDAG] _find_new_file: monitoring {len(roots)} root folder(s): {', '.join(roots)}")
    unavailable = set(list_unavailable_paths(dag_id, owner_run_id=owner_run_id))
    print(f"[COSIDAG] _find_new_file: loaded {len(unavailable)} unavailable file(s)")

    candidates: list[str] = []
    for root in sorted(roots):
        files = sorted(_iter_direct_files(root))
        print(f"[COSIDAG] _find_new_file: found {len(files)} direct file(s) in {root}")
        for file_path in files:
            if only_basename and os.path.basename(file_path) != only_basename:
                continue
            if _date_filter_ok(file_path, date_queries):
                candidates.append(file_path)

    if not candidates:
        print("[COSIDAG] _find_new_file: no candidates found after filtering")
        return None

    print(f"[COSIDAG] _find_new_file: {len(candidates)} candidate file(s) after filtering")
    for path in sorted(candidates):
        if path not in unavailable:
            print(f"[COSIDAG] _find_new_file: found new file: {path}")
            return path

    print(f"[COSIDAG] _find_new_file: all {len(candidates)} candidate(s) unavailable")
    return None


def _normalize_optional_int(value, field_name: str) -> Optional[int]:
    """Return None for unset/blank values, otherwise parse a non-negative int."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be blank or an integer, got {value!r}") from exc
    if parsed < 0:
        raise ValueError(f"{field_name} must be blank or a non-negative integer, got {value!r}")
    return parsed


def _build_data_explorer_url(
    homepage: str,
    detected_path: str,
    policy: str,
    data_root: str,
) -> Optional[str]:
    """Build a URL to the detected folder in the Data Explorer."""
    if not homepage or not detected_path or not data_root:
        return None

    link_path = (
        os.path.dirname(detected_path)
        if policy == "file-driven"
        else detected_path
    )
    root_path = os.path.abspath(os.path.expanduser(data_root))
    if not os.path.isabs(link_path):
        link_path = os.path.join(root_path, link_path)
    link_path = os.path.abspath(os.path.expanduser(link_path))

    try:
        if os.path.commonpath([root_path, link_path]) != root_path:
            return None
    except ValueError:
        return None

    relative_path = os.path.relpath(link_path, root_path)
    parsed_homepage = urlsplit(homepage)
    explorer_path = parsed_homepage.path.rstrip("/")
    if not explorer_path.endswith("/heasarcbrowser"):
        explorer_path = f"{explorer_path}/heasarcbrowser"

    if relative_path == ".":
        target_path = f"{explorer_path}/"
    else:
        url_path = quote(relative_path.replace(os.sep, "/"), safe="/")
        target_path = f"{explorer_path}/folder/{url_path}"

    return urlunsplit(
        (
            parsed_homepage.scheme,
            parsed_homepage.netloc,
            target_path,
            parsed_homepage.query,
            parsed_homepage.fragment,
        )
    )



class ConditionalTriggerDagRunOperator(TriggerDagRunOperator):
    """
    Wraps TriggerDagRunOperator to skip execution if 'auto_retrig' is False in dag_run.conf.
    Also supports max_retrig_runs to limit the number of automatic retriggers.
    """

    def __init__(self, max_retrig_runs=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_retrig_runs = _normalize_optional_int(max_retrig_runs, "max_retrig_runs")

    def execute(self, context):
        dag_run = context.get("dag_run")
        if dag_run is None:
            raise AirflowException("automatic_retrig requires a DagRun context")
        conf = normalize_run_conf(dag_run.conf)

        # Check runtime override
        val = conf.get("auto_retrig")
        if val is not None:
            is_on = parse_runtime_bool(val, "auto_retrig")
            if not is_on:
                print(f"[COSIDAG] Skipping automatic_retrig (dag_run.conf['auto_retrig']={val})")
                return None

        # Check run counter limit before building the successor configuration.
        # Empty strings from the Trigger UI mean "no limit".
        max_retrig_runs = _normalize_optional_int(
            conf.get("max_retrig_runs", self.max_retrig_runs),
            "max_retrig_runs",
        )
        if max_retrig_runs is not None:
            run_count = conf.get("retrig_run_count", 0)
            run_count = int(run_count) if isinstance(run_count, (int, str)) else 0

            if run_count >= max_retrig_runs:
                print(f"[COSIDAG] Skipping automatic_retrig (reached max_retrig_runs={max_retrig_runs}, current_count={run_count})")
                return None

        self.trigger_run_id = automatic_retrigger_run_id(
            dag_id=self.trigger_dag_id,
            source_run_id=dag_run.run_id,
            task_id=self.task_id,
        )
        self.conf = build_successor_conf(conf)
        try:
            return super().execute(context)
        except DagRunAlreadyExists:
            # A retry after an ambiguous worker failure must continue the current
            # scientific chain instead of creating a duplicate successor.
            self.log.info(
                "Successor DagRun %s already exists; treating the retrigger as idempotent",
                self.trigger_run_id,
            )
            return None


# ---- COSIDAG --------------------------------------------------------------------


class COSIDAG(DAG):
    """
    DAG subclass that wires:
      check_new_file -> automatic_retrig -> resolve_inputs -> [custom] ->
      show_results -> finalize_cosidag_state

    Optional steps can be disabled:
      - check_new_file is not created if monitoring_folders is empty.
      - automatic_retrig is not created if auto_retrig is False.
    """

    def __init__(
        self,
        monitoring_folders,
        level: int = 1,
        date: Optional[str] = None,
        date_queries: Optional[str | list[str]] = None,
        build_custom: Optional[Callable[[DAG], None]] = None,
        sensor_poke_seconds: int = 30,
        sensor_timeout_seconds: int = 60 * 60 * 6,
        home_env_var: str = "COSIFLOW_HOME_URL",
        idle_seconds: int = 20,
        min_files: int = 1,
        ready_marker: Optional[str] = None,
        only_basename: Optional[str] = None,
        prefer_deepest: bool = True,
        file_patterns: Optional[dict] = None,  # {"xcom_key": "glob_pattern", ...}
        select_policy: str = "first",  # "first" | "latest_mtime"
        policy: str = "folder-driven",  # "folder-driven" | "file-driven"
        tags: Optional[list[str]] = None,
        default_args_extra: Optional[dict] = None,
        auto_retrig: bool = True,
        max_retrig_runs: Optional[int] = None,
        claim_stale_seconds: int = 86400,
        input_poke_seconds: int = 120,
        input_timeout_seconds: int = 30 * 60,
        *args,
        **kwargs,
    ) -> None:
        # --- merge default_args ---
        # priority: kwargs.default_args < _BASE_DEFAULT_ARGS < default_args_extra
        base = dict(_BASE_DEFAULT_ARGS)
        if "default_args" in kwargs and kwargs["default_args"]:
            base.update(kwargs["default_args"])  # allows override from caller
        if default_args_extra:
            base.update(default_args_extra)  # extensions/override requested

        # ensure that DAG receives the final default_args
        kwargs["default_args"] = base
        policy = normalize_monitoring_policy(policy)
        select_policy = normalize_select_policy(select_policy)
        ready_marker = normalize_relative_runtime_path(ready_marker, "ready_marker")
        prefer_deepest = parse_runtime_bool(prefer_deepest, "prefer_deepest")
        auto_retrig = parse_runtime_bool(auto_retrig, "auto_retrig")

        # --- merge tags ---
        existing_tags = list(kwargs.get("tags", []) or [])
        merged_tags = sorted(set((tags or []) + existing_tags))
        if merged_tags:
            kwargs["tags"] = merged_tags

        super().__init__(*args, **kwargs)

        # Decide whether monitoring is enabled (task existence, not just runtime behavior).
        self.has_monitoring = bool(monitoring_folders)

        self.cosidag_defaults = {
            "monitoring_folders": monitoring_folders,
            "level": int(level),
            "date": date,
            "date_queries": date_queries,
            "home_env_var": home_env_var,
            "input_poke_seconds": int(input_poke_seconds),
            "input_timeout_seconds": int(input_timeout_seconds),
            "idle_seconds": int(idle_seconds),
            "min_files": int(min_files),
            "ready_marker": ready_marker,
            "only_basename": only_basename,
            "prefer_deepest": prefer_deepest,
            "file_patterns": file_patterns,
            "select_policy": select_policy,
            "policy": policy,
            "max_active_runs": int(kwargs.get("max_active_runs", 2)),
            "max_active_tasks": int(kwargs.get("max_active_tasks", 8)),
            "concurrency": int(kwargs.get("concurrency", 8)),
            "auto_retrig": auto_retrig,
            "max_retrig_runs": _normalize_optional_int(max_retrig_runs, "max_retrig_runs"),
            "claim_stale_seconds": int(claim_stale_seconds),
        }

        # Base params (can be overridden by dag_run.conf at runtime)
        self.params.update(
            {
                "monitoring_folders": _param(
                    self.cosidag_defaults["monitoring_folders"],
                    "List of root folders watched by the COSIDAG sensor. "
                    "In folder-driven mode the sensor scans child directories; "
                    "in file-driven mode it scans direct child files.",
                ),
                "level": _param(
                    self.cosidag_defaults["level"],
                    "Maximum child directory depth to scan in folder-driven mode. "
                    "A value of 1 means direct child folders only. File-driven mode always uses direct child files.",
                ),
                "date": _param(
                    self.cosidag_defaults["date"],
                    "Optional exact date filter. When provided, it is treated as a date_queries value like '==YYYYMMDD'.",
                ),
                "date_queries": _param(
                    self.cosidag_defaults["date_queries"],
                    "Optional date filter expression or list of expressions, such as '>=2026-01-01' "
                    "or ['>=2026-01-01', '<=2026-01-31']. The sensor applies these to candidate path names or mtimes.",
                ),
                "home_env_var": _param(
                    self.cosidag_defaults["home_env_var"],
                    "Environment variable that contains the COSIFLOW home URL used by show_results to build links.",
                ),
                "input_poke_seconds": _param(
                    self.cosidag_defaults["input_poke_seconds"],
                    "Polling interval for input-pattern readiness. The sensor releases its worker slot between checks.",
                ),
                "input_timeout_seconds": _param(
                    self.cosidag_defaults["input_timeout_seconds"],
                    "Maximum time to wait for all required input patterns to become stable.",
                ),
                "idle_seconds": _param(
                    self.cosidag_defaults["idle_seconds"],
                    "Minimum time that file size and mtime metadata must remain unchanged before input is stable.",
                ),
                "min_files": _param(
                    self.cosidag_defaults["min_files"],
                    "Minimum number of files required inside a candidate folder in folder-driven mode. "
                    "Ignored by file-driven mode.",
                ),
                "ready_marker": _param(
                    self.cosidag_defaults["ready_marker"],
                    "Optional marker filename that must exist inside a candidate folder before it can be processed. "
                    "Used only in folder-driven mode.",
                ),
                "only_basename": _param(
                    self.cosidag_defaults["only_basename"],
                    "Optional basename filter. If set, only candidate folders or files whose final path component "
                    "matches this value are accepted.",
                ),
                "prefer_deepest": _param(
                    self.cosidag_defaults["prefer_deepest"],
                    "When folder-driven mode finds multiple candidate folders, prefer the deepest path first. "
                    "Ignored by file-driven mode.",
                ),
                "file_patterns": _param(
                    self.cosidag_defaults["file_patterns"],
                    "Optional mapping of XCom keys to glob patterns used by resolve_inputs to find files "
                    "inside the detected folder.",
                ),
                "select_policy": _param(
                    self.cosidag_defaults["select_policy"],
                    "Selection strategy used by resolve_inputs when a file pattern matches multiple files. "
                    "Supported values are 'first' and 'latest_mtime'.",
                ),
                "policy": _param(
                    self.cosidag_defaults["policy"],
                    "Monitoring policy used by check_new_file. Use 'folder-driven' to process candidate folders "
                    "or 'file-driven' to process direct child files.",
                ),
                "max_active_runs": _param(
                    self.cosidag_defaults["max_active_runs"],
                    "Maximum number of active runs allowed for this DAG.",
                ),
                "max_active_tasks": _param(
                    self.cosidag_defaults["max_active_tasks"],
                    "Maximum number of active tasks allowed for this DAG.",
                ),
                "concurrency": _param(
                    self.cosidag_defaults["concurrency"],
                    "Legacy Airflow concurrency limit for this DAG, kept for compatibility with older deployments.",
                ),
                "auto_retrig": _param(
                    self.cosidag_defaults["auto_retrig"],
                    "If true, the automatic_retrig task triggers a new run of the same DAG after a candidate is found. "
                    "If false, automatic retriggering is skipped at runtime.",
                ),
                "max_retrig_runs": _param(
                    "" if self.cosidag_defaults["max_retrig_runs"] is None else self.cosidag_defaults["max_retrig_runs"],
                    "Optional safety limit for automatic retriggers. Leave this field empty for no limit.",
                ),
                "claim_stale_seconds": _param(
                    self.cosidag_defaults["claim_stale_seconds"],
                    "Minimum age before a claim whose owning DagRun is no longer active may be recovered.",
                ),
            }
        )

        self.auto_retrig = auto_retrig
        self.max_retrig_runs = self.cosidag_defaults["max_retrig_runs"]
        if self.cosidag_defaults["claim_stale_seconds"] <= 0:
            raise ValueError("claim_stale_seconds must be positive")
        if self.cosidag_defaults["idle_seconds"] < 0:
            raise ValueError("idle_seconds must be non-negative")
        if int(sensor_poke_seconds) <= 0 or int(sensor_timeout_seconds) <= 0:
            raise ValueError("sensor poke and timeout values must be positive")
        if self.cosidag_defaults["input_poke_seconds"] <= 0:
            raise ValueError("input_poke_seconds must be positive")
        if self.cosidag_defaults["input_timeout_seconds"] <= 0:
            raise ValueError("input_timeout_seconds must be positive")

        print(
            "[COSIDAG] enabled: "
            f"check_new_file={self.has_monitoring}, "
            f"automatic_retrig={self.auto_retrig}, "
            f"resolve_inputs={bool(file_patterns)}, "
            f"policy={policy}"
        )

        # ---------------------------------------------------------------------
        # 1) check_new_file — PythonSensor (optional)
        # ---------------------------------------------------------------------

        def _sensor_poke(ti, **context):
            dag_run = context.get("dag_run")
            if dag_run is None:
                raise AirflowException("check_new_file requires a DagRun context")
            defaults = self.cosidag_defaults
            runtime = validate_sensor_runtime_config(dag_run.conf, defaults)
            monitoring = runtime.monitoring_folders
            level_val = runtime.level
            conf_date_queries = runtime.date_queries
            idle_s = runtime.idle_seconds
            min_f = runtime.min_files
            marker = runtime.ready_marker
            only_bn = runtime.only_basename
            prefer_deep = runtime.prefer_deepest
            selected_policy = runtime.policy
            release_orphaned_claims(self.dag_id, runtime.claim_stale_seconds)

            print(f"[COSIDAG] _sensor_poke: marker={marker}")
            print(f"[COSIDAG] _sensor_poke: idle_seconds={idle_s}, min_files={min_f}")

            def _folder_candidate_ready(path: str, snapshot: DirectorySnapshot) -> bool:
                if marker:
                    marker_path = resolve_confined_relative_path(path, marker)
                    if not os.path.exists(marker_path):
                        return False

                if snapshot[0] < min_f:
                    return False

                ready = _stability_ready(
                    ti=ti,
                    key=_candidate_stability_key("folder", path),
                    identity=f"folder-driven:{path}",
                    snapshot=snapshot,
                    idle_seconds=idle_s,
                )
                if not ready:
                    LOGGER.debug(
                        "COSIDAG folder candidate metadata has not remained "
                        "unchanged for %ss: %s",
                        idle_s,
                        path,
                    )
                return ready

            if selected_policy == "file-driven":
                new_path = _find_new_file(
                    monitoring_folders=monitoring,
                    dag_id=self.dag_id,
                    date_queries=conf_date_queries,
                    only_basename=only_bn,
                    owner_run_id=dag_run.run_id,
                )
            else:
                new_path = _find_new_folder(
                    monitoring_folders=monitoring,
                    level=level_val,
                    date_queries=conf_date_queries,
                    dag_id=self.dag_id,
                    only_basename=only_bn,
                    prefer_deepest=prefer_deep,
                    owner_run_id=dag_run.run_id,
                    candidate_ready=_folder_candidate_ready,
                )

            print(f"[COSIDAG] _sensor_poke: policy={selected_policy}, new_path={new_path}")
            if not new_path:
                return False

            if selected_policy == "file-driven":
                snapshot = file_snapshot(new_path)
                if snapshot is None:
                    return False
                if not _stability_ready(
                    ti=ti,
                    key="candidate_stability",
                    identity=f"{selected_policy}:{new_path}",
                    snapshot=snapshot,
                    idle_seconds=idle_s,
                ):
                    print(
                        "[COSIDAG] _sensor_poke: candidate metadata has not yet "
                        f"remained unchanged for {idle_s}s"
                    )
                    return False

            if not claim_path(
                dag_id=self.dag_id,
                path=new_path,
                owner_run_id=dag_run.run_id,
                monitoring_policy=selected_policy,
            ):
                print(f"[COSIDAG] _sensor_poke: path claimed by another run: {new_path}")
                return False

            print("[COSIDAG] _sensor_poke: claimed path and pushing it to XCom")
            ti.xcom_push(key="detected_path", value=new_path)
            ti.xcom_push(key="monitoring_policy", value=selected_policy)
            if selected_policy == "file-driven":
                ti.xcom_push(key="detected_file", value=new_path)
                ti.xcom_push(key="detected_folder", value=os.path.dirname(new_path))
            else:
                ti.xcom_push(key="detected_folder", value=new_path)
            return True

        check_new_file = None
        if self.has_monitoring:
            check_new_file = PythonSensor(
                task_id="check_new_file",
                poke_interval=sensor_poke_seconds,
                timeout=sensor_timeout_seconds,
                mode="reschedule",
                python_callable=_sensor_poke,
                dag=self,
            )
            self.check_new_file = check_new_file
        else:
            print("[COSIDAG] monitoring_folders empty → check_new_file disabled")
            self.check_new_file = None

        # ---------------------------------------------------------------------
        # 2) automatic_retrig — Trigger this same DAG again (optional)
        # ---------------------------------------------------------------------

        automatic_retrig = None
        if self.auto_retrig:
            trig_kwargs = {
                "task_id": "automatic_retrig",
                "trigger_dag_id": self.dag_id,
                "reset_dag_run": False,
                "wait_for_completion": False,
                "conf": {},
                "dag": self,
            }

            trig_kwargs["max_retrig_runs"] = self.max_retrig_runs
            automatic_retrig = ConditionalTriggerDagRunOperator(**trig_kwargs)
            self.automatic_retrig = automatic_retrig
        else:
            self.automatic_retrig = None

        # ---------------------------------------------------------------------
        # 3) resolve_inputs (optional)
        # ---------------------------------------------------------------------
        resolve_inputs = None
        if file_patterns:
            def _resolve_inputs_poke(**context):
                ti = context["ti"]
                dag_run = context.get("dag_run")
                conf = normalize_run_conf(dag_run.conf) if dag_run else {}

                # Prefer XCom from check_new_file, but allow manual runs by passing detected_folder in conf.
                run_dir = None
                if check_new_file is not None:
                    run_dir = ti.xcom_pull(task_ids="check_new_file", key="detected_folder")
                if not run_dir:
                    run_dir = conf.get("detected_folder")

                if not run_dir or not os.path.isdir(run_dir):
                    raise AirflowFailException(f"[resolve_inputs] invalid run_dir: {run_dir}")

                inventory = scan_file_inventory(run_dir)
                selected, missing = resolve_patterns(
                    inventory=inventory,
                    patterns=file_patterns,
                    select_policy=select_policy,
                )
                if missing:
                    print(
                        "[resolve_inputs] waiting for required pattern keys: "
                        + ", ".join(sorted(missing))
                    )
                    return False

                snapshot = [
                    [key, record.path, record.size, record.mtime_ns]
                    for key, record in sorted(selected.items())
                ]
                idle_s = int(conf.get("idle_seconds", self.cosidag_defaults["idle_seconds"]))
                if not _stability_ready(
                    ti=ti,
                    key="input_stability",
                    identity=os.path.abspath(run_dir),
                    snapshot=snapshot,
                    idle_seconds=idle_s,
                ):
                    print(
                        "[resolve_inputs] selected file metadata has not yet "
                        f"remained unchanged for {idle_s}s"
                    )
                    return False

                for key, record in selected.items():
                    ti.xcom_push(key=key, value=record.path)
                    print(f"[resolve_inputs] {key} = {record.path} (stable)")

                # Also republish run_dir for convenience.
                ti.xcom_push(key="run_dir", value=run_dir)
                print(f"[resolve_inputs] run_dir = {run_dir}")
                return True

            resolve_inputs = PythonSensor(
                task_id="resolve_inputs",
                python_callable=_resolve_inputs_poke,
                poke_interval=self.cosidag_defaults["input_poke_seconds"],
                timeout=self.cosidag_defaults["input_timeout_seconds"],
                mode="reschedule",
                dag=self,
            )

        # ---------------------------------------------------------------------
        # 4) [custom] — Let users append their tasks (optional)
        # ---------------------------------------------------------------------

        before_tasks = set(self.task_dict.keys())
        if callable(build_custom):
            build_custom(self)
        after_tasks = set(self.task_dict.keys())
        new_ids = sorted(after_tasks - before_tasks)

        if new_ids:
            new_set = set(new_ids)
            new_tasks = [self.task_dict[t] for t in new_ids]

            roots, leaves = [], []
            for t in new_tasks:
                ups = {u.task_id for u in t.upstream_list}
                if ups.isdisjoint(new_set):
                    roots.append(t)
            for t in new_tasks:
                downs = {d.task_id for d in t.downstream_list}
                if downs.isdisjoint(new_set):
                    leaves.append(t)

            last_custom = EmptyOperator(task_id="custom_anchor", dag=self)
            for t in leaves:
                t >> last_custom
        else:
            # No custom tasks created
            roots = []
            last_custom = EmptyOperator(task_id="custom_placeholder", dag=self)

        # ---------------------------------------------------------------------
        # 5) show_results — Log homepage and optional deep link
        # ---------------------------------------------------------------------

        def _show_results(**context):
            ti = context["ti"]
            dag_run = context.get("dag_run")
            conf = normalize_run_conf(dag_run.conf) if dag_run else {}

            # -------------------------------------------------
            # 1) Retrieve detected path
            # -------------------------------------------------
            detected = None
            detected_file = None
            policy_value = None

            if check_new_file is not None:
                policy_value = ti.xcom_pull(
                    task_ids="check_new_file",
                    key="monitoring_policy"
                )
                detected = ti.xcom_pull(
                    task_ids="check_new_file",
                    key="detected_path"
                )
                detected_file = ti.xcom_pull(
                    task_ids="check_new_file",
                    key="detected_file"
                )
            if check_new_file is not None and not detected:
                detected = ti.xcom_pull(
                    task_ids="check_new_file",
                    key="detected_folder"
                )
            elif check_new_file is None:
                detected = ti.xcom_pull(
                    key="detected_folder"
                )

            # Allow manual runs
            if not detected:
                detected = conf.get("detected_path") or conf.get("detected_file") or conf.get("detected_folder")
            if not detected_file:
                detected_file = conf.get("detected_file") if conf else None
            if not policy_value:
                policy_value = conf.get(
                    "monitoring_policy",
                    conf.get("policy", self.cosidag_defaults.get("policy", "folder-driven")),
                )
            policy_value = normalize_monitoring_policy(policy_value)

            if not detected:
                print(
                    "[COSIDAG] No detected path available "
                    "(monitoring disabled and no detected path in dag_run.conf)"
                )
                return None

            # -------------------------------------------------
            # 2) Build deep-link URL (if possible)
            # -------------------------------------------------
            homepage = os.environ.get(
                self.cosidag_defaults.get("home_env_var", "COSIFLOW_HOME_URL")
            ) or os.environ.get("COSIFLOW_HOME_URL")

            url = None
            if homepage:
                data_root = os.environ.get(
                    "COSI_DATA_DIR",
                    "/home/gamma/workspace/data",
                )
                url = _build_data_explorer_url(
                    homepage=homepage,
                    detected_path=detected,
                    policy=policy_value,
                    data_root=data_root,
                )
                if not url:
                    print(
                        "[COSIDAG] Cannot build Data Explorer URL: "
                        f"detected path {detected!r} is outside "
                        f"COSI_DATA_DIR {data_root!r}"
                    )

            # -------------------------------------------------
            # 3) Push structured result to XCom (canonical output)
            # -------------------------------------------------
            result = {
                "path": detected,
                "folder": os.path.dirname(detected) if policy_value == "file-driven" else detected,
                "file": detected_file or (detected if policy_value == "file-driven" else None),
                "policy": policy_value,
                "url": url,
            }

            ti.xcom_push(
                key="cosidag_result",
                value=result,
            )

            # -------------------------------------------------
            # 4) Human-friendly logs
            # -------------------------------------------------
            print("=" * 80)
            print("📂 COSIDAG RESULT")
            print(f"Policy: {policy_value}")
            print(f"Path:   {detected}")
            if url:
                print(f"URL:    {url}")
            else:
                print("URL:    <not available>")
            print("=" * 80)

            # -------------------------------------------------
            # 5) Optional deep-link via PathInfo (fallback / enrichment)
            # -------------------------------------------------
            if not url and PathInfo is not None:
                try:
                    info = PathInfo.from_path(detected)  # type: ignore[attr-defined]
                    if callable(build_url_fragment) and homepage:
                        frag = build_url_fragment(info)  # type: ignore
                        deep = f"{homepage.rstrip('/')}/{frag.lstrip('/')}"
                        print(f"[COSIDAG] Result page (PathInfo): {deep}")
                except Exception as e:
                    print(f"[COSIDAG] Deep-linking via PathInfo failed: {e}")

            # -------------------------------------------------
            # 6) Return value (kept for backward compatibility)
            # -------------------------------------------------
            return result


        show_results = PythonOperator(
            task_id="show_results",
            python_callable=_show_results,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            dag=self,
        )

        # ---------------------------------------------------------------------
        # 6) finalize_cosidag_state — Commit only completed scientific work
        # ---------------------------------------------------------------------
        finalize_cosidag_state = None
        if check_new_file is not None:
            def _finalize_cosidag_state(**context):
                ti = context["ti"]
                dag_run = context["dag_run"]
                detected_path = ti.xcom_pull(
                    task_ids="check_new_file",
                    key="detected_path",
                )
                show_results_ti = dag_run.get_task_instance(task_id="show_results")
                show_results_state = getattr(show_results_ti, "state", None)

                if show_results_state == "success":
                    if not detected_path:
                        raise AirflowFailException(
                            "COSIDAG succeeded without a detected path to finalize"
                        )
                    if not mark_path_succeeded(
                        self.dag_id,
                        detected_path,
                        dag_run.run_id,
                    ):
                        raise AirflowFailException(
                            "COSIDAG state claim is missing or owned by another run"
                        )
                    return {"path": detected_path, "status": "succeeded"}

                if detected_path:
                    mark_path_failed(
                        self.dag_id,
                        detected_path,
                        dag_run.run_id,
                        f"show_results state={show_results_state or 'missing'}",
                    )
                raise AirflowFailException(
                    "Required COSIDAG work did not succeed; the input remains retryable"
                )

            finalize_cosidag_state = PythonOperator(
                task_id="finalize_cosidag_state",
                python_callable=_finalize_cosidag_state,
                trigger_rule=TriggerRule.ALL_DONE,
                dag=self,
            )

        # ---------------------------------------------------------------------
        # Wiring — Build a robust chain depending on what exists.
        # ---------------------------------------------------------------------

        # Start anchor: the last "pre-custom" task that exists.
        anchor = None

        if check_new_file is not None and automatic_retrig is not None:
            check_new_file >> automatic_retrig
            anchor = automatic_retrig
        elif check_new_file is not None:
            anchor = check_new_file
        elif automatic_retrig is not None:
            # Note: without check_new_file, retrigger is still allowed (manual DAG that loops),
            # but it is usually not recommended unless you pass detected_folder in conf.
            anchor = automatic_retrig

        # Optional resolve_inputs comes after anchor if anchor exists; otherwise it can run standalone.
        if resolve_inputs is not None:
            if anchor is not None:
                anchor >> resolve_inputs
            anchor = resolve_inputs

        # Attach custom roots after anchor if possible.
        if roots and anchor is not None:
            for t in roots:
                anchor >> t

        # Close chain into show_results
        if anchor is not None:
            anchor >> last_custom >> show_results
        else:
            # No monitoring, no retrigger, no resolve_inputs: run custom (or placeholder) then show results.
            last_custom >> show_results

        if finalize_cosidag_state is not None:
            show_results >> finalize_cosidag_state

        # Expose handles
        self.show_results = show_results
        self.finalize_cosidag_state = finalize_cosidag_state

    def find_file_by_pattern(self, pattern: str, detected_folder: str) -> Optional[str]:
        """Find the first file matching the given regex pattern under detected_folder."""
        print(f"[COSIDAG] find_file_by_pattern: pattern={pattern}, detected_folder={detected_folder}")
        rx = re.compile(pattern)
        for root, _, files in os.walk(detected_folder):
            for fname in files:
                if rx.search(fname):
                    return os.path.join(root, fname)
        return None


# ------------------------------ Example usage ------------------------------------
# Put the following into your DAG file under the Airflow 'dags/' directory.
#
# from datetime import datetime
# from cosidag import COSIDAG
# from airflow.operators.python import PythonOperator
#
# def build_custom(dag):
#     def _process_folder(folder_path: str):
#         print(f"Processing folder: {folder_path}")
#
#     PythonOperator(
#         task_id="custom_process",
#         python_callable=lambda ti, **_: _process_folder(
#             ti.xcom_pull(task_ids="check_new_file", key="detected_folder")
#         ),
#         dag=dag,
#     )
#
# with COSIDAG(
#     dag_id="cosipipe_example",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     monitoring_folders=["/data/incoming", "/data/alt"],
#     level=3,
#     only_basename="products",
#     idle_seconds=30,
#     min_files=1,
#     date=None,
#     auto_retrig=False,   # disable retrigger
#     build_custom=build_custom,
# ) as dag:
#     pass
#
# Manual run without monitoring:
#   monitoring_folders=[]
#   and trigger with dag_run.conf = {"detected_folder": "/path/to/process"}
