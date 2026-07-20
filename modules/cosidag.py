"""
COSIDAG — a convenience DAG subclass that wires a standard layout:

  1) check_new_file  ->  2) automatic_retrig  ->  3) resolve_inputs  ->  4) [custom tasks]  ->  5) show_results

This implementation supports disabling optional steps:

- check_new_file is NOT created if monitoring_folders is empty / None.
- automatic_retrig is NOT created if auto_retrig is False.

The chaining logic adapts automatically depending on which tasks exist.

Notes
------
* Requires Airflow 2.x.
* Environment: define COSIFLOW_HOME_URL (in your .env) to point to the web UI homepage.
* State: a Variable named f"COSIDAG_PROCESSED::{dag_id}" is used to track processed
  paths across runs. In folder-driven mode it stores folder paths; in file-driven
  mode it stores file paths.
  * To clear the processed paths, delete the Variable, with the command:
    airflow variables set COSIDAG_PROCESSED::{cosidag_id} []
* Date queries: use date_queries (e.g. '>=2025-11-01' or ['>=2025-11-01','<=2025-11-05']).
* Only basename: if only_basename is provided, it only accepts candidate paths with the given basename.
* Prefer deepest: if prefer_deepest is True, it prefers the deepest subfolder.
* File patterns: if file_patterns is provided, it searches for files matching the given patterns
  using glob recursion and selects according to select_policy.
* Path helper: if available, the module cosiflow.modules.path is used to parse/build
  URL fragments from detected folders. The code degrades gracefully if not found.
"""
from __future__ import annotations

import os
import json
import re
import time
from datetime import datetime
from typing import Callable, Iterable, Optional, Sequence

from airflow import DAG
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

_BASE_DEFAULT_ARGS = {
    "owner": "cosiflow",
    "email_on_failure": True,
    "on_failure_callback": notify_email,  # from callbacks/on_failure_callback.py
}

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
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _param(default, description: str) -> Param:
    """Create an Airflow Param with a UI-facing description."""
    return Param(default, description=description)


# ----- Helper functions (MUST stay at module top-level) --------------------------


def _dir_stats(path: str):
    """Return (count, total_size_bytes, latest_mtime) across all files under path."""
    count = 0
    total = 0
    latest = 0.0
    for root, _, files in os.walk(path):
        for fn in files:
            fp = os.path.join(root, fn)
            try:
                st = os.stat(fp)
            except FileNotFoundError:
                continue
            count += 1
            total += st.st_size
            if st.st_mtime > latest:
                latest = st.st_mtime
    return count, total, latest


def _is_dir_stable(path: str, idle_seconds: int, min_files: int) -> bool:
    """True if dir has >= min_files and last write is older than idle_seconds."""
    count, _, latest = _dir_stats(path)
    if count < min_files:
        return False
    return (time.time() - latest) >= idle_seconds


def _is_file_stable(path: str, idle_seconds: int) -> bool:
    """True if file exists and its last write is older than idle_seconds."""
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return False
    if not os.path.isfile(path):
        return False
    return (time.time() - st.st_mtime) >= idle_seconds


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


def _iter_subfolders(root: str, max_depth: int) -> Iterable[str]:
    """Yield subfolders under root up to max_depth (depth 1 = direct children)."""
    root_depth = root.rstrip(os.sep).count(os.sep)
    for current_root, dirs, _ in os.walk(root):
        current_depth = current_root.rstrip(os.sep).count(os.sep) - root_depth
        if current_depth > max_depth:
            dirs[:] = []
            continue
        if current_depth >= 1:
            yield current_root


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
    print(f"[COSIDAG] _date_filter_ok: path={path}, date_queries={date_queries}")
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
            print(f"[COSIDAG] _date_filter_ok: failed to parse folder date {ds!r}: {e}")

    # 2) Fallback to mtime date
    if ref_date is None:
        try:
            ref_date = datetime.fromtimestamp(os.stat(path).st_mtime).date()
        except Exception as e:
            print(f"[COSIDAG] _date_filter_ok: failed to get mtime for {path}: {e}")
            # If we cannot determine a reference date, do not filter out for safety.
            return True

    return _apply_date_queries(ref_date, date_queries)


def _load_processed_set(dag_id: str) -> set:
    """Load processed paths set from Airflow Variable."""
    key = f"COSIDAG_PROCESSED::{dag_id}"
    raw = Variable.get(key, default_var="[]")
    try:
        return set(json.loads(raw))
    except Exception:
        return set()


def _save_processed_set(dag_id: str, processed: set) -> None:
    """Save processed paths set to Airflow Variable."""
    key = f"COSIDAG_PROCESSED::{dag_id}"
    Variable.set(key, json.dumps(sorted(processed)))


def _find_new_folder(
    monitoring_folders: Iterable[str],
    level: int,
    dag_id: str,
    date_queries: Optional[str | list[str]] = None,
    only_basename: Optional[str] = None,
    prefer_deepest: bool = True,
) -> Optional[str]:
    """Return the first new folder across roots (filtered & depth-limited)."""
    print(
        "[COSIDAG] _find_new_folder: searching for new folders "
        f"(dag_id={dag_id}, level={level}, date_queries={date_queries}, only_basename={only_basename})"
    )
    roots = _normalize_folders(monitoring_folders)
    if not roots:
        print("[COSIDAG] _find_new_folder: no valid monitoring folders found")
        return None

    print(f"[COSIDAG] _find_new_folder: monitoring {len(roots)} root folder(s): {', '.join(roots)}")
    processed = _load_processed_set(dag_id)
    print(f"[COSIDAG] _find_new_folder: loaded {len(processed)} already processed folder(s)")

    candidates: list[str] = []
    for root in sorted(roots):
        subfolders = list(_iter_subfolders(root, max_depth=level))  # materialize once
        print(f"[COSIDAG] _find_new_folder: found {len(subfolders)} subfolder(s) in {root} (max_depth={level})")
        for sub in subfolders:
            if only_basename and os.path.basename(sub) != only_basename:
                continue
            if _date_filter_ok(sub, date_queries):
                candidates.append(sub)

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

    for path in candidates:
        if path not in processed:
            print(f"[COSIDAG] _find_new_folder: found new folder: {path}")
            return path

    print(f"[COSIDAG] _find_new_folder: all {len(candidates)} candidate(s) already processed")
    return None


def _find_new_file(
    monitoring_folders: Iterable[str],
    dag_id: str,
    date_queries: Optional[str | list[str]] = None,
    only_basename: Optional[str] = None,
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
    processed = _load_processed_set(dag_id)
    print(f"[COSIDAG] _find_new_file: loaded {len(processed)} already processed file(s)")

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
        if path not in processed:
            print(f"[COSIDAG] _find_new_file: found new file: {path}")
            return path

    print(f"[COSIDAG] _find_new_file: all {len(candidates)} candidate(s) already processed")
    return None


def _normalize_monitoring_policy(policy: Optional[str]) -> str:
    """Normalize and validate the COSIDAG monitoring policy."""
    normalized = str(policy or "folder-driven").strip().lower()
    if normalized not in {"folder-driven", "file-driven"}:
        raise ValueError(
            f"Unsupported COSIDAG monitoring policy {policy!r}. "
            "Expected 'folder-driven' or 'file-driven'."
        )
    return normalized


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
        conf = (dag_run.conf or {}) if dag_run else {}

        # Check runtime override
        val = conf.get("auto_retrig")
        if val is not None:
            is_on = val
            if isinstance(val, str):
                is_on = val.strip().lower() in {"1", "true", "t", "yes", "y", "on"}

            if not is_on:
                print(f"[COSIDAG] Skipping automatic_retrig (dag_run.conf['auto_retrig']={val})")
                return None

        # Check run counter limit (before increment, which happens in the template).
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

        return super().execute(context)


# ---- COSIDAG --------------------------------------------------------------------


class COSIDAG(DAG):
    """
    DAG subclass that wires:
      check_new_file -> automatic_retrig -> resolve_inputs -> [custom] -> show_results

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
        policy = _normalize_monitoring_policy(policy)

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
            "idle_seconds": int(idle_seconds),
            "min_files": int(min_files),
            "ready_marker": ready_marker,
            "only_basename": only_basename,
            "prefer_deepest": bool(prefer_deepest),
            "file_patterns": file_patterns,
            "select_policy": select_policy,
            "policy": policy,
            "max_active_runs": int(kwargs.get("max_active_runs", 2)),
            "max_active_tasks": int(kwargs.get("max_active_tasks", 8)),
            "concurrency": int(kwargs.get("concurrency", 8)),
            "auto_retrig": bool(auto_retrig),
            "max_retrig_runs": _normalize_optional_int(max_retrig_runs, "max_retrig_runs"),
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
                "idle_seconds": _param(
                    self.cosidag_defaults["idle_seconds"],
                    "Minimum number of seconds since the last write before a candidate folder or file is considered stable.",
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
            }
        )

        self.auto_retrig = bool(auto_retrig)
        self.max_retrig_runs = self.cosidag_defaults["max_retrig_runs"]

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
            conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
            defaults = self.cosidag_defaults
            monitoring = conf.get("monitoring_folders", defaults["monitoring_folders"])
            level_val = int(conf.get("level", defaults["level"]))

            # Date queries: runtime conf has precedence.
            conf_date_queries = conf.get("date_queries", None)
            if conf_date_queries is None:
                # fallback: use the optional "date" as '==date'
                conf_date = conf.get("date", defaults.get("date"))
                if conf_date:
                    conf_date_queries = f"=={conf_date}"
                else:
                    conf_date_queries = defaults.get("date_queries")

            idle_s = int(conf.get("idle_seconds", defaults.get("idle_seconds", 20)))
            min_f = int(conf.get("min_files", defaults.get("min_files", 1)))
            marker = conf.get("ready_marker", defaults.get("ready_marker"))
            only_bn = conf.get("only_basename", defaults.get("only_basename"))
            prefer_deep = bool(conf.get("prefer_deepest", defaults.get("prefer_deepest", True)))
            selected_policy = _normalize_monitoring_policy(
                conf.get("monitoring_policy", conf.get("policy", defaults.get("policy", "folder-driven")))
            )

            if selected_policy == "file-driven":
                new_path = _find_new_file(
                    monitoring_folders=monitoring,
                    dag_id=self.dag_id,
                    date_queries=conf_date_queries,
                    only_basename=only_bn,
                )
            else:
                new_path = _find_new_folder(
                    monitoring_folders=monitoring,
                    level=level_val,
                    date_queries=conf_date_queries,
                    dag_id=self.dag_id,
                    only_basename=only_bn,
                    prefer_deepest=prefer_deep,
                )

            print(f"[COSIDAG] _sensor_poke: policy={selected_policy}, new_path={new_path}")
            if not new_path:
                return False

            print(f"[COSIDAG] _sensor_poke: marker={marker}")
            if marker and selected_policy == "folder-driven":
                marker_path = os.path.join(new_path, marker)
                if not os.path.exists(marker_path):
                    return False

            print(f"[COSIDAG] _sensor_poke: idle_seconds={idle_s}, min_files={min_f}")
            if selected_policy == "file-driven":
                if not _is_file_stable(new_path, idle_seconds=idle_s):
                    return False
            elif not _is_dir_stable(new_path, idle_seconds=idle_s, min_files=min_f):
                return False

            print("[COSIDAG] _sensor_poke: pushing detected path to XCom")
            ti.xcom_push(key="detected_path", value=new_path)
            ti.xcom_push(key="monitoring_policy", value=selected_policy)
            if selected_policy == "file-driven":
                ti.xcom_push(key="detected_file", value=new_path)
                ti.xcom_push(key="detected_folder", value=os.path.dirname(new_path))
            else:
                ti.xcom_push(key="detected_folder", value=new_path)
            processed = _load_processed_set(self.dag_id)
            processed.add(new_path)
            _save_processed_set(self.dag_id, processed)
            return True

        check_new_file = None
        if self.has_monitoring:
            check_new_file = PythonSensor(
                task_id="check_new_file",
                poke_interval=sensor_poke_seconds,
                timeout=sensor_timeout_seconds,
                mode="poke",
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

        def _unique_run_id() -> str:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
            return f"auto::{self.dag_id}::{ts}"

        automatic_retrig = None
        if self.auto_retrig:
            import inspect

            trig_kwargs = {
                "task_id": "automatic_retrig",
                "trigger_dag_id": self.dag_id,
                "reset_dag_run": False,
                "wait_for_completion": False,
                "dag": self,
            }

            # Propagate conf from previous run and increment the retrigger counter.
            trig_kwargs["conf"] = """{% set current_conf = dag_run.conf if dag_run and dag_run.conf else {} %}
{% set run_count = current_conf.get('retrig_run_count', 0) | int %}
{% set new_conf = current_conf.copy() %}
{% set _ = new_conf.update({'retrig_run_count': run_count + 1}) %}
{{ new_conf | tojson }}"""

            # Airflow version differences
            params = inspect.signature(TriggerDagRunOperator.__init__).parameters
            if "trigger_run_id" in params:
                trig_kwargs["trigger_run_id"] = _unique_run_id()
            elif "run_id" in params:
                trig_kwargs["run_id"] = _unique_run_id()

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
            import glob
            from airflow.exceptions import AirflowFailException

            def _resolve_inputs(**context):
                ti = context["ti"]
                dag_run = context.get("dag_run")
                conf = (dag_run.conf or {}) if dag_run else {}

                # Prefer XCom from check_new_file, but allow manual runs by passing detected_folder in conf.
                run_dir = None
                if check_new_file is not None:
                    run_dir = ti.xcom_pull(task_ids="check_new_file", key="detected_folder")
                if not run_dir:
                    run_dir = conf.get("detected_folder")

                if not run_dir or not os.path.isdir(run_dir):
                    raise AirflowFailException(f"[resolve_inputs] invalid run_dir: {run_dir}")

                def pick_one(paths: list[str]) -> Optional[str]:
                    if not paths:
                        return None
                    if select_policy == "first":
                        return sorted(paths)[0]
                    if select_policy == "latest_mtime":
                        return max(paths, key=lambda p: os.stat(p).st_mtime)
                    return sorted(paths)[0]

                def _can_open_file(file_path: str) -> bool:
                    """Try to open the file in read mode. Returns True if successful, False otherwise."""
                    if not os.path.exists(file_path):
                        return False
                    try:
                        # Try to open the file in read mode
                        # This will fail if the file is still being written or locked by another process
                        with open(file_path, 'rb') as f:
                            # Try to read at least one byte to ensure file is readable
                            f.read(1)
                        return True
                    except (IOError, OSError, PermissionError, FileNotFoundError) as e:
                        print(f"[resolve_inputs] Cannot open file {file_path}: {e}")
                        return False

                # First pass: find all required files
                found_files = {}
                for key, pattern in file_patterns.items():
                    if isinstance(pattern, str) and pattern.startswith("regex:"):
                        rx = re.compile(pattern[len("regex:"):])
                        candidates = glob.glob(os.path.join(run_dir, "**", "*"), recursive=True)
                        matches = sorted(
                            path for path in candidates
                            if os.path.isfile(path) and rx.match(os.path.basename(path))
                        )
                    else:
                        matches = sorted(glob.glob(os.path.join(run_dir, "**", pattern), recursive=True))
                    chosen = pick_one(matches)
                    if not chosen:
                        raise AirflowFailException(
                            f"[resolve_inputs] no file for key={key!r} pattern={pattern!r} under {run_dir}"
                        )
                    found_files[key] = chosen
                    print(f"[resolve_inputs] Found {key} = {chosen}")

                # Second pass: wait for all files to be completely written (can be opened)
                print(f"[resolve_inputs] Waiting for all {len(found_files)} files to be completely written...")
                retry_interval = 120  # Wait 2 minutes between retries
                max_wait_seconds = 1800  # Maximum 30 minutes total wait
                start_time = time.time()
                attempt = 0
                
                while (time.time() - start_time) < max_wait_seconds:
                    attempt += 1
                    all_ready = True
                    unready_files = []
                    ready_files = []
                    
                    # Check each file individually
                    for key, file_path in found_files.items():
                        print(f"[resolve_inputs] Checking file {key}: {file_path}")
                        if _can_open_file(file_path):
                            ready_files.append(key)
                            print(f"[resolve_inputs] ✓ File {key} is ready and can be opened")
                        else:
                            all_ready = False
                            unready_files.append(key)
                            print(f"[resolve_inputs] ✗ File {key} is still being written or locked")
                    
                    if all_ready:
                        print(f"[resolve_inputs] All {len(found_files)} files are ready and can be opened (attempt {attempt})")
                        break
                    
                    elapsed = time.time() - start_time
                    print(f"[resolve_inputs] Attempt {attempt}: {len(ready_files)}/{len(found_files)} files ready. "
                          f"Still waiting for: {unready_files}. "
                          f"Elapsed: {elapsed:.1f}s. Retrying in {retry_interval}s...")
                    time.sleep(retry_interval)
                else:
                    # Timeout reached - check each file one more time to report final status
                    print(f"[resolve_inputs] Timeout reached. Checking final status of all files...")
                    still_unready = []
                    for key, file_path in found_files.items():
                        if not _can_open_file(file_path):
                            still_unready.append(key)
                            print(f"[resolve_inputs] ✗ File {key} ({file_path}) still cannot be opened")
                    
                    if still_unready:
                        raise AirflowFailException(
                            f"[resolve_inputs] Timeout ({max_wait_seconds}s) waiting for files to be ready. "
                            f"Files still cannot be opened: {still_unready}"
                        )

                # All files are ready, push to XCom
                for key, file_path in found_files.items():
                    ti.xcom_push(key=key, value=file_path)
                    print(f"[resolve_inputs] {key} = {file_path} (ready)")

                # Also republish run_dir for convenience.
                ti.xcom_push(key="run_dir", value=run_dir)
                print(f"[resolve_inputs] run_dir = {run_dir}")

            resolve_inputs = PythonOperator(
                task_id="resolve_inputs",
                python_callable=_resolve_inputs,
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
            conf = (dag_run.conf or {}) if dag_run else {}

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
            policy_value = _normalize_monitoring_policy(policy_value)

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
                # ⚠️ Adapt this base path to your filesystem layout
                DATA_ROOT = "/home/gamma/workspace/data"
                link_path = os.path.dirname(detected) if policy_value == "file-driven" else detected
                rel = link_path.replace(DATA_ROOT, "").lstrip("/")
                url = f"{homepage.rstrip('/')}/folder/{rel}"

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

        # Expose handles
        self.show_results = show_results

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
# from cosiflow.cosidag import COSIDAG
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
