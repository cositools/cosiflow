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
  folder paths across runs, to avoid reprocessing the same folder.
  * To clear the processed folder paths, delete the Variable, with the command:
    airflow variables set COSIDAG_PROCESSED::{cosidag_id} []
* Date queries: use date_queries (e.g. '>=2025-11-01' or ['>=2025-11-01','<=2025-11-05']).
* Only basename: if only_basename is provided, it only accepts subfolders with the given basename.
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
        tags: Optional[list[str]] = None,
        default_args_extra: Optional[dict] = None,
        auto_retrig: bool = True,
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

        # --- merge tags ---
        existing_tags = list(kwargs.get("tags", []) or [])
        merged_tags = sorted(set((tags or []) + existing_tags))
        if merged_tags:
            kwargs["tags"] = merged_tags

        super().__init__(*args, **kwargs)

        # Decide whether monitoring is enabled (task existence, not just runtime behavior).
        self.has_monitoring = bool(monitoring_folders)

        # Base params (can be overridden by dag_run.conf at runtime)
        self.params.update(
            {
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
                "max_active_runs": int(kwargs.get("max_active_runs", 2)),
                "max_active_tasks": int(kwargs.get("max_active_tasks", 8)),
                "concurrency": int(kwargs.get("concurrency", 8)),
                "auto_retrig": bool(auto_retrig),
            }
        )

        self.auto_retrig = bool(auto_retrig)

        print(
            "[COSIDAG] enabled: "
            f"check_new_file={self.has_monitoring}, "
            f"automatic_retrig={self.auto_retrig}, "
            f"resolve_inputs={bool(file_patterns)}"
        )

        # ---------------------------------------------------------------------
        # 1) check_new_file — PythonSensor (optional)
        # ---------------------------------------------------------------------

        def _sensor_poke(ti, **context):
            conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
            monitoring = conf.get("monitoring_folders", self.params["monitoring_folders"])
            level_val = int(conf.get("level", self.params["level"]))

            # Date queries: runtime conf has precedence.
            conf_date_queries = conf.get("date_queries", None)
            if conf_date_queries is None:
                # fallback: use the optional "date" as '==date'
                conf_date = conf.get("date", self.params.get("date"))
                if conf_date:
                    conf_date_queries = f"=={conf_date}"
                else:
                    conf_date_queries = self.params.get("date_queries")

            idle_s = int(conf.get("idle_seconds", self.params.get("idle_seconds", 20)))
            min_f = int(conf.get("min_files", self.params.get("min_files", 1)))
            marker = conf.get("ready_marker", self.params.get("ready_marker"))
            only_bn = conf.get("only_basename", self.params.get("only_basename"))
            prefer_deep = bool(conf.get("prefer_deepest", self.params.get("prefer_deepest", True)))

            new_path = _find_new_folder(
                monitoring_folders=monitoring,
                level=level_val,
                date_queries=conf_date_queries,
                dag_id=self.dag_id,
                only_basename=only_bn,
                prefer_deepest=prefer_deep,
            )

            print(f"[COSIDAG] _sensor_poke: new_path={new_path}")
            if not new_path:
                return False

            print(f"[COSIDAG] _sensor_poke: marker={marker}")
            if marker:
                marker_path = os.path.join(new_path, marker)
                if not os.path.exists(marker_path):
                    return False

            print(f"[COSIDAG] _sensor_poke: idle_seconds={idle_s}, min_files={min_f}")
            if not _is_dir_stable(new_path, idle_seconds=idle_s, min_files=min_f):
                return False

            print("[COSIDAG] _sensor_poke: pushing detected_folder to XCom")
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

            # Propagate conf from previous run — must be valid JSON.
            trig_kwargs["conf"] = "{{ dag_run.conf | tojson if dag_run and dag_run.conf else '{}' }}"

            # Airflow version differences
            params = inspect.signature(TriggerDagRunOperator.__init__).parameters
            if "trigger_run_id" in params:
                trig_kwargs["trigger_run_id"] = _unique_run_id()
            elif "run_id" in params:
                trig_kwargs["run_id"] = _unique_run_id()

            automatic_retrig = TriggerDagRunOperator(**trig_kwargs)
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

                for key, pattern in file_patterns.items():
                    matches = sorted(glob.glob(os.path.join(run_dir, "**", pattern), recursive=True))
                    chosen = pick_one(matches)
                    if not chosen:
                        raise AirflowFailException(
                            f"[resolve_inputs] no file for key={key!r} pattern={pattern!r} under {run_dir}"
                        )
                    ti.xcom_push(key=key, value=chosen)
                    print(f"[resolve_inputs] {key} = {chosen}")

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
            # 1) Retrieve detected folder
            # -------------------------------------------------
            detected = None

            if check_new_file is not None:
                detected = ti.xcom_pull(
                    task_ids="check_new_file",
                    key="detected_folder"
                )
            else:
                detected = ti.xcom_pull(
                    key="detected_folder"
                )

            # Allow manual runs
            if not detected:
                detected = conf.get("detected_folder")

            if not detected:
                print(
                    "[COSIDAG] No detected folder available "
                    "(monitoring disabled and no dag_run.conf['detected_folder'])"
                )
                return None

            # -------------------------------------------------
            # 2) Build deep-link URL (if possible)
            # -------------------------------------------------
            homepage = os.environ.get(
                self.params.get("home_env_var", "COSIFLOW_HOME_URL")
            ) or os.environ.get("COSIFLOW_HOME_URL")

            url = None
            if homepage:
                # ⚠️ Adapt this base path to your filesystem layout
                DATA_ROOT = "/home/gamma/workspace/data"
                rel = detected.replace(DATA_ROOT, "").lstrip("/")
                url = f"{homepage.rstrip('/')}/folder/{rel}"

            # -------------------------------------------------
            # 3) Push structured result to XCom (canonical output)
            # -------------------------------------------------
            result = {
                "folder": detected,
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
            print(f"Folder: {detected}")
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
