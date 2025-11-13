"""
COSIDAG — a convenience DAG subclass that wires a standard layout:

  1) check_new_file  ->  2) automatic_retrig  ->  3) resolve_inputs  ->  4) [custom tasks]  ->  5) show_results

- check_new_file: a PythonSensor scanning one or more monitoring folders for a new
  (previously unprocessed) subfolder up to a given depth level. When a new folder
  is found, its absolute path is pushed to XCom with key 'detected_folder'.
- automatic_retrig: triggers the current DAG again as soon as step (2) completes.
- resolve_inputs: if file_patterns is provided, it searches for files matching the given patterns
  under the detected folder and pushes the selected file paths to XCom with the corresponding key.
- [custom]: user-defined tasks; they can pull the detected folder or resolved files from XCom using
  "{{ ti.xcom_pull(task_ids='check_new_file', key='detected_folder') }}" or 
  "{{ ti.xcom_pull(task_ids='resolve_inputs', key='xcom_key') }}" for each key in file_patterns.
- show_results: writes to logs the homepage URL read from env (e.g. COSIFLOW_HOME_URL)
  and, if possible, composes a deeper link using the new path module.

Notes
------
* Requires Airflow 2.x.
* Environment: define COSIFLOW_HOME_URL (in your .env) to point to the web UI homepage.
* State: a Variable named f"COSIDAG_PROCESSED::{dag_id}" is used to track processed
  folder paths across runs, to avoid reprocessing the same folder.
  * To clear the processed folder paths, delete the Variable, with the command:
    `$ airflow variables delete COSIDAG_PROCESSED::{dag_id}`
* Date: if date is provided, it only accepts subfolders with the given date.
* Only basename: if only_basename is provided, it only accepts subfolders with the given basename.
* Prefer deepest: if prefer_deepest is True, it prefers the deepest subfolder.
* File patterns: if file_patterns is provided, it searches for files matching the given patterns
  using regular expressions.
* Select policy: if select_policy is "latest_mtime", it selects the file with the latest modification time.
  If select_policy is "first", it selects the first file found.
* Tags: if tags is provided, it adds the tags to the DAG.
* Default args extra: if default_args_extra is provided, it adds the default args to the DAG.
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
from pathlib import Path

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

# ----- Import onfailure callback -------------------------------------------------
import sys
import os
airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
sys.path.append(os.path.join(airflow_home, "callbacks"))
from on_failure_callback import notify_email

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
    """Read config from Airflow Variable, then ENV, else default (string)."""
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
    return str(v).strip().lower() in {"1","true","t","yes","y","on"}

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
    """Yield subfolders under *root* up to *max_depth* (depth 1 = direct children)."""
    root_depth = root.rstrip(os.sep).count(os.sep)
    for current_root, dirs, _ in os.walk(root):
        current_depth = current_root.rstrip(os.sep).count(os.sep) - root_depth
        if current_depth > max_depth:
            dirs[:] = []
            continue
        if current_depth >= 1:
            yield current_root


def _looks_like_date_folder(name: str) -> bool:
    return bool(
        re.match(r"^\d{8}(?:_|$)", name) or re.match(r"^\d{4}-\d{2}-\d{2}(?:_|$)", name)
    )


def _date_filter_ok(path: str, date_str: Optional[str]) -> bool:
    """Accept path if it matches the given date (folder name or mtime)."""
    print(f"[COSIDAG] _date_filter_ok: path={path}, date_str={date_str}")   # DEBUG
    if not date_str:
        return True
    last = os.path.basename(os.path.normpath(path))
    print(f"[COSIDAG] _date_filter_ok: last={last}")
    try:
        if re.match(r"^\d{8}$", date_str):
            target = datetime.strptime(date_str, "%Y%m%d").date()
        else:
            target = datetime.strptime(date_str, "%Y-%m-%d").date()
        print(f"[COSIDAG] _date_filter_ok: target={target}")   # DEBUG
    except Exception:
        return True
    if _looks_like_date_folder(last):
        ds = last.split("_")[0]
        try:
            d = datetime.strptime(ds, "%Y%m%d").date() if len(ds) == 8 else datetime.strptime(ds, "%Y-%m-%d").date()
            return d == target
        except Exception:
            pass
        print(f"[COSIDAG] _date_filter_ok: d={d} != target={target}")   # DEBUG
    try:
        mtime = datetime.fromtimestamp(os.stat(path).st_mtime).date()
        print(f"[COSIDAG] _date_filter_ok: mtime={mtime} == target={target}")   # DEBUG
        return mtime == target
    except Exception:
        return True


def _load_processed_set(dag_id: str) -> set:
    """Load processed paths set from Airflow Variable."""
    key = f"COSIDAG_PROCESSED::{dag_id}"
    raw = Variable.get(key, default_var="[]")
    try:
        return set(json.loads(raw))
    except Exception:
        return set()


def _save_processed_set(dag_id: str, processed: set) -> None:
    key = f"COSIDAG_PROCESSED::{dag_id}"
    Variable.set(key, json.dumps(sorted(processed)))


def _find_new_folder(
    monitoring_folders: Iterable[str],
    level: int,
    date: Optional[str],
    dag_id: str,
    only_basename: Optional[str] = None,
    prefer_deepest: bool = True,
) -> Optional[str]:
    """Return the first new folder across roots (filtered & depth-limited)."""
    print(f"[COSIDAG] _find_new_folder: searching for new folders (dag_id={dag_id}, level={level}, date={date}, only_basename={only_basename})")
    roots = _normalize_folders(monitoring_folders)
    if not roots:
        print(f"[COSIDAG] _find_new_folder: no valid monitoring folders found")
        return None

    print(f"[COSIDAG] _find_new_folder: monitoring {len(roots)} root folder(s): {', '.join(roots)}")
    processed = _load_processed_set(dag_id)
    print(f"[COSIDAG] _find_new_folder: loaded {len(processed)} already processed folder(s)")

    candidates = []
    for root in sorted(roots):
        subfolders = list(_iter_subfolders(root, max_depth=level))  # materialize once
        print(f"[COSIDAG] _find_new_folder: found {len(subfolders)} subfolder(s) in {root} (max_depth={level})")
        for sub in subfolders:
            if only_basename and os.path.basename(sub) != only_basename:
               continue
            if _date_filter_ok(sub, date):
                candidates.append(sub)

    if not candidates:
        print(f"[COSIDAG] _find_new_folder: no candidates found after filtering")
        return None

    print(f"[COSIDAG] _find_new_folder: {len(candidates)} candidate folder(s) after filtering")

    # Prefer deeper paths (e.g., .../products) first
    if prefer_deepest:
        candidates.sort(key=lambda p: (p.count(os.sep), p), reverse=True)
        print(f"[COSIDAG] _find_new_folder: sorted candidates by depth (deepest first)")
    else:
        candidates.sort()
        print(f"[COSIDAG] _find_new_folder: sorted candidates alphabetically")

    for path in candidates:
        if path not in processed:
            print(f"[COSIDAG] _find_new_folder: found new folder: {path}")
            return path
    
    print(f"[COSIDAG] _find_new_folder: all {len(candidates)} candidate(s) already processed")
    return None

# ---- COSIDAG --------------------------------------------------------------------

class COSIDAG(DAG):
    """DAG subclass that wires: check_new_file -> automatic_retrig -> resolve_inputs -> [custom] -> show_results"""

    def __init__(
        self,
        monitoring_folders,
        level: int = 1,
        date: Optional[str] = None,
        build_custom: Optional[Callable[[DAG], None]] = None,
        sensor_poke_seconds: int = 30,
        sensor_timeout_seconds: int = 60 * 60 * 6,
        home_env_var: str = "COSIFLOW_HOME_URL",
        idle_seconds: int = 20,
        min_files: int = 1,
        ready_marker: Optional[str] = None,
        only_basename: Optional[str] = None,
        prefer_deepest: bool = True,
        file_patterns: Optional[dict] = None,   # {"xcom_key": "glob_pattern", ...}
        select_policy: str = "first",           # "first" | "latest_mtime"
        tags: Optional[list[str]] = None,
        default_args_extra: Optional[dict] = None,
        *args,
        **kwargs,
    ) -> None:
        """
        COSIDAG — a convenience DAG subclass that wires a standard layout:

        1) check_new_file  ->  2) automatic_retrig  ->  3) resolve_inputs  ->  4) [custom tasks]  ->  5) show_results

        - check_new_file: a PythonSensor scanning one or more monitoring folders for a new
        (previously unprocessed) subfolder up to a given depth level. When a new folder
        is found, its absolute path is pushed to XCom with key 'detected_folder'.
        - automatic_retrig: triggers the current DAG again as soon as step (2) completes.
        - resolve_inputs: if file_patterns is provided, it searches for files matching the given patterns
        under the detected folder and pushes the selected file paths to XCom with the corresponding key.
        - [custom]: user-defined tasks; they can pull the detected folder or resolved files from XCom using
        "{{ ti.xcom_pull(task_ids='check_new_file', key='detected_folder') }}" or 
        "{{ ti.xcom_pull(task_ids='resolve_inputs', key='xcom_key') }}" for each key in file_patterns.
        - show_results: writes to logs the homepage URL read from env (e.g. COSIFLOW_HOME_URL)
        and, if possible, composes a deeper link using the new path module.

        
        Args:
            monitoring_folders: list of directories to monitor for new subfolders.
            level: maximum depth of subfolders to consider.
            date: only accept subfolders with the given date.
            build_custom: function to build the custom tasks.
            sensor_poke_seconds: interval in seconds to check for new subfolders.
            sensor_timeout_seconds: timeout in seconds to check for new subfolders.
            home_env_var: environment variable to get the homepage URL.
            idle_seconds: minimum time in seconds that a folder must be stable to be accepted.
            min_files: minimum number of files in a folder to be accepted.
            ready_marker: file name to check for successful completion.
            only_basename: only accept subfolders with the given basename.
            prefer_deepest: prefer the deepest subfolder.
            file_patterns: dictionary of file patterns to search for.
            select_policy: policy to select the file to use.
            tags: list of tags to add to the DAG.
            default_args_extra: dictionary of default arguments to add to the DAG.
            *args: positional arguments to pass to the DAG constructor.
            **kwargs: keyword arguments to pass to the DAG constructor.
        """

        # --- merge default_args ---
        # priority: kwargs.default_args < _BASE_DEFAULT_ARGS < default_args_extra
        base = dict(_BASE_DEFAULT_ARGS)
        if "default_args" in kwargs and kwargs["default_args"]:
            base.update(kwargs["default_args"])  # allows override from caller
        if default_args_extra:
            base.update(default_args_extra)      # extensions/override requested

        # ensure that DAG receives the final default_args
        kwargs["default_args"] = base

        # --- merge tags ---
        existing_tags = list(kwargs.get("tags", []) or [])
        merged_tags = sorted(set((tags or []) + existing_tags))
        if merged_tags:
            kwargs["tags"] = merged_tags

        super().__init__(*args, **kwargs)

        # Base params (can be overridden by dag_run.conf at runtime)
        self.params.update(
            {
                "monitoring_folders": monitoring_folders,
                "level": int(level),
                "date": date,
                "home_env_var": home_env_var,
                "idle_seconds": int(idle_seconds),
                "min_files": int(min_files),
                "ready_marker": ready_marker,
                "only_basename": only_basename,
                "prefer_deepest": bool(prefer_deepest),
            }
        )

        # 1) check_new_file — PythonSensor
        def _sensor_poke(ti, **context):
            # Merge dag_run.conf over self.params
            conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
            monitoring = conf.get("monitoring_folders", self.params["monitoring_folders"])
            level_val = int(conf.get("level", self.params["level"]))
            date_val = conf.get("date", self.params.get("date"))
            idle_s = int(conf.get("idle_seconds", self.params.get("idle_seconds", 20)))
            min_f = int(conf.get("min_files", self.params.get("min_files", 1)))
            marker = conf.get("ready_marker", self.params.get("ready_marker"))
            only_bn = conf.get("only_basename", self.params.get("only_basename"))
            prefer_deep = bool(conf.get("prefer_deepest", self.params.get("prefer_deepest", True)))

            new_path = _find_new_folder(
                monitoring_folders=monitoring,
                level=level_val,
                date=date_val,
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

            print(f"[COSIDAG] _sensor_poke: pushing detected_folder to XCom")
            ti.xcom_push(key="detected_folder", value=new_path)
            processed = _load_processed_set(self.dag_id)
            processed.add(new_path)
            _save_processed_set(self.dag_id, processed)
            return True

        check_new_file = PythonSensor(
            task_id="check_new_file",
            poke_interval=sensor_poke_seconds,
            timeout=sensor_timeout_seconds,
            mode="poke",
            python_callable=_sensor_poke,
            dag=self,
        )

        # 2) automatic_retrig — trigger this same DAG again
        def _unique_run_id() -> str:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
            return f"auto::{self.dag_id}::{ts}"

        import inspect
        trig_kwargs = {
            "task_id": "automatic_retrig",
            "trigger_dag_id": self.dag_id,
            "reset_dag_run": False,
            "wait_for_completion": False,
            "dag": self,
        }
        params = inspect.signature(TriggerDagRunOperator.__init__).parameters
        if "trigger_run_id" in params:
            trig_kwargs["trigger_run_id"] = _unique_run_id()
        elif "run_id" in params:
            trig_kwargs["run_id"] = _unique_run_id()

        automatic_retrig = TriggerDagRunOperator(**trig_kwargs)
        self.automatic_retrig = automatic_retrig

        # --- 2bis) resolve_inputs (opzionale) ----------------------------------
        # Se file_patterns è passato, crea un PythonOperator che:
        # - legge run_dir da XCom (check_new_file/detected_folder)
        # - fa una ricerca glob(**, pattern) ricorsiva
        # - seleziona un file per chiave (first | latest_mtime)
        # - pusha su XCom: {key: path}
        resolve_inputs = None
        if file_patterns:
            import glob, os
            from airflow.exceptions import AirflowFailException

            def _resolve_inputs(ti):
                run_dir = ti.xcom_pull(task_ids="check_new_file", key="detected_folder")
                if not run_dir or not os.path.isdir(run_dir):
                    raise AirflowFailException(f"[resolve_inputs] run_dir non valido: {run_dir}")

                def pick_one(paths):
                    if not paths:
                        return None
                    if select_policy == "first":
                        return sorted(paths)[0]
                    elif select_policy == "latest_mtime":
                        return max(paths, key=lambda p: os.stat(p).st_mtime)
                    else:
                        return sorted(paths)[0]

                found = {}
                for key, pattern in file_patterns.items():
                    matches = sorted(
                        glob.glob(os.path.join(run_dir, "**", pattern), recursive=True)
                    )
                    chosen = pick_one(matches)
                    if not chosen:
                        raise AirflowFailException(
                            f"[resolve_inputs] Nessun file per {key} con pattern '{pattern}' sotto {run_dir}"
                        )
                    ti.xcom_push(key=key, value=chosen)
                    found[key] = chosen
                    print(f"[resolve_inputs] {key} = {chosen}")

                # utile anche ripubblicare run_dir
                ti.xcom_push(key="run_dir", value=run_dir)
                print(f"[resolve_inputs] run_dir = {run_dir}")

            resolve_inputs = PythonOperator(
                task_id="resolve_inputs",
                python_callable=_resolve_inputs,
                dag=self,
            )

        # 3) [custom] — let users append their tasks (optional)
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

                # Se c'è resolve_inputs: automatic_retrig >> resolve_inputs >> roots
                # altrimenti: automatic_retrig >> roots
                anchor_after_retrig = resolve_inputs if resolve_inputs else automatic_retrig
                if resolve_inputs:
                    automatic_retrig >> resolve_inputs
                for t in roots:
                    anchor_after_retrig >> t

                last_custom = EmptyOperator(task_id="custom_anchor", dag=self)
                for t in leaves:
                    t >> last_custom
            else:
                last_custom = EmptyOperator(task_id="custom_placeholder", dag=self)
        else:
            last_custom = EmptyOperator(task_id="custom_placeholder", dag=self)

        # 4) show_results — log homepage and optional deep link
        def _show_results(**context):
            ti = context["ti"]
            detected = ti.xcom_pull(task_ids="check_new_file", key="detected_folder")
            homepage = os.environ.get(self.params.get("home_env_var", "COSIFLOW_HOME_URL")) \
                       or os.environ.get("COSIFLOW_HOME_URL")
            if homepage:
                # make the union between homepage and detected folder
                # homepage is the base url, e.g. http://agilehost3.iasfbo.inaf.it:8080/heasarcbrowser
                # detected folder is the path to the folder, e.g. /home/gamma/workspace/data/tsmap/20251111
                # the union is the base url + the detected folder, e.g. http://agilehost3.iasfbo.inaf.it:8080/heasarcbrowser/folder/tsmap/2025_11/251111001/products
                deep = f"{homepage.rstrip('/')}/folder/{detected.replace("/home/gamma/workspace/data", "").lstrip('/')}" if homepage else detected
                print(f"[COSIDAG] Result page: {deep}")
                return deep
            else:
                print("[COSIDAG] Homepage URL not set. Define COSIFLOW_HOME_URL in your .env.")
            if detected and PathInfo is not None:
                try:
                    info = PathInfo.from_path(detected)  # type: ignore[attr-defined]
                    if callable(build_url_fragment):
                        frag = build_url_fragment(info)  # type: ignore
                        deep = f"{homepage.rstrip('/')}/{frag.lstrip('/')}" if homepage else frag
                    else:
                        parts = [getattr(info, k, None) for k in ("domain", "year", "month", "identifier")]
                        parts = [str(p) for p in parts if p]
                        deep = f"{homepage.rstrip('/')}/" + "/".join(parts) if (homepage and parts) else None
                    if deep:
                        print(f"[COSIDAG] Result page: {deep}")
                except Exception as e:
                    print(f"[COSIDAG] Deep-linking failed: {e}")
            elif detected:
                print(f"[COSIDAG] Detected folder: {detected}")

        show_results = PythonOperator(
            task_id="show_results",
            python_callable=_show_results,
            trigger_rule=TriggerRule.ALL_DONE,
            dag=self,
        )

        # Wire: 1 -> 2 -> [3] -> 4
        check_new_file >> automatic_retrig >> last_custom >> show_results

        # Expose handles
        self.check_new_file = check_new_file
        self.show_results = show_results

    def find_file_by_pattern(self, pattern: str, detected_folder: str) -> Optional[str]:
        """Find the first file matching the given pattern under the detected folder."""
        print(f"[COSIDAG] find_file_by_pattern: pattern={pattern}, detected_folder={detected_folder}")
        # search for the file by pattern
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
#     # Example custom task consuming the detected folder via XCom
#     def _process_folder(folder_path: str):
#         # Do your science here
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
#     build_custom=build_custom,
# ) as dag:
#     pass
