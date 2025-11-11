"""
COSIDAG — a convenience DAG subclass that wires a standard layout:

  1) check_new_file  ->  2) automatic_retrig  ->  3) [custom tasks]  ->  4) show_results

- check_new_file: a PythonSensor scanning one or more monitoring folders for a new
  (previously unprocessed) subfolder up to a given depth level. When a new folder
  is found, its absolute path is pushed to XCom with key 'detected_folder'.
- automatic_retrig: triggers the current DAG again as soon as step (2) completes.
- [custom]: user-defined tasks; they can pull the detected folder from XCom using
  "{{ ti.xcom_pull(task_ids='check_new_file', key='detected_folder') }}".
- show_results: writes to logs the homepage URL read from env (e.g. COSIFLOW_HOME_URL)
  and, if possible, composes a deeper link using the new path module.

Notes
-----
* Requires Airflow 2.x.
* Environment: define COSIFLOW_HOME_URL (in your .env) to point to the web UI homepage.
* State: a Variable named f"COSIDAG_PROCESSED::{dag_id}" is used to track processed
  folder paths across runs, to avoid reprocessing the same folder.
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

# ---- Helpers (MUST stay at module top-level) ------------------------------------

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
            # if only_basename and os.path.basename(sub) != only_basename:
            #    continue
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
    """DAG subclass that wires: check_new_file -> automatic_retrig -> [custom] -> show_results"""

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
        *args,
        **kwargs,
    ) -> None:
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

        # 3) [custom] — user tasks
        before_tasks = set(self.task_dict.keys())
        if callable(build_custom):
            build_custom(self)
            after_tasks = set(self.task_dict.keys())
            new_tasks_ids = sorted(after_tasks - before_tasks)
            if new_tasks_ids:
                for tid in new_tasks_ids:
                    automatic_retrig >> self.task_dict[tid]
                last_custom = EmptyOperator(task_id="custom_anchor", dag=self)
                for tid in new_tasks_ids:
                    self.task_dict[tid] >> last_custom
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
