#!/usr/bin/env python3
"""
Configurable performance runner for Airflow DAGs/COSIDAGs.

The script is intended to be run from the host:

    python3 cosiflow/test/performance_test.py --config cosiflow/test/performance_test.yaml

It uses Docker Compose to execute Airflow commands inside the configured Airflow
service, while resolving all data paths relative to the local cosiflow folder.
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import os
import shutil
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    yaml = None


TERMINAL_STATES = {"success", "failed"}
ACTIVE_STATES = {"queued", "running", "scheduled", "up_for_retry", "up_for_reschedule"}


def log(message: str) -> None:
    print(f"[perf] {message}", flush=True)


@dataclass
class DagConfig:
    dag_id: str
    reset_cosidag_variable: bool = False
    trigger: bool = True
    conf: dict[str, Any] = field(default_factory=dict)
    run_id: str | None = None


@dataclass
class RuntimeDag:
    dag_id: str
    run_id: str
    trigger_time: str
    conf: dict[str, Any]
    task_ids: list[str]
    state: str = "unknown"
    end_time: str = ""
    task_states: dict[str, str] = field(default_factory=dict)


class CommandError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_scalar(value: str) -> Any:
    value = value.strip()
    if not value:
        return ""
    if value[0:1] in {"'", '"'} and value[-1:] == value[0]:
        return value[1:-1]
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none", "~"}:
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def yaml_fallback_load(text: str) -> Any:
    """Parse the small YAML subset used by this test config when PyYAML is absent."""
    parsed_lines: list[tuple[int, str]] = []
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        parsed_lines.append((indent, raw_line.strip()))

    def parse_block(index: int, indent: int) -> tuple[Any, int]:
        if index >= len(parsed_lines):
            return {}, index
        current_indent, text_at_index = parsed_lines[index]
        if current_indent < indent:
            return {}, index
        if text_at_index.startswith("- "):
            return parse_list(index, current_indent)
        return parse_dict(index, current_indent)

    def parse_dict(index: int, indent: int) -> tuple[dict[str, Any], int]:
        result: dict[str, Any] = {}
        while index < len(parsed_lines):
            current_indent, text_at_index = parsed_lines[index]
            if current_indent < indent:
                break
            if current_indent > indent:
                raise ValueError(f"Unexpected indentation near: {text_at_index}")
            if text_at_index.startswith("- "):
                break
            if ":" not in text_at_index:
                raise ValueError(f"Expected `key: value` near: {text_at_index}")
            key, raw_value = text_at_index.split(":", 1)
            key = key.strip()
            raw_value = raw_value.strip()
            index += 1
            if raw_value:
                result[key] = parse_scalar(raw_value)
            elif index < len(parsed_lines) and parsed_lines[index][0] > current_indent:
                result[key], index = parse_block(index, parsed_lines[index][0])
            else:
                result[key] = {}
        return result, index

    def parse_list(index: int, indent: int) -> tuple[list[Any], int]:
        result: list[Any] = []
        while index < len(parsed_lines):
            current_indent, text_at_index = parsed_lines[index]
            if current_indent < indent:
                break
            if current_indent > indent:
                raise ValueError(f"Unexpected indentation near: {text_at_index}")
            if not text_at_index.startswith("- "):
                break
            item_text = text_at_index[2:].strip()
            index += 1
            if not item_text:
                if index < len(parsed_lines) and parsed_lines[index][0] > current_indent:
                    item, index = parse_block(index, parsed_lines[index][0])
                else:
                    item = None
            elif ":" in item_text:
                key, raw_value = item_text.split(":", 1)
                item = {key.strip(): parse_scalar(raw_value.strip()) if raw_value.strip() else {}}
                if index < len(parsed_lines) and parsed_lines[index][0] > current_indent:
                    extra, index = parse_block(index, parsed_lines[index][0])
                    if isinstance(extra, dict):
                        item.update(extra)
                    else:
                        raise ValueError(f"Expected mapping continuation near: {item_text}")
            else:
                item = parse_scalar(item_text)
            result.append(item)
        return result, index

    data, final_index = parse_block(0, parsed_lines[0][0] if parsed_lines else 0)
    if final_index != len(parsed_lines):
        raise ValueError("Unable to parse the complete YAML file")
    return data


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        text = handle.read()
    if yaml is not None:
        data = yaml.safe_load(text) or {}
    else:
        data = yaml_fallback_load(text) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a mapping: {path}")
    return data


def script_paths(config_path: Path) -> tuple[Path, Path, Path]:
    test_dir = Path(__file__).resolve().parent
    cosiflow_root = test_dir.parent
    workspace_root = cosiflow_root.parent
    return test_dir, cosiflow_root, workspace_root


def resolve_from(base: Path, candidate: str | os.PathLike[str]) -> Path:
    path = Path(candidate).expanduser()
    if path.is_absolute():
        return path
    return (base / path).resolve()


def run_command(
    args: list[str],
    cwd: Path | None = None,
    check: bool = True,
    show_command: bool = True,
    show_stderr: bool = True,
) -> str:
    if show_command:
        print("+ " + " ".join(shlex.quote(str(part)) for part in args))
    try:
        proc = subprocess.run(
            args,
            cwd=str(cwd) if cwd else None,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except FileNotFoundError as exc:
        if check:
            raise CommandError(f"Command executable not found: {args[0]}") from exc
        return ""
    if check and proc.returncode != 0:
        raise CommandError(
            f"Command failed with exit code {proc.returncode}: {' '.join(args)}\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    if show_stderr and proc.stderr.strip():
        print(proc.stderr.strip(), file=sys.stderr)
    return proc.stdout.strip()


class AirflowClient:
    def __init__(self, config: dict[str, Any], test_dir: Path, inside_container: bool = False):
        airflow_cfg = config.get("airflow", {})
        logging_cfg = config.get("logging", {})
        self.test_dir = test_dir
        self.inside_container = inside_container
        self.service = airflow_cfg.get("service", "airflow")
        self.compose_file = resolve_from(test_dir, airflow_cfg.get("compose_file", "../env/docker-compose.yaml"))
        self.project_directory = resolve_from(
            test_dir,
            airflow_cfg.get("project_directory", self.compose_file.parent),
        )
        self.verbose_commands = bool(logging_cfg.get("verbose_commands", False))
        self.show_subprocess_stderr = bool(logging_cfg.get("show_subprocess_stderr", False))

    def compose(self, *args: str, check: bool = True) -> str:
        cmd = ["docker", "compose", "-f", str(self.compose_file), *args]
        return run_command(
            cmd,
            cwd=self.project_directory,
            check=check,
            show_command=self.verbose_commands,
            show_stderr=self.show_subprocess_stderr,
        )

    def airflow(self, *args: str, check: bool = True) -> str:
        if self.inside_container:
            return run_command(
                ["airflow", *args],
                check=check,
                show_command=self.verbose_commands,
                show_stderr=self.show_subprocess_stderr,
            )
        return self.compose("exec", "-T", self.service, "airflow", *args, check=check)

    def container_test_path(self, path: Path) -> str:
        if self.inside_container:
            return str(path)
        try:
            relative = path.resolve().relative_to(self.test_dir.resolve())
        except ValueError:
            return str(path)
        return "/shared_dir/test/" + relative.as_posix()

    def python(self, code: str) -> str:
        if self.inside_container:
            return run_command(
                ["python", "-c", code],
                show_command=self.verbose_commands,
                show_stderr=self.show_subprocess_stderr,
            )
        return self.compose("exec", "-T", self.service, "python", "-c", code)

    def python_json(self, code: str) -> Any:
        output = self.python(code)
        lines = [line for line in output.splitlines() if line.strip()]
        if not lines:
            return None
        return json.loads(lines[-1])

    def reset_cosidag_variable(self, dag_id: str) -> None:
        self.airflow("variables", "set", f"COSIDAG_PROCESSED::{dag_id}", "[]")

    def unpause(self, dag_id: str) -> None:
        self.airflow("dags", "unpause", dag_id)

    def pause(self, dag_id: str) -> None:
        self.airflow("dags", "pause", dag_id)

    def trigger(self, dag_id: str, run_id: str, conf: dict[str, Any]) -> None:
        args = ["dags", "trigger", dag_id, "--run-id", run_id]
        if conf:
            args.extend(["--conf", json.dumps(conf, sort_keys=True)])
        self.airflow(*args)

    def task_ids(self, dag_id: str) -> list[str]:
        code = f"""
import json
from airflow.models.dagbag import DagBag
dag = DagBag().get_dag({dag_id!r})
if dag is None:
    raise SystemExit("DAG not found: {dag_id}")
print(json.dumps([task.task_id for task in dag.tasks]))
"""
        return list(self.python_json(code) or [])

    def dag_structure(self, dag_id: str) -> dict[str, Any]:
        code = f"""
import json
from airflow.models.dagbag import DagBag
dag = DagBag().get_dag({dag_id!r})
if dag is None:
    raise SystemExit("DAG not found: {dag_id}")
tasks = []
edges = []
for task in dag.tasks:
    downstream = sorted(task.downstream_task_ids)
    tasks.append({{
        "task_id": task.task_id,
        "downstream_task_ids": downstream,
    }})
    for target in downstream:
        edges.append({{"source": task.task_id, "target": target}})
print(json.dumps({{
    "dag_id": dag.dag_id,
    "tasks": tasks,
    "edges": edges,
}}, sort_keys=True))
"""
        return dict(self.python_json(code) or {})

    def run_status(self, dag_id: str, run_id: str) -> dict[str, Any]:
        code = f"""
import json
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session
with create_session() as session:
    dr = session.query(DagRun).filter(DagRun.dag_id == {dag_id!r}, DagRun.run_id == {run_id!r}).one_or_none()
    if dr is None:
        print(json.dumps({{"dag_id": {dag_id!r}, "run_id": {run_id!r}, "state": "missing", "tasks": {{}}}}))
    else:
        tasks = {{}}
        for ti in dr.get_task_instances(session=session):
            tasks[ti.task_id] = ti.state or "none"
        print(json.dumps({{
            "dag_id": dr.dag_id,
            "run_id": dr.run_id,
            "state": str(dr.state),
            "start_date": dr.start_date.isoformat() if dr.start_date else "",
            "end_date": dr.end_date.isoformat() if dr.end_date else "",
            "tasks": tasks,
        }}, sort_keys=True))
"""
        return dict(self.python_json(code) or {})

    def task_timings(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        code = f"""
import json
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session
with create_session() as session:
    dr = session.query(DagRun).filter(DagRun.dag_id == {dag_id!r}, DagRun.run_id == {run_id!r}).one_or_none()
    rows = []
    if dr is not None:
        for ti in dr.get_task_instances(session=session):
            rows.append({{
                "dag_id": ti.dag_id,
                "run_id": ti.run_id,
                "task_id": ti.task_id,
                "state": ti.state or "none",
                "start_date": ti.start_date.isoformat() if ti.start_date else "",
                "end_date": ti.end_date.isoformat() if ti.end_date else "",
                "duration": float(ti.duration or 0.0),
            }})
    print(json.dumps(rows, sort_keys=True))
"""
        return list(self.python_json(code) or [])

    def export_dag_graph(self, dag_id: str, output_path: Path) -> bool:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        container_output_path = self.container_test_path(output_path)
        self.airflow("dags", "show", dag_id, "--save", container_output_path, check=False)
        return output_path.exists() and output_path.stat().st_size > 0

    def stop_active_run(self, dag_id: str, run_id: str) -> None:
        code = f"""
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session
try:
    from airflow.utils.state import DagRunState, TaskInstanceState
    failed_dag = DagRunState.FAILED
    failed_task = TaskInstanceState.FAILED
except Exception:
    failed_dag = "failed"
    failed_task = "failed"
active = {sorted(ACTIVE_STATES)!r}
with create_session() as session:
    dr = session.query(DagRun).filter(DagRun.dag_id == {dag_id!r}, DagRun.run_id == {run_id!r}).one_or_none()
    if dr and str(dr.state) not in ("success", "failed"):
        for ti in dr.get_task_instances(session=session):
            if ti.state is None or str(ti.state) in active:
                ti.set_state(failed_task, session=session)
        dr.set_state(failed_dag)
        session.merge(dr)
"""
        self.python(code)

    def stop_other_active_runs(self, dag_ids: list[str], keep_run_ids: set[str]) -> list[dict[str, str]]:
        code = f"""
import json
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session
try:
    from airflow.utils.state import DagRunState, TaskInstanceState
    failed_dag = DagRunState.FAILED
    failed_task = TaskInstanceState.FAILED
except Exception:
    failed_dag = "failed"
    failed_task = "failed"
dag_ids = {dag_ids!r}
keep_run_ids = {sorted(keep_run_ids)!r}
active = {sorted(ACTIVE_STATES)!r}
stopped = []
with create_session() as session:
    runs = (
        session.query(DagRun)
        .filter(DagRun.dag_id.in_(dag_ids))
        .all()
    )
    for dr in runs:
        if dr.run_id in keep_run_ids:
            continue
        if str(dr.state) in ("success", "failed"):
            continue
        for ti in dr.get_task_instances(session=session):
            if ti.state is None or str(ti.state) in active:
                ti.set_state(failed_task, session=session)
        dr.set_state(failed_dag)
        session.merge(dr)
        stopped.append({{"dag_id": dr.dag_id, "run_id": dr.run_id, "state": str(dr.state)}})
print(json.dumps(stopped, sort_keys=True))
"""
        return list(self.python_json(code) or [])


def parse_dags(config: dict[str, Any]) -> list[DagConfig]:
    dags = []
    for item in config.get("dags", []):
        if not isinstance(item, dict) or not item.get("dag_id"):
            raise ValueError("Each dag entry must be a mapping with dag_id")
        dags.append(
            DagConfig(
                dag_id=str(item["dag_id"]),
                reset_cosidag_variable=bool(item.get("reset_cosidag_variable", False)),
                trigger=bool(item.get("trigger", True)),
                conf=dict(item.get("conf") or {}),
                run_id=item.get("run_id"),
            )
        )
    if not dags:
        raise ValueError("No DAGs configured under `dags`")
    return dags


def container_dataset_path(dataset: str, config: dict[str, Any]) -> str:
    if dataset.startswith("/"):
        return dataset
    data_root = str((config.get("airflow") or {}).get("container_data_root", "/home/gamma/workspace/data")).rstrip("/")
    normalized = dataset.strip("/")
    if normalized == "data":
        return data_root
    if normalized.startswith("data/"):
        return f"{data_root}/{normalized[len('data/'):]}"
    return f"{data_root}/{normalized}"


def cosidag_conf_for_dataset(dag_cfg: DagConfig, config: dict[str, Any]) -> dict[str, Any]:
    conf = dict(dag_cfg.conf)
    dataset = conf.get("performance_test_dataset")
    if not dataset:
        return conf

    detected_folder = container_dataset_path(str(dataset), config)
    conf.setdefault("detected_folder", detected_folder)
    conf.setdefault("monitoring_folders", [str(Path(detected_folder).parent)])
    conf.setdefault("level", 1)
    conf.setdefault("only_basename", Path(detected_folder).name)
    conf.setdefault("auto_retrig", False)
    conf.setdefault("max_retrig_runs", 0)
    return conf


def cleanup_data(config: dict[str, Any], cosiflow_root: Path) -> None:
    cleanup_cfg = config.get("cleanup", {})
    if not cleanup_cfg.get("enabled", False):
        log("cleanup disabled")
        return

    dry_run = bool(cleanup_cfg.get("dry_run", False))
    verbose = bool(cleanup_cfg.get("verbose", (config.get("logging", {}) or {}).get("verbose_cleanup", False)))
    for root_cfg in cleanup_cfg.get("roots", []):
        root = resolve_from(cosiflow_root, root_cfg["path"])
        keep_names = {str(name) for name in root_cfg.get("keep_files", [])}
        if not root.exists():
            log(f"cleanup skipped, path does not exist: {root}")
            continue
        if not root.is_dir():
            raise ValueError(f"Cleanup path is not a directory: {root}")
        if cosiflow_root not in root.parents and root != cosiflow_root:
            raise ValueError(f"Refusing to clean outside cosiflow root: {root}")

        protected_dirs: set[Path] = set()
        removed_files = 0
        removed_dirs = 0
        kept_files = 0

        for path in sorted(root.rglob("*"), key=lambda p: len(p.parts), reverse=True):
            if path.is_file():
                if path.name in keep_names:
                    kept_files += 1
                    protected_dirs.update(path.parents)
                    continue
                if verbose:
                    log(f"{'[dry-run] ' if dry_run else ''}delete file {path}")
                if not dry_run:
                    path.unlink()
                removed_files += 1

        for path in sorted((p for p in root.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
            if path in protected_dirs:
                continue
            try:
                if any(path.iterdir()):
                    continue
            except FileNotFoundError:
                continue
            if verbose:
                log(f"{'[dry-run] ' if dry_run else ''}delete empty dir {path}")
            if not dry_run:
                path.rmdir()
            removed_dirs += 1

        log(
            f"cleanup {root}: removed_files={removed_files}, "
            f"removed_dirs={removed_dirs}, kept_files={kept_files}, dry_run={dry_run}"
        )


def docker_stats(config: dict[str, Any]) -> dict[str, str]:
    resources_cfg = config.get("resources", {})
    if not resources_cfg.get("docker_stats", True):
        return {}
    containers = [str(c) for c in resources_cfg.get("containers", [])]
    if not containers:
        return {}
    output = run_command(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *containers],
        check=False,
        show_command=False,
        show_stderr=False,
    )
    stats: dict[str, str] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = item.get("Name") or item.get("Container") or "container"
        prefix = str(name).replace("-", "_").replace(".", "_")
        stats[f"{prefix}_cpu_percent"] = item.get("CPUPerc", "")
        stats[f"{prefix}_memory_usage"] = item.get("MemUsage", "")
        stats[f"{prefix}_memory_percent"] = item.get("MemPerc", "")
        stats[f"{prefix}_network_io"] = item.get("NetIO", "")
        stats[f"{prefix}_block_io"] = item.get("BlockIO", "")
        stats[f"{prefix}_pids"] = item.get("PIDs", "")
    return stats


def read_first_existing(paths: list[str]) -> str:
    for path in paths:
        try:
            value = Path(path).read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if value:
            return value
    return ""


def current_container_stats() -> dict[str, str]:
    stats: dict[str, str] = {
        "current_container_cpu_count": str(os.cpu_count() or ""),
    }

    cpu_stat = read_first_existing(["/sys/fs/cgroup/cpu.stat"])
    for line in cpu_stat.splitlines():
        key, _, value = line.partition(" ")
        if key in {"usage_usec", "user_usec", "system_usec"}:
            stats[f"current_container_cpu_{key}"] = value

    memory_current = read_first_existing([
        "/sys/fs/cgroup/memory.current",
        "/sys/fs/cgroup/memory/memory.usage_in_bytes",
    ])
    memory_limit = read_first_existing([
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ])
    stats["current_container_memory_current_bytes"] = memory_current
    stats["current_container_memory_limit_bytes"] = memory_limit

    try:
        load1, load5, load15 = Path("/proc/loadavg").read_text(encoding="utf-8").split()[:3]
        stats["current_container_load_1m"] = load1
        stats["current_container_load_5m"] = load5
        stats["current_container_load_15m"] = load15
    except OSError:
        pass

    for label, path in {
        "workspace": "/shared_dir",
        "airflow_home": os.environ.get("AIRFLOW_HOME", "/home/gamma/airflow"),
    }.items():
        try:
            usage = shutil.disk_usage(path)
        except OSError:
            continue
        stats[f"current_container_disk_{label}_total_bytes"] = str(usage.total)
        stats[f"current_container_disk_{label}_used_bytes"] = str(usage.used)
        stats[f"current_container_disk_{label}_free_bytes"] = str(usage.free)

    return stats


def gpu_stats(client: AirflowClient, config: dict[str, Any]) -> dict[str, str]:
    gpu_cfg = (config.get("resources", {}).get("gpu") or {})
    if not gpu_cfg.get("enabled", False):
        return {}
    container = str(gpu_cfg.get("container") or client.service)
    query = (
        "index,name,utilization.gpu,memory.used,memory.total"
    )
    cmd = [
        "nvidia-smi",
        f"--query-gpu={query}",
        "--format=csv,noheader,nounits",
    ]
    if not client.inside_container:
        cmd = ["docker", "exec", container, *cmd]
    output = run_command(
        cmd,
        check=False,
        show_command=False,
        show_stderr=False,
    )
    stats: dict[str, str] = {}
    for idx, line in enumerate(output.splitlines()):
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 5:
            continue
        gpu_index, gpu_name, gpu_util, mem_used, mem_total = parts
        prefix = f"{container.replace('-', '_')}_gpu_{gpu_index or idx}"
        stats[f"{prefix}_name"] = gpu_name
        stats[f"{prefix}_util_percent"] = gpu_util
        stats[f"{prefix}_memory_used_mb"] = mem_used
        stats[f"{prefix}_memory_total_mb"] = mem_total
    return stats


def resource_snapshot(client: AirflowClient, config: dict[str, Any]) -> dict[str, str]:
    stats = current_container_stats() if client.inside_container else docker_stats(config)
    stats.update(gpu_stats(client, config))
    return stats


def build_headers(dags: list[RuntimeDag], resource_columns: list[str]) -> tuple[list[str], list[str]]:
    macro = ["test", "test"]
    header = ["sample_time_utc", "elapsed_seconds"]
    for dag in dags:
        dag_columns = [
            f"{dag.dag_id}_run",
            f"{dag.dag_id}_triggered_at",
            f"{dag.dag_id}_finished_at",
            f"{dag.dag_id}_state",
            *[f"{task_id}_status" for task_id in dag.task_ids],
        ]
        macro.extend([dag.dag_id] * len(dag_columns))
        header.extend(dag_columns)
    macro.extend(["resources"] * len(resource_columns))
    header.extend(resource_columns)
    return macro, header


def row_values(
    dags: list[RuntimeDag],
    resource_columns: list[str],
    resources: dict[str, str],
    start_time: float,
) -> list[str]:
    row = [utc_now(), f"{time.time() - start_time:.1f}"]
    for dag in dags:
        row.extend([dag.run_id, dag.trigger_time, dag.end_time, dag.state])
        row.extend([dag.task_states.get(task_id, "") for task_id in dag.task_ids])
    row.extend([resources.get(col, "") for col in resource_columns])
    return row


def open_csv(path: Path, append: bool, macro: list[str], header: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    handle = path.open("a" if append else "w", newline="", encoding="utf-8")
    writer = csv.writer(handle)
    if not append or not exists:
        writer.writerow(macro)
        writer.writerow(header)
    return handle, writer


def terminal(dags: list[RuntimeDag]) -> bool:
    return all(dag.state in TERMINAL_STATES for dag in dags)


def state_counts(task_states: dict[str, str]) -> str:
    counts: dict[str, int] = {}
    for state in task_states.values():
        counts[state or "none"] = counts.get(state or "none", 0) + 1
    if not counts:
        return "tasks=0"
    return ",".join(f"{state}={counts[state]}" for state in sorted(counts))


def status_summary(dags: list[RuntimeDag]) -> str:
    return " | ".join(
        f"{dag.dag_id}:{dag.state}({state_counts(dag.task_states)})"
        for dag in dags
    )


def parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def seconds_between(start: datetime, end: datetime) -> float:
    return max(0.0, (end - start).total_seconds())


def svg_escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


def write_svg(path: Path, width: int, height: int, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<style>
text {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #1f2937; }}
.title {{ font-size: 22px; font-weight: 700; }}
.subtitle {{ font-size: 13px; fill: #6b7280; }}
.label {{ font-size: 12px; }}
.small {{ font-size: 10px; fill: #6b7280; }}
.grid {{ stroke: #e5e7eb; stroke-width: 1; }}
.axis {{ stroke: #9ca3af; stroke-width: 1; }}
</style>
<rect width="100%" height="100%" fill="#ffffff"/>
{body}
</svg>
"""
    path.write_text(svg, encoding="utf-8")


def state_color(state: str) -> str:
    return {
        "success": "#0a8f08",
        "running": "#2563eb",
        "failed": "#d92d20",
        "upstream_failed": "#f97316",
        "queued": "#9ca3af",
        "scheduled": "#a78bfa",
        "none": "#cbd5e1",
    }.get(str(state or "none"), "#64748b")


def generate_elapsed_chart(dags: list[RuntimeDag], output_path: Path) -> None:
    starts = [parse_iso_datetime(dag.trigger_time) for dag in dags]
    ends = [parse_iso_datetime(dag.end_time) for dag in dags]
    starts = [value for value in starts if value is not None]
    ends = [value for value in ends if value is not None]
    if not starts or not ends:
        return

    full_start = min(starts)
    full_end = max(ends)
    bars = []
    for dag in dags:
        start = parse_iso_datetime(dag.trigger_time)
        end = parse_iso_datetime(dag.end_time)
        if start and end:
            bars.append((dag.dag_id, seconds_between(full_start, start), seconds_between(start, end), state_color(dag.state)))
    bars.append(("Full benchmark", 0.0, seconds_between(full_start, full_end), "#111827"))

    max_end = max(offset + duration for _, offset, duration, _ in bars) or 1.0
    width = 1100
    height = 190 + len(bars) * 46
    left = 210
    top = 102
    chart_width = width - left - 80
    row_h = 42
    body = [
        '<text x="30" y="38" class="title">Elapsed time by COSIDAG</text>',
        '<text x="30" y="60" class="subtitle">Horizontal bars are aligned to the first trigger time.</text>',
    ]

    for tick in range(0, int(max_end) + 1, max(10, int(max_end // 6) or 10)):
        x = left + (tick / max_end) * chart_width
        body.append(f'<line x1="{x:.1f}" y1="{top-20}" x2="{x:.1f}" y2="{top + row_h * len(bars)}" class="grid"/>')
        body.append(f'<text x="{x:.1f}" y="{top-28}" class="small" text-anchor="middle">{tick}s</text>')

    for i, (name, offset, duration, color) in enumerate(bars):
        y = top + i * row_h
        x = left + (offset / max_end) * chart_width
        w = max(2.0, (duration / max_end) * chart_width)
        body.append(f'<text x="30" y="{y+18}" class="label">{svg_escape(name)}</text>')
        body.append(f'<rect x="{x:.1f}" y="{y}" width="{w:.1f}" height="22" rx="4" fill="{color}"/>')
        body.append(f'<text x="{x + w + 8:.1f}" y="{y+16}" class="label">{duration:.1f}s</text>')

    write_svg(output_path, width, height, "\n".join(body))


def generate_gantt_chart(dags: list[RuntimeDag], timings_by_dag: dict[str, list[dict[str, Any]]], output_path: Path) -> None:
    rows: list[tuple[str, str, str, datetime, datetime]] = []
    all_starts: list[datetime] = []
    all_ends: list[datetime] = []

    for dag in dags:
        timing_map = {item.get("task_id"): item for item in timings_by_dag.get(dag.dag_id, [])}
        ordered_ids = dag.task_ids or list(timing_map)
        for task_id in ordered_ids:
            item = timing_map.get(task_id)
            if not item:
                continue
            start = parse_iso_datetime(str(item.get("start_date", "")))
            end = parse_iso_datetime(str(item.get("end_date", "")))
            if not start:
                continue
            if not end:
                duration = float(item.get("duration") or 0.0)
                end = start
                if duration > 0:
                    end = start + timedelta(seconds=duration)
            all_starts.append(start)
            all_ends.append(end)
            rows.append((dag.dag_id, task_id, str(item.get("state") or "none"), start, end))

    if not rows or not all_starts or not all_ends:
        return

    min_start = min(all_starts)
    max_end = max(all_ends)
    total_s = max(1.0, seconds_between(min_start, max_end))
    width = 1700
    left = 360
    right = 50
    top = 95
    row_h = 22
    height = top + len(rows) * row_h + 70
    chart_width = width - left - right
    body = [
        '<text x="30" y="38" class="title">Task Gantt chart</text>',
        '<text x="30" y="60" class="subtitle">Bars show Airflow task start and end times for each benchmark run.</text>',
    ]

    tick_step = max(10, int(total_s // 8) or 10)
    for tick in range(0, int(total_s) + tick_step, tick_step):
        x = left + (tick / total_s) * chart_width
        body.append(f'<line x1="{x:.1f}" y1="{top-28}" x2="{x:.1f}" y2="{height-50}" class="grid"/>')
        body.append(f'<text x="{x:.1f}" y="{top-36}" class="small" text-anchor="middle">+{tick}s</text>')

    current_dag = None
    for i, (dag_id, task_id, state, start, end) in enumerate(rows):
        y = top + i * row_h
        if dag_id != current_dag:
            current_dag = dag_id
            body.append(f'<text x="30" y="{y+13}" class="label" font-weight="700">{svg_escape(dag_id)}</text>')
        start_s = seconds_between(min_start, start)
        duration_s = max(0.2, seconds_between(start, end))
        x = left + (start_s / total_s) * chart_width
        w = max(3.0, (duration_s / total_s) * chart_width)
        body.append(f'<text x="130" y="{y+13}" class="label">{svg_escape(task_id)}</text>')
        body.append(f'<rect x="{x:.1f}" y="{y+4}" width="{w:.1f}" height="11" rx="2" fill="#a3a3a3"/>')
        body.append(f'<rect x="{x+4:.1f}" y="{y+4}" width="{max(2, w-4):.1f}" height="11" rx="2" fill="{state_color(state)}"/>')
        body.append(f'<title>{svg_escape(dag_id)} / {svg_escape(task_id)}: {svg_escape(state)} ({duration_s:.1f}s)</title>')

    legend_y = height - 28
    for i, (state, color) in enumerate([("success", "#0a8f08"), ("running", "#2563eb"), ("failed", "#d92d20"), ("queued/none", "#9ca3af")]):
        x = 30 + i * 130
        body.append(f'<rect x="{x}" y="{legend_y-11}" width="18" height="10" fill="{color}"/>')
        body.append(f'<text x="{x+24}" y="{legend_y-2}" class="small">{state}</text>')

    write_svg(output_path, width, height, "\n".join(body))


def load_csv_rows(csv_path: Path) -> tuple[list[str], list[str], list[list[str]]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))
    if len(rows) < 3:
        return [], [], []
    return rows[0], rows[1], rows[2:]


def float_cell(row: list[str], header_index: dict[str, int], column: str) -> float | None:
    try:
        value = row[header_index[column]]
        if value in {"", "max"}:
            return None
        return float(value)
    except Exception:
        return None


def line_path(points: list[tuple[float, float]]) -> str:
    if not points:
        return ""
    first, *rest = points
    parts = [f"M {first[0]:.1f} {first[1]:.1f}"]
    parts.extend(f"L {x:.1f} {y:.1f}" for x, y in rest)
    return " ".join(parts)


def generate_resource_chart(csv_path: Path, output_path: Path) -> None:
    _macro, header, data = load_csv_rows(csv_path)
    if not data:
        return
    idx = {name: i for i, name in enumerate(header)}
    elapsed = [float_cell(row, idx, "elapsed_seconds") or 0.0 for row in data]
    max_elapsed = max(elapsed) or 1.0

    series_defs = [
        ("Memory", "current_container_memory_current_bytes", "GB", 1024 ** 3, "#2563eb"),
        ("Load 1m", "current_container_load_1m", "", 1, "#d97706"),
    ]
    cpu_points: list[tuple[float, float]] = []
    prev_elapsed = elapsed[0]
    prev_cpu = float_cell(data[0], idx, "current_container_cpu_usage_usec")
    for row, current_elapsed in zip(data[1:], elapsed[1:]):
        cpu = float_cell(row, idx, "current_container_cpu_usage_usec")
        if cpu is not None and prev_cpu is not None and current_elapsed > prev_elapsed:
            cpu_points.append((current_elapsed, max(0.0, (cpu - prev_cpu) / 1_000_000.0 / (current_elapsed - prev_elapsed))))
        prev_elapsed = current_elapsed
        prev_cpu = cpu

    width = 1200
    panel_h = 190
    left = 90
    right = 40
    top = 100
    gap = 48
    height = top + panel_h * 3 + gap * 2 + 60
    chart_w = width - left - right
    body = [
        '<text x="30" y="38" class="title">Container resource usage over time</text>',
        '<text x="30" y="60" class="subtitle">Resource samples are collected during polling and are container-level, not per-task.</text>',
    ]

    panels: list[tuple[str, list[tuple[float, float]], str, str]] = []
    for title, column, unit, divisor, color in series_defs:
        values = []
        for row, elapsed_s in zip(data, elapsed):
            raw = float_cell(row, idx, column)
            if raw is not None:
                values.append((elapsed_s, raw / divisor))
        panels.append((title, values, unit, color))
    panels.append(("Estimated CPU cores", cpu_points, "cores", "#16a34a"))

    for panel_index, (title, values, unit, color) in enumerate(panels):
        y0 = top + panel_index * (panel_h + gap)
        max_v = max([value for _, value in values] or [1.0])
        min_v = min([value for _, value in values] or [0.0])
        if max_v == min_v:
            max_v += 1.0
            min_v = 0.0
        body.append(f'<text x="30" y="{y0-18}" class="label" font-weight="700">{svg_escape(title)}</text>')
        for tick in range(0, 6):
            y = y0 + panel_h - (tick / 5) * panel_h
            v = min_v + (tick / 5) * (max_v - min_v)
            body.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" class="grid"/>')
            body.append(f'<text x="{left-10}" y="{y+4:.1f}" class="small" text-anchor="end">{v:.1f}{unit}</text>')
        for tick in range(0, int(max_elapsed) + 1, max(20, int(max_elapsed // 5) or 20)):
            x = left + (tick / max_elapsed) * chart_w
            body.append(f'<line x1="{x:.1f}" y1="{y0}" x2="{x:.1f}" y2="{y0+panel_h}" class="grid"/>')
            if panel_index == len(panels) - 1:
                body.append(f'<text x="{x:.1f}" y="{y0+panel_h+18}" class="small" text-anchor="middle">{tick}s</text>')
        pts = []
        for elapsed_s, value in values:
            x = left + (elapsed_s / max_elapsed) * chart_w
            y = y0 + panel_h - ((value - min_v) / (max_v - min_v)) * panel_h
            pts.append((x, y))
        body.append(f'<path d="{line_path(pts)}" fill="none" stroke="{color}" stroke-width="2.5"/>')

    write_svg(output_path, width, height, "\n".join(body))


def generate_disk_chart(csv_path: Path, output_path: Path) -> None:
    _macro, header, data = load_csv_rows(csv_path)
    if not data:
        return
    idx = {name: i for i, name in enumerate(header)}
    elapsed = [float_cell(row, idx, "elapsed_seconds") or 0.0 for row in data]
    max_elapsed = max(elapsed) or 1.0
    columns = [
        ("Workspace used delta", "current_container_disk_workspace_used_bytes", 1024 ** 2, "MB", "#7c3aed"),
        ("Airflow home used delta", "current_container_disk_airflow_home_used_bytes", 1024 ** 2, "MB", "#0891b2"),
    ]
    width = 1200
    height = 360
    left = 90
    right = 40
    top = 85
    chart_h = 210
    chart_w = width - left - right
    body = [
        '<text x="30" y="38" class="title">Disk usage delta during benchmark</text>',
        '<text x="30" y="60" class="subtitle">Disk growth is shown relative to the first resource sample.</text>',
    ]
    all_series = []
    max_v = 1.0
    for title, col, divisor, unit, color in columns:
        first = float_cell(data[0], idx, col) or 0.0
        values = []
        for row, elapsed_s in zip(data, elapsed):
            raw = float_cell(row, idx, col)
            if raw is not None:
                value = (raw - first) / divisor
                max_v = max(max_v, value)
                values.append((elapsed_s, value))
        all_series.append((title, values, unit, color))
    for tick in range(0, 6):
        y = top + chart_h - (tick / 5) * chart_h
        v = (tick / 5) * max_v
        body.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" class="grid"/>')
        body.append(f'<text x="{left-10}" y="{y+4:.1f}" class="small" text-anchor="end">{v:.1f} MB</text>')
    for tick in range(0, int(max_elapsed) + 1, max(20, int(max_elapsed // 5) or 20)):
        x = left + (tick / max_elapsed) * chart_w
        body.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top+chart_h}" class="grid"/>')
        body.append(f'<text x="{x:.1f}" y="{top+chart_h+18}" class="small" text-anchor="middle">{tick}s</text>')
    for i, (title, values, _unit, color) in enumerate(all_series):
        pts = []
        for elapsed_s, value in values:
            x = left + (elapsed_s / max_elapsed) * chart_w
            y = top + chart_h - (value / max_v) * chart_h
            pts.append((x, y))
        body.append(f'<path d="{line_path(pts)}" fill="none" stroke="{color}" stroke-width="2.5"/>')
        legend_x = 30 + i * 250
        body.append(f'<rect x="{legend_x}" y="{height-38}" width="18" height="10" fill="{color}"/>')
        body.append(f'<text x="{legend_x+24}" y="{height-30}" class="small">{svg_escape(title)}</text>')

    write_svg(output_path, width, height, "\n".join(body))


def wrap_task_label(task_id: str, max_chars: int = 22, max_lines: int = 3) -> list[str]:
    parts = str(task_id).split("_")
    lines: list[str] = []
    current = ""
    for part in parts:
        candidate = part if not current else f"{current}_{part}"
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            lines.append(current)
        if len(part) > max_chars:
            chunks = [part[index : index + max_chars] for index in range(0, len(part), max_chars)]
            lines.extend(chunks[:-1])
            current = chunks[-1]
        else:
            current = part
    if current:
        lines.append(current)
    if not lines:
        lines = [str(task_id)]
    if len(lines) > max_lines:
        lines = lines[: max_lines - 1] + [lines[max_lines - 1][: max_chars - 1] + "..."]
    return lines


def dag_graph_layout(structure: dict[str, Any]) -> tuple[list[str], list[tuple[str, str]], dict[str, tuple[float, float]]]:
    tasks = [str(item.get("task_id")) for item in structure.get("tasks", []) if item.get("task_id")]
    task_set = set(tasks)
    order = {task_id: index for index, task_id in enumerate(tasks)}
    edges = sorted(
        {
            (str(edge.get("source")), str(edge.get("target")))
            for edge in structure.get("edges", [])
            if edge.get("source") in task_set and edge.get("target") in task_set
        },
        key=lambda item: (order.get(item[0], 0), order.get(item[1], 0)),
    )

    successors: dict[str, list[str]] = {task_id: [] for task_id in tasks}
    indegree: dict[str, int] = {task_id: 0 for task_id in tasks}
    for source, target in edges:
        successors[source].append(target)
        indegree[target] += 1

    levels = {task_id: 0 for task_id in tasks}
    queue = sorted([task_id for task_id in tasks if indegree[task_id] == 0], key=order.get)
    processed: set[str] = set()
    while queue:
        task_id = queue.pop(0)
        processed.add(task_id)
        for target in sorted(successors[task_id], key=order.get):
            levels[target] = max(levels[target], levels[task_id] + 1)
            indegree[target] -= 1
            if indegree[target] == 0:
                queue.append(target)
                queue.sort(key=order.get)

    for task_id in tasks:
        if task_id not in processed:
            levels[task_id] = max(levels.values() or [0]) + 1

    layers: dict[int, list[str]] = {}
    for task_id in tasks:
        layers.setdefault(levels[task_id], []).append(task_id)
    for layer_tasks in layers.values():
        layer_tasks.sort(key=order.get)

    node_w = 156
    node_h = 42
    col_gap = 52
    row_gap = 26
    margin_x = 58
    margin_y = 125
    max_rows = max((len(layer_tasks) for layer_tasks in layers.values()), default=1)
    positions: dict[str, tuple[float, float]] = {}
    for level in sorted(layers):
        layer_tasks = layers[level]
        layer_top = margin_y + ((max_rows - len(layer_tasks)) * (node_h + row_gap) / 2)
        for row, task_id in enumerate(layer_tasks):
            positions[task_id] = (
                margin_x + level * (node_w + col_gap),
                layer_top + row * (node_h + row_gap),
            )
    return tasks, edges, positions


def generate_airflow_ui_graph(structure: dict[str, Any], output_path: Path) -> None:
    tasks, edges, positions = dag_graph_layout(structure)
    if not tasks:
        return

    node_w = 156
    node_h = 42
    max_x = max(x for x, _y in positions.values()) + node_w + 80
    max_y = max(y for _x, y in positions.values()) + node_h + 95
    width = max(1000, int(max_x))
    height = max(520, int(max_y))
    dag_id = str(structure.get("dag_id") or "DAG")

    body = [
        "<defs>",
        '<pattern id="airflow-dot-grid" width="14" height="14" patternUnits="userSpaceOnUse">',
        '<circle cx="1" cy="1" r="0.8" fill="#d9e2ec"/>',
        "</pattern>",
        '<marker id="airflow-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="strokeWidth">',
        '<path d="M 0 0 L 8 4 L 0 8 z" fill="#a8b3c2"/>',
        "</marker>",
        "</defs>",
        '<rect x="0" y="78" width="100%" height="100%" fill="url(#airflow-dot-grid)"/>',
        '<text x="30" y="38" class="title">Airflow UI-style DAG graph</text>',
        f'<text x="30" y="60" class="subtitle">{svg_escape(dag_id)} dependencies extracted from Airflow DagBag.</text>',
    ]

    for source, target in edges:
        if source not in positions or target not in positions:
            continue
        source_x, source_y = positions[source]
        target_x, target_y = positions[target]
        sx = source_x + node_w
        sy = source_y + node_h / 2
        tx = target_x
        ty = target_y + node_h / 2
        dx = max(34.0, (tx - sx) / 2)
        path = f"M {sx:.1f} {sy:.1f} C {sx + dx:.1f} {sy:.1f}, {tx - dx:.1f} {ty:.1f}, {tx:.1f} {ty:.1f}"
        body.append(
            f'<path d="{path}" fill="none" stroke="#a8b3c2" stroke-width="2" '
            'marker-end="url(#airflow-arrow)"/>'
        )

    for task_id in tasks:
        x, y = positions[task_id]
        lines = wrap_task_label(task_id)
        body.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{node_w}" height="{node_h}" '
            'rx="4" fill="#ffffff" stroke="#0a8f08" stroke-width="2"/>'
        )
        first_y = y + 16 if len(lines) > 1 else y + 26
        for index, line in enumerate(lines):
            body.append(
                f'<text x="{x + 8:.1f}" y="{first_y + index * 12:.1f}" '
                f'class="small">{svg_escape(line)}</text>'
            )
        body.append(f'<title>{svg_escape(task_id)}</title>')

    write_svg(output_path, width, height, "\n".join(body))


def generate_charts(client: AirflowClient, config: dict[str, Any], test_dir: Path, csv_path: Path, dags: list[RuntimeDag]) -> list[Path]:
    charts_cfg = config.get("charts", {})
    if not charts_cfg.get("enabled", True) or not dags:
        return []
    output_dir = resolve_from(test_dir, charts_cfg.get("output", "results/charts"))
    output_dir.mkdir(parents=True, exist_ok=True)

    timings_by_dag = {dag.dag_id: client.task_timings(dag.dag_id, dag.run_id) for dag in dags}
    outputs = [
        output_dir / "cosidag_gantt.svg",
        output_dir / "elapsed_time.svg",
        output_dir / "resources_memory_load_cpu.svg",
        output_dir / "resources_disk.svg",
    ]
    generate_gantt_chart(dags, timings_by_dag, outputs[0])
    generate_elapsed_chart(dags, outputs[1])
    generate_resource_chart(csv_path, outputs[2])
    generate_disk_chart(csv_path, outputs[3])

    airflow_graphs_cfg = charts_cfg.get("airflow_graphs", {})
    if airflow_graphs_cfg.get("enabled", True):
        graph_format = str(airflow_graphs_cfg.get("format", "png")).lstrip(".")
        graph_dir = output_dir / str(airflow_graphs_cfg.get("output", "airflow_graphs"))
        graph_dir.mkdir(parents=True, exist_ok=True)
        for dag in dags:
            structure = client.dag_structure(dag.dag_id)
            ui_graph_path = graph_dir / f"{dag.dag_id}_ui_graph.svg"
            generate_airflow_ui_graph(structure, ui_graph_path)
            outputs.append(ui_graph_path)

            if airflow_graphs_cfg.get("include_graphviz", False):
                graph_path = graph_dir / f"{dag.dag_id}_graph.{graph_format}"
                if client.export_dag_graph(dag.dag_id, graph_path):
                    outputs.append(graph_path)
                else:
                    log(
                        f"Airflow graph export failed for {dag.dag_id}; "
                        "Graphviz may be missing in the Airflow environment"
                    )
    return [path for path in outputs if path.exists()]


def runtime_dags_from_csv(csv_path: Path) -> list[RuntimeDag]:
    macro, header, data = load_csv_rows(csv_path)
    if not data:
        return []
    last = data[-1]
    idx = {name: i for i, name in enumerate(header)}
    dag_ids: list[str] = []
    for value in macro:
        if value not in {"test", "resources"} and value not in dag_ids:
            dag_ids.append(value)

    dags: list[RuntimeDag] = []
    for dag_id in dag_ids:
        task_ids = [
            column.removesuffix("_status")
            for column, group in zip(header, macro)
            if group == dag_id and column.endswith("_status")
        ]
        dags.append(
            RuntimeDag(
                dag_id=dag_id,
                run_id=last[idx.get(f"{dag_id}_run", 0)],
                trigger_time=last[idx.get(f"{dag_id}_triggered_at", 0)],
                end_time=last[idx.get(f"{dag_id}_finished_at", 0)],
                state=last[idx.get(f"{dag_id}_state", 0)],
                conf={},
                task_ids=task_ids,
            )
        )
    return dags


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a configurable Airflow DAG/COSIDAG performance test.")
    parser.add_argument("--config", default="cosiflow/test/performance_test.yaml", help="Path to the YAML config.")
    parser.add_argument(
        "--inside-container",
        action="store_true",
        help="Run Airflow and Python commands directly in the current Airflow container.",
    )
    parser.add_argument(
        "--finalize-only",
        action="store_true",
        help="Stop non-terminal runs for configured DAGs and pause them without cleanup or triggering.",
    )
    parser.add_argument(
        "--charts-only",
        action="store_true",
        help="Generate charts from the configured CSV without cleanup or triggering.",
    )
    args = parser.parse_args()

    os.environ.setdefault("PYTHONWARNINGS", "ignore::FutureWarning,ignore::UserWarning")

    config_path = Path(args.config).expanduser().resolve()
    config = load_config(config_path)
    test_dir, cosiflow_root, _workspace_root = script_paths(config_path)
    dag_configs = parse_dags(config)
    client = AirflowClient(config, test_dir, inside_container=args.inside_container)
    configured_dag_ids = [dag_cfg.dag_id for dag_cfg in dag_configs]
    csv_cfg = config.get("csv", {})
    csv_path = resolve_from(test_dir, csv_cfg.get("output", "results/cosidag_performance.csv"))

    if args.charts_only:
        chart_dags = runtime_dags_from_csv(csv_path)
        chart_paths = generate_charts(client, config, test_dir, csv_path, chart_dags)
        log("charts written:")
        for path in chart_paths:
            log(f"  {path}")
        return 0

    if args.finalize_only:
        stopped = client.stop_other_active_runs(configured_dag_ids, keep_run_ids=set())
        for dag_id in configured_dag_ids:
            log(f"pause {dag_id}")
            client.pause(dag_id)
        log(f"finalize-only complete: stopped_non_terminal_runs={len(stopped)}")
        return 0

    unpause = bool((config.get("airflow") or {}).get("unpause_before_trigger", True))
    final_cfg = config.get("finalization", {})
    started: list[RuntimeDag] = []
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    resource_columns: list[str] = []
    writer = None
    handle = None
    start_time = time.time()

    try:
        log(f"config: {config_path}")
        cleanup_data(config, cosiflow_root)

        for dag_cfg in dag_configs:
            dag_conf = cosidag_conf_for_dataset(dag_cfg, config)
            if dag_cfg.reset_cosidag_variable:
                log(f"reset COSIDAG_PROCESSED::{dag_cfg.dag_id}")
                client.reset_cosidag_variable(dag_cfg.dag_id)
            if unpause:
                log(f"unpause {dag_cfg.dag_id}")
                client.unpause(dag_cfg.dag_id)
            if not dag_cfg.trigger:
                continue
            run_id = dag_cfg.run_id or f"perf__{dag_cfg.dag_id}__{run_stamp}"
            task_ids = client.task_ids(dag_cfg.dag_id)
            trigger_time = utc_now()
            log(f"trigger {dag_cfg.dag_id}: run_id={run_id}")
            client.trigger(dag_cfg.dag_id, run_id, dag_conf)
            started.append(
                RuntimeDag(
                    dag_id=dag_cfg.dag_id,
                    run_id=run_id,
                    trigger_time=trigger_time,
                    conf=dag_conf,
                    task_ids=task_ids,
                )
            )

        if not started:
            log("no DAG runs were triggered")
            return 0

        initial_resources = resource_snapshot(client, config)
        resource_columns = sorted(initial_resources)
        macro, header = build_headers(started, resource_columns)

        append = bool(csv_cfg.get("append", False))
        interval = int((config.get("polling") or {}).get("interval_seconds", 30))
        timeout = int((config.get("polling") or {}).get("timeout_seconds", 14400))

        handle, writer = open_csv(csv_path, append, macro, header)
        log(f"csv: {csv_path}")
        while True:
            for dag in started:
                status = client.run_status(dag.dag_id, dag.run_id)
                dag.state = str(status.get("state") or "unknown")
                dag.task_states = dict(status.get("tasks") or {})
                if dag.state in TERMINAL_STATES and not dag.end_time:
                    dag.end_time = status.get("end_date") or utc_now()

            resources = resource_snapshot(client, config)
            if sorted(resources) != resource_columns:
                resources = {col: resources.get(col, "") for col in resource_columns}
            writer.writerow(row_values(started, resource_columns, resources, start_time))
            handle.flush()
            log(f"status: {status_summary(started)}")

            if terminal(started):
                break
            if time.time() - start_time >= timeout:
                log(f"timeout reached after {timeout} seconds")
                break
            time.sleep(interval)
    finally:
        stopped_runs = False
        if bool(final_cfg.get("stop_active_runs", False)):
            for dag in started:
                if dag.state not in TERMINAL_STATES:
                    client.stop_active_run(dag.dag_id, dag.run_id)
                    dag.state = "failed"
                    dag.end_time = dag.end_time or utc_now()
                    stopped_runs = True

        if started and bool(final_cfg.get("stop_other_active_runs", True)):
            keep_run_ids = {dag.run_id for dag in started}
            stopped = client.stop_other_active_runs(
                configured_dag_ids,
                keep_run_ids,
            )
            if stopped:
                stopped_runs = True
                log(f"stopped other active runs: {len(stopped)}")

        if started and bool(final_cfg.get("pause_dags", True)):
            for dag_cfg in dag_configs:
                log(f"pause {dag_cfg.dag_id}")
                client.pause(dag_cfg.dag_id)

        if stopped_runs and writer is not None:
            resources = resource_snapshot(client, config)
            resources = {col: resources.get(col, "") for col in resource_columns}
            writer.writerow(row_values(started, resource_columns, resources, start_time))
            if handle is not None:
                handle.flush()

        if handle is not None:
            handle.close()

    if "csv_path" in locals():
        log(f"performance CSV written to: {csv_path}")
        if started:
            try:
                chart_paths = generate_charts(client, config, test_dir, csv_path, started)
                if chart_paths:
                    log("charts written:")
                    for path in chart_paths:
                        log(f"  {path}")
            except Exception as exc:
                log(f"chart generation failed: {exc}")
    return 0 if terminal(started) else 2


if __name__ == "__main__":
    raise SystemExit(main())
