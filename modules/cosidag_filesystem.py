"""Dependency-free filesystem helpers used by COSIDAG sensors.

The helpers in this module deliberately do not import Airflow.  This keeps
filesystem matching, stability checks, and load benchmarks deterministic and
cheap to exercise in unit tests.
"""
from __future__ import annotations

import fnmatch
import hashlib
import os
import re
import stat
from dataclasses import dataclass
from pathlib import PurePath
from typing import Iterable, Mapping, Optional


@dataclass(frozen=True)
class FileRecord:
    """Metadata collected with one stat during a directory inventory."""

    path: str
    relative_path: str
    basename: str
    size: int
    mtime_ns: int

    def snapshot(self) -> tuple[int, int]:
        return self.size, self.mtime_ns


def scan_file_inventory(root: str) -> list[FileRecord]:
    """Return a sorted recursive file inventory using one filesystem walk."""
    root = os.path.abspath(os.path.expanduser(root))
    records: list[FileRecord] = []
    for current_root, dirs, files in os.walk(root):
        dirs.sort()
        for filename in sorted(files):
            path = os.path.join(current_root, filename)
            try:
                stat_result = os.stat(path)
            except OSError:
                continue
            if not stat.S_ISREG(stat_result.st_mode):
                continue
            records.append(
                FileRecord(
                    path=path,
                    relative_path=os.path.relpath(path, root),
                    basename=filename,
                    size=stat_result.st_size,
                    mtime_ns=stat_result.st_mtime_ns,
                )
            )
    records.sort(key=lambda record: record.path)
    return records


def file_snapshot(path: str) -> Optional[tuple[int, int]]:
    """Return ``(size, mtime_ns)`` for a regular file, or ``None``."""
    try:
        stat_result = os.stat(path)
    except OSError:
        return None
    if not stat.S_ISREG(stat_result.st_mode):
        return None
    return stat_result.st_size, stat_result.st_mtime_ns


def directory_snapshot(path: str) -> Optional[tuple[int, int, int, str]]:
    """Return a compact snapshot that detects directory content changes.

    The count, total size, latest mtime, and digest are all derived from the
    same inventory.  The digest includes relative path, size, and mtime for
    every file, so changes that preserve aggregate totals are still detected.
    """
    if not os.path.isdir(path):
        return None
    inventory = scan_file_inventory(path)
    digest = hashlib.sha256()
    total_size = 0
    latest_mtime_ns = 0
    for record in inventory:
        total_size += record.size
        latest_mtime_ns = max(latest_mtime_ns, record.mtime_ns)
        digest.update(record.relative_path.encode("utf-8", errors="surrogateescape"))
        digest.update(b"\0")
        digest.update(str(record.size).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(record.mtime_ns).encode("ascii"))
        digest.update(b"\n")
    return len(inventory), total_size, latest_mtime_ns, digest.hexdigest()


def _matches_glob(record: FileRecord, pattern: str) -> bool:
    normalized = pattern.replace(os.sep, "/")
    relative = record.relative_path.replace(os.sep, "/")
    if "/" not in normalized:
        return fnmatch.fnmatchcase(record.basename, normalized)
    return PurePath(relative).match(normalized)


def resolve_patterns(
    inventory: Iterable[FileRecord],
    patterns: Mapping[str, str],
    select_policy: str,
) -> tuple[dict[str, FileRecord], list[str]]:
    """Resolve every pattern from one materialized inventory.

    Returns the selected records and keys that have no match.  Regex patterns
    retain the existing ``re.match``-against-basename behavior.
    """
    if select_policy not in {"first", "latest_mtime"}:
        raise ValueError(
            f"Unsupported select_policy {select_policy!r}; expected 'first' or 'latest_mtime'"
        )

    records = list(inventory)
    selected: dict[str, FileRecord] = {}
    missing: list[str] = []
    for key, pattern in patterns.items():
        if not isinstance(pattern, str):
            raise TypeError(f"Pattern for {key!r} must be a string")
        if pattern.startswith("regex:"):
            regex = re.compile(pattern[len("regex:") :])
            matches = [record for record in records if regex.match(record.basename)]
        else:
            matches = [record for record in records if _matches_glob(record, pattern)]

        if not matches:
            missing.append(key)
            continue
        if select_policy == "latest_mtime":
            selected[key] = max(matches, key=lambda record: record.mtime_ns)
        else:
            selected[key] = min(matches, key=lambda record: record.path)
    return selected, missing


def stability_observation(
    previous: Optional[Mapping[str, object]],
    identity: str,
    snapshot: object,
    observed_at: float,
    idle_seconds: int,
) -> tuple[bool, dict[str, object]]:
    """Compare a snapshot with the prior observation and track stable time."""
    if idle_seconds < 0:
        raise ValueError("idle_seconds must be non-negative")
    serialized_snapshot = list(snapshot) if isinstance(snapshot, tuple) else snapshot
    current = {
        "identity": identity,
        "snapshot": serialized_snapshot,
        "stable_since": observed_at,
    }
    if not previous:
        return False, current
    if previous.get("identity") != identity or previous.get("snapshot") != serialized_snapshot:
        return False, current
    try:
        stable_since = float(previous["stable_since"])
    except (KeyError, TypeError, ValueError):
        return False, current
    current["stable_since"] = stable_since
    return (observed_at - stable_since) >= idle_seconds, current
