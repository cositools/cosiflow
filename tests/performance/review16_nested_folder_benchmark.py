#!/usr/bin/env python3
"""Benchmark nested folder-candidate snapshots for Review 16."""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "modules"))

import cosidag_filesystem  # noqa: E402
from cosidag_filesystem import (  # noqa: E402
    directory_snapshot,
    scan_directory_snapshots,
)


def create_nested_tree(root: Path, depth: int, files_per_level: int) -> list[Path]:
    candidates = []
    current = root
    payload = b"cosiflow-review-16\n"
    for level in range(1, depth + 1):
        current = current / f"level-{level:03d}"
        current.mkdir()
        candidates.append(current)
        for index in range(files_per_level):
            (current / f"input-{level:03d}-{index:05d}.fits").write_bytes(payload)
    return candidates


def measured_call(file_paths: set[str], callback):
    original_walk = cosidag_filesystem.os.walk
    original_stat = cosidag_filesystem.os.stat
    walk_calls = 0
    file_visits = 0

    def counted_walk(*args, **kwargs):
        nonlocal walk_calls
        walk_calls += 1
        return original_walk(*args, **kwargs)

    def counted_stat(path, *args, **kwargs):
        nonlocal file_visits
        if os.fspath(path) in file_paths:
            file_visits += 1
        return original_stat(path, *args, **kwargs)

    started = time.perf_counter()
    with (
        patch.object(cosidag_filesystem.os, "walk", side_effect=counted_walk),
        patch.object(cosidag_filesystem.os, "stat", side_effect=counted_stat),
    ):
        result = callback()
    elapsed = time.perf_counter() - started
    return result, walk_calls, file_visits, elapsed


def benchmark_case(depth: int, files_per_level: int) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="cosidag-review16-nested-") as tmp:
        root = Path(tmp)
        candidates = create_nested_tree(root, depth, files_per_level)
        file_paths = {str(path) for path in root.rglob("*.fits")}

        legacy, legacy_walks, legacy_visits, legacy_seconds = measured_call(
            file_paths,
            lambda: {str(candidate): directory_snapshot(str(candidate)) for candidate in candidates},
        )
        current, current_walks, current_visits, current_seconds = measured_call(
            file_paths,
            lambda: scan_directory_snapshots(str(root), depth),
        )

        if current_walks != 1 or current_visits != len(file_paths):
            raise AssertionError(
                "nested scan is not linear: "
                f"walks={current_walks}, visits={current_visits}, files={len(file_paths)}"
            )
        for candidate in candidates:
            path = str(candidate)
            if legacy[path][:3] != current[path][:3]:
                raise AssertionError(f"snapshot aggregates differ for {path}")

        return {
            "candidate_depth": depth,
            "candidates": len(candidates),
            "files_per_level": files_per_level,
            "files": len(file_paths),
            "shared_inventory_walks": current_walks,
            "shared_inventory_file_visits": current_visits,
            "legacy_recursive_walks": legacy_walks,
            "legacy_file_visits": legacy_visits,
            "legacy_visit_amplification": round(legacy_visits / max(current_visits, 1), 2),
            "shared_inventory_seconds": round(current_seconds, 6),
            "legacy_seconds": round(legacy_seconds, 6),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--depths", default="4,8,16")
    parser.add_argument("--files-per-level", type=int, default=50)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    depths = [int(value.strip()) for value in args.depths.split(",") if value.strip()]
    if any(depth < 1 for depth in depths) or args.files_per_level < 1:
        parser.error("depths and files-per-level must be positive")

    results = [benchmark_case(depth, args.files_per_level) for depth in depths]
    rendered = json.dumps(results, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
