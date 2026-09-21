#!/usr/bin/env python3
"""Synthetic load test for Review 16 filesystem pattern resolution."""
from __future__ import annotations

import argparse
import csv
import glob
import json
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "modules"))

import cosidag_filesystem  # noqa: E402
from cosidag_filesystem import resolve_patterns, scan_file_inventory  # noqa: E402


EXTENSIONS = ("fits", "h5", "json", "dat")


def create_tree(root: Path, file_count: int, directories: int) -> None:
    for index in range(directories):
        (root / f"batch-{index:04d}").mkdir(parents=True, exist_ok=True)
    payload = b"cosiflow-review-16\n"
    for index in range(file_count):
        directory = root / f"batch-{index % directories:04d}"
        extension = EXTENSIONS[index % len(EXTENSIONS)]
        (directory / f"input-{index:08d}.{extension}").write_bytes(payload)


def build_patterns(pattern_count: int) -> dict[str, str]:
    patterns = {}
    for index in range(pattern_count):
        extension = EXTENSIONS[index % len(EXTENSIONS)]
        if index % 3 == 2:
            patterns[f"pattern_{index}"] = rf"regex:^input-.*\.{extension}$"
        else:
            patterns[f"pattern_{index}"] = f"*.{extension}"
    return patterns


def legacy_repeated_scan(root: Path, patterns: dict[str, str]) -> int:
    matches = 0
    for pattern in patterns.values():
        if pattern.startswith("regex:"):
            matches += len(glob.glob(str(root / "**" / "*"), recursive=True))
        else:
            matches += len(glob.glob(str(root / "**" / pattern), recursive=True))
    return matches


def benchmark_case(file_count: int, pattern_count: int, directories: int) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="cosidag-review16-") as tmp:
        root = Path(tmp)
        create_tree(root, file_count, directories)
        patterns = build_patterns(pattern_count)

        legacy_start = time.perf_counter()
        legacy_matches = legacy_repeated_scan(root, patterns)
        legacy_seconds = time.perf_counter() - legacy_start

        walk_calls = 0
        original_walk = cosidag_filesystem.os.walk

        def counted_walk(*walk_args, **walk_kwargs):
            nonlocal walk_calls
            walk_calls += 1
            return original_walk(*walk_args, **walk_kwargs)

        start = time.perf_counter()
        with patch.object(cosidag_filesystem.os, "walk", side_effect=counted_walk):
            inventory = scan_file_inventory(str(root))
            selected, missing = resolve_patterns(inventory, patterns, "first")
        elapsed_seconds = time.perf_counter() - start

        tracemalloc.start()
        measured_inventory = scan_file_inventory(str(root))
        resolve_patterns(measured_inventory, patterns, "first")
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        return {
            "files": file_count,
            "directories": directories,
            "patterns": pattern_count,
            "inventory_records": len(inventory),
            "selected": len(selected),
            "missing": len(missing),
            "filesystem_walks_per_cycle": walk_calls,
            "legacy_recursive_scans_per_cycle": pattern_count,
            "elapsed_seconds": round(elapsed_seconds, 6),
            "legacy_elapsed_seconds": round(legacy_seconds, 6),
            "peak_memory_bytes": peak_bytes,
            "legacy_match_entries_materialized": legacy_matches,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", default="1000,5000,10000")
    parser.add_argument("--patterns", type=int, default=8)
    parser.add_argument("--directories", type=int, default=50)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    sizes = [int(item.strip()) for item in args.sizes.split(",") if item.strip()]
    results = [
        benchmark_case(size, args.patterns, min(args.directories, max(1, size)))
        for size in sizes
    ]

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        if args.output.suffix.lower() == ".csv":
            with args.output.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(results[0]))
                writer.writeheader()
                writer.writerows(results)
        else:
            args.output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
