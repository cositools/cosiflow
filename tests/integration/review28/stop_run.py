from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path


MODULE_PATH = Path("/opt/review28/performance_test.py")
SPEC = importlib.util.spec_from_file_location("review28_performance_test", MODULE_PATH)
module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dag_id")
    parser.add_argument("run_id")
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    client = module.AirflowClient({}, Path("/opt/review28"), inside_container=True)
    result = client.stop_active_run(
        args.dag_id,
        args.run_id,
        timeout_seconds=args.timeout,
        poll_seconds=0.5,
    )
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("verified") else 2


if __name__ == "__main__":
    raise SystemExit(main())
