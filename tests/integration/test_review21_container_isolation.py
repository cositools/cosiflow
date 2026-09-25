from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_DIR = REPO_ROOT / "env"


@unittest.skipUnless(
    os.environ.get("COSIFLOW_RUN_REVIEW21_CONTAINER_TESTS") == "1",
    "set COSIFLOW_RUN_REVIEW21_CONTAINER_TESTS=1 to run the Review 21 container test",
)
class Review21ContainerIsolationTest(unittest.TestCase):
    def test_base_airflow_container_cannot_reach_host_secrets_or_write_code(self):
        command = [
            "docker",
            "compose",
            "-f",
            str(ENV_DIR / "docker-compose.yaml"),
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "/bin/bash",
            "airflow",
            "-ec",
            """
test ! -e /shared_dir
test ! -e /tmp/.X11-unix
test ! -e /home/gamma/airflow/modules_pool/cosiflow/env/.env
test -r /home/gamma/airflow/airflow.cfg
test ! -w /home/gamma/airflow/plugins
test ! -w /home/gamma/airflow/modules
test -w /home/gamma/workspace/data
test -w /home/gamma/airflow/logs
""",
        ]
        result = subprocess.run(
            command,
            cwd=ENV_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
