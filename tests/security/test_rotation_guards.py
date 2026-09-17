from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Dict, Optional

from .support import REPO_ROOT


ROTATION_SCRIPT = REPO_ROOT / "env/rotate-local-databases.sh"


class CredentialRotationGuardTests(unittest.TestCase):
    def run_rotation(
        self,
        working_directory: Path,
        *arguments: str,
        environment: Optional[Dict[str, str]] = None,
    ) -> subprocess.CompletedProcess[str]:
        process_environment = {"PATH": os.environ.get("PATH", "")}
        if environment:
            process_environment.update(environment)
        return subprocess.run(
            ["bash", str(ROTATION_SCRIPT), *arguments],
            cwd=working_directory,
            env=process_environment,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_rotation_refuses_to_start_without_backup_confirmation(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = self.run_rotation(Path(temporary_directory))
        self.assertEqual(result.returncode, 2)
        self.assertIn("Refusing rotation", result.stderr)

    def test_rotation_requires_all_previous_credentials(self):
        complete_environment = {
            "OLD_POSTGRES_PASSWORD": "old-postgres-password",
            "OLD_GCN_DB_PASSWORD": "old-gcn-password",
            "OLD_GCN_MYSQL_ROOT_PASSWORD": "old-root-password",
        }
        for missing_name in complete_environment:
            with self.subTest(missing_name=missing_name):
                environment = dict(complete_environment)
                environment.pop(missing_name)
                with tempfile.TemporaryDirectory() as temporary_directory:
                    result = self.run_rotation(
                        Path(temporary_directory),
                        "--backup-confirmed",
                        environment=environment,
                    )
                self.assertEqual(result.returncode, 2)
                self.assertIn(f"{missing_name} is required", result.stderr)

    def test_unsafe_generated_passwords_fail_before_docker_is_called(self):
        environment = {
            "OLD_POSTGRES_PASSWORD": "old-postgres-password",
            "OLD_GCN_DB_PASSWORD": "old-gcn-password",
            "OLD_GCN_MYSQL_ROOT_PASSWORD": "old-root-password",
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            marker = directory / "docker-was-called"
            fake_bin = directory / "bin"
            fake_bin.mkdir()
            fake_docker = fake_bin / "docker"
            fake_docker.write_text(
                f"#!/bin/sh\ntouch {marker!s}\nexit 99\n", encoding="utf-8"
            )
            fake_docker.chmod(0o755)
            (directory / ".env").write_text(
                "POSTGRES_PASSWORD=short\n"
                "GCN_DB_PASSWORD=short\n"
                "GCN_MYSQL_ROOT_PASSWORD=short\n",
                encoding="utf-8",
            )
            environment["PATH"] = f"{fake_bin}{os.pathsep}{os.environ.get('PATH', '')}"
            result = self.run_rotation(
                directory, "--backup-confirmed", environment=environment
            )

            self.assertEqual(result.returncode, 2)
            self.assertIn("unsafe format", result.stderr)
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
