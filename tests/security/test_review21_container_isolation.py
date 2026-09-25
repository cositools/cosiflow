from __future__ import annotations

import json
import os
import subprocess
import unittest
from pathlib import Path

from .support import REPO_ROOT


ENV_DIR = REPO_ROOT / "env"
BASE_COMPOSE = ENV_DIR / "docker-compose.yaml"
DEVELOPMENT_COMPOSE = ENV_DIR / "docker-compose.development.yaml"
X11_COMPOSE = ENV_DIR / "docker-compose.x11.yaml"

REQUIRED_ENVIRONMENT = {
    "AIRFLOW_ADMIN_PASSWORD": "review21-admin-password",
    "AIRFLOW__WEBSERVER__SECRET_KEY": "review21-web-secret-key",
    "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY": "review21-api-secret-key",
    "AIRFLOW__CORE__FERNET_KEY": "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
    "POSTGRES_PASSWORD": "review21-postgres-password",
    "GCN_DB_PASSWORD": "review21-gcn-db-password",
    "GCN_MYSQL_ROOT_PASSWORD": "review21-gcn-root-password",
    "GCN_CLIENT_ID": "review21-client-id",
    "GCN_CLIENT_SECRET": "review21-client-secret",
    "DISPLAY": ":99",
}


def compose_config(*overrides: Path) -> dict:
    command = ["docker", "compose", "-f", str(BASE_COMPOSE)]
    for override in overrides:
        command.extend(("-f", str(override)))
    command.extend(("config", "--format", "json"))
    environment = os.environ.copy()
    environment.update(REQUIRED_ENVIRONMENT)
    result = subprocess.run(
        command,
        cwd=ENV_DIR,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr)
    return json.loads(result.stdout)


def mounts_by_target(service: dict) -> dict[str, dict]:
    return {mount["target"]: mount for mount in service.get("volumes", [])}


class Review21ComposeIsolationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = compose_config()
        cls.services = cls.base["services"]

    def test_airflow_environment_is_scoped_by_service(self):
        runtime = self.services["airflow"]["environment"]
        initialization = self.services["airflow-init"]["environment"]

        self.assertNotIn("AIRFLOW_ADMIN_PASSWORD", runtime)
        self.assertNotIn("AIRFLOW_ADMIN_USERNAME", runtime)
        self.assertNotIn("AIRFLOW_ADMIN_EMAIL", runtime)
        self.assertIn("GCN_DB_PASSWORD", runtime)

        self.assertIn("AIRFLOW_ADMIN_PASSWORD", initialization)
        self.assertNotIn("GCN_DB_PASSWORD", initialization)
        self.assertNotIn("GCN_CONSUMER_TOPICS", initialization)

    def test_base_stack_exposes_no_root_or_x11_mount(self):
        forbidden_targets = {"/shared_dir", "/tmp/.X11-unix"}
        forbidden_sources = {str(REPO_ROOT.resolve()), str(REPO_ROOT.parent.resolve())}
        for service_name in ("airflow", "airflow-init"):
            with self.subTest(service=service_name):
                mounts = self.services[service_name].get("volumes", [])
                self.assertTrue(forbidden_targets.isdisjoint(m["target"] for m in mounts))
                self.assertTrue(forbidden_sources.isdisjoint(m["source"] for m in mounts))

        self.assertNotIn("DISPLAY", self.services["airflow"]["environment"])

    def test_runtime_code_and_configuration_mounts_are_read_only(self):
        mounts = mounts_by_target(self.services["airflow"])
        read_only_targets = {
            "/home/gamma/envs",
            "/home/gamma/airflow/dags",
            "/home/gamma/airflow/modules_pool",
            "/home/gamma/airflow/plugins",
            "/home/gamma/airflow/pipeline",
            "/home/gamma/airflow/callbacks",
            "/home/gamma/airflow/modules",
            "/home/gamma/airflow/airflow.cfg",
        }
        for target in read_only_targets:
            with self.subTest(target=target):
                self.assertTrue(mounts[target]["read_only"])

        self.assertFalse(mounts["/home/gamma/workspace/data"].get("read_only", False))
        self.assertFalse(mounts["/home/gamma/airflow/logs"].get("read_only", False))

    def test_initialization_has_only_required_mounts_and_database_network(self):
        service = self.services["airflow-init"]
        self.assertEqual(
            set(mounts_by_target(service)),
            {
                "/home/gamma/airflow/plugins",
                "/home/gamma/airflow/modules",
                "/home/gamma/airflow/airflow.cfg",
                "/home/gamma/workspace/data",
                "/home/gamma/airflow/logs",
            },
        )
        self.assertEqual(set(service["networks"]), {"database"})

    def test_development_override_grants_only_documented_write_paths(self):
        config = compose_config(DEVELOPMENT_COMPOSE)
        mounts = mounts_by_target(config["services"]["airflow"])
        for target in (
            "/home/gamma/envs",
            "/home/gamma/airflow/dags",
            "/home/gamma/airflow/pipeline",
        ):
            with self.subTest(target=target):
                self.assertFalse(mounts[target].get("read_only", False))
        self.assertTrue(mounts["/shared_dir/test"]["read_only"])
        self.assertNotIn("/shared_dir", mounts)

    def test_x11_override_is_explicit_and_read_only(self):
        config = compose_config(X11_COMPOSE)
        airflow = config["services"]["airflow"]
        self.assertEqual(airflow["environment"]["DISPLAY"], ":99")
        x11_mount = mounts_by_target(airflow)["/tmp/.X11-unix"]
        self.assertEqual(x11_mount["source"], "/tmp/.X11-unix")
        self.assertTrue(x11_mount["read_only"])


if __name__ == "__main__":
    unittest.main()
