from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_DIR = REPO_ROOT / "env"
ENTRYPOINT = ENV_DIR / "entrypoint-airflow.sh"
COMPOSE = ENV_DIR / "docker-compose.yaml"


def compose_environment(**overrides: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "AIRFLOW_ADMIN_PASSWORD": "review22-admin-password",
            "AIRFLOW__WEBSERVER__SECRET_KEY": "review22-web-secret-key",
            "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY": "review22-api-secret-key",
            "AIRFLOW__CORE__FERNET_KEY": (
                "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
            ),
            "POSTGRES_PASSWORD": "review22-postgres-password",
            "GCN_DB_PASSWORD": "review22-gcn-password",
            "GCN_MYSQL_ROOT_PASSWORD": "review22-gcn-root-password",
            "GCN_CLIENT_ID": "review22-client-id",
            "GCN_CLIENT_SECRET": "review22-client-secret",
        }
    )
    environment.update(overrides)
    return environment


def resolved_compose(**overrides: str) -> dict:
    result = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE), "config", "--format", "json"],
        cwd=ENV_DIR,
        env=compose_environment(**overrides),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr)
    return json.loads(result.stdout)


def runtime_environment(root: Path, airflow_script: str) -> dict[str, str]:
    fake_bin = root / "bin"
    fake_bin.mkdir()
    airflow = fake_bin / "airflow"
    airflow.write_text(airflow_script, encoding="utf-8")
    airflow.chmod(0o755)
    (fake_bin / "python").symlink_to(sys.executable)

    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{fake_bin}:{environment['PATH']}",
            "COSI_RUNTIME_HOME": str(ENV_DIR),
            "COSI_DATA_DIR": str(root / "data"),
            "AIRFLOW__WEBSERVER__SECRET_KEY": "review22-web-secret-key",
            "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY": "review22-api-secret-key",
            "AIRFLOW__CORE__FERNET_KEY": (
                "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
            ),
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "airflow_user",
            "POSTGRES_DB": "airflow_db",
            "POSTGRES_PASSWORD": "review22-postgres-password",
            "GCN_DB_PASSWORD": "review22-gcn-password",
            "AIRFLOW_WEBUI_PORT": "18080",
            "MAILHOG_WEBUI_PORT": "18025",
            "HOST_IP": "127.0.0.1",
            "AIRFLOW_PUBLIC_BASE_URL": "",
            "CAPTURE_FILE": str(root / "capture.json"),
        }
    )
    return environment


class Review22EntrypointTests(unittest.TestCase):
    FAKE_AIRFLOW = """#!/bin/sh
python3 -c 'import json, os, sys; json.dump({"args": sys.argv[1:], "base_url": os.environ.get("AIRFLOW__WEBSERVER__BASE_URL"), "home_url": os.environ.get("COSIFLOW_HOME_URL")}, open(os.environ["CAPTURE_FILE"], "w"))' "$@"
exit "${FAKE_EXIT_STATUS:-0}"
"""

    def run_mode(self, mode: str, **overrides: str):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        environment = runtime_environment(root, self.FAKE_AIRFLOW)
        environment.update(overrides)
        result = subprocess.run(
            ["bash", str(ENTRYPOINT), mode],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        capture_path = root / "capture.json"
        capture = json.loads(capture_path.read_text()) if capture_path.exists() else None
        return result, capture

    def test_webserver_and_scheduler_exec_one_process(self):
        cases = (
            ("webserver", ["webserver", "--port", "8080"]),
            ("scheduler", ["scheduler"]),
        )
        for mode, expected in cases:
            with self.subTest(mode=mode):
                result, capture = self.run_mode(mode)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(capture["args"], expected)

    def test_child_exit_status_is_the_service_exit_status(self):
        result, _ = self.run_mode("scheduler", FAKE_EXIT_STATUS="23")
        self.assertEqual(result.returncode, 23)

    def test_default_public_url_follows_configured_host_port(self):
        result, capture = self.run_mode("webserver")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(capture["base_url"], "http://127.0.0.1:18080")
        self.assertEqual(
            capture["home_url"], "http://127.0.0.1:18080/heasarcbrowser"
        )

    def test_explicit_public_url_override_is_preserved(self):
        result, capture = self.run_mode(
            "webserver", AIRFLOW_PUBLIC_BASE_URL="https://cosiflow.example/airflow/"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(capture["base_url"], "https://cosiflow.example/airflow/")
        self.assertEqual(
            capture["home_url"],
            "https://cosiflow.example/airflow/heasarcbrowser",
        )

    def test_sigterm_reaches_execed_scheduler(self):
        script = """#!/bin/sh
trap 'printf terminated > "$TERM_FILE"; exit 0' TERM
printf ready > "$READY_FILE"
while :; do sleep 0.1; done
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            environment = runtime_environment(root, script)
            ready = root / "ready"
            terminated = root / "terminated"
            environment["READY_FILE"] = str(ready)
            environment["TERM_FILE"] = str(terminated)
            process = subprocess.Popen(
                ["bash", str(ENTRYPOINT), "scheduler"],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 5
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(ready.exists(), "scheduler fixture did not start")
                process.terminate()
                process.wait(timeout=5)
                self.assertEqual(process.returncode, 0)
                self.assertTrue(terminated.exists())
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=5)


class Review22ComposeTests(unittest.TestCase):
    def test_runtime_is_split_into_single_process_services(self):
        services = resolved_compose()["services"]
        self.assertNotIn("airflow", services)
        self.assertEqual(
            services["airflow-webserver"]["entrypoint"],
            ["bash", "/home/gamma/entrypoint-airflow.sh", "webserver"],
        )
        self.assertEqual(
            services["airflow-scheduler"]["entrypoint"],
            ["bash", "/home/gamma/entrypoint-airflow.sh", "scheduler"],
        )

    def test_both_runtime_services_share_the_successful_init_gate(self):
        services = resolved_compose()["services"]
        for name in ("airflow-webserver", "airflow-scheduler"):
            with self.subTest(service=name):
                self.assertEqual(
                    services[name]["depends_on"]["airflow-init"]["condition"],
                    "service_completed_successfully",
                )
                self.assertEqual(services[name]["restart"], "on-failure:5")
                self.assertEqual(services[name]["stop_grace_period"], "30s")
                self.assertIn("healthcheck", services[name])

    def test_only_webserver_publishes_the_configured_airflow_port(self):
        services = resolved_compose(AIRFLOW_WEBUI_PORT="18080")["services"]
        self.assertNotIn("ports", services["airflow-scheduler"])
        self.assertEqual(
            services["airflow-webserver"]["ports"][0],
            {
                "mode": "ingress",
                "target": 8080,
                "published": "18080",
                "protocol": "tcp",
                "host_ip": "127.0.0.1",
            },
        )
        self.assertEqual(
            services["airflow-webserver"]["environment"]["AIRFLOW_PUBLIC_BASE_URL"],
            "",
        )

    def test_mailhog_web_port_follows_configuration(self):
        services = resolved_compose(MAILHOG_WEBUI_PORT="18025")["services"]
        published = {port["target"]: port["published"] for port in services["mailhog"]["ports"]}
        self.assertEqual(published[8025], "18025")


if __name__ == "__main__":
    unittest.main()
