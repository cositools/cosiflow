from __future__ import annotations

import re
import unittest

from .support import REPO_ROOT


COMPOSE_PATH = REPO_ROOT / "env/docker-compose.yaml"
RUNTIME_MANIFESTS = (
    COMPOSE_PATH,
    REPO_ROOT / "env/Dockerfile.airflow",
    REPO_ROOT / "env/entrypoint-airflow.sh",
    REPO_ROOT / "gcn-client/Dockerfile",
)


class ComposeSecurityContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = COMPOSE_PATH.read_text(encoding="utf-8")

    def test_required_secrets_have_no_compose_default(self):
        names = (
            "AIRFLOW_ADMIN_PASSWORD",
            "AIRFLOW__WEBSERVER__SECRET_KEY",
            "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY",
            "AIRFLOW__CORE__FERNET_KEY",
            "POSTGRES_PASSWORD",
            "GCN_DB_PASSWORD",
            "GCN_MYSQL_ROOT_PASSWORD",
            "GCN_CLIENT_ID",
            "GCN_CLIENT_SECRET",
        )
        for name in names:
            with self.subTest(name=name):
                self.assertIn(f"${{{name}:?{name} is required}}", self.source)
                self.assertNotRegex(self.source, rf"\${{{re.escape(name)}:-")

    def test_application_services_have_no_docker_daemon_path(self):
        runtime_configuration = "\n".join(
            path.read_text(encoding="utf-8") for path in RUNTIME_MANIFESTS
        )
        forbidden = ("/var/run/docker.sock", "DOCKER_HOST", "2375")
        for value in forbidden:
            with self.subTest(value=value):
                self.assertNotIn(value, runtime_configuration)

    def test_every_published_port_is_bound_to_loopback(self):
        published_ports = re.findall(r'^\s+-\s+"([^"\n]+:[^"\n]+)"\s*$', self.source, re.MULTILINE)
        self.assertTrue(published_ports)
        for port_mapping in published_ports:
            with self.subTest(port_mapping=port_mapping):
                self.assertTrue(port_mapping.startswith("127.0.0.1:"))

    def test_database_network_is_internal(self):
        self.assertRegex(
            self.source,
            r"(?ms)^\s{2}database:\s*\n\s{4}internal:\s*true\s*$",
        )


if __name__ == "__main__":
    unittest.main()
