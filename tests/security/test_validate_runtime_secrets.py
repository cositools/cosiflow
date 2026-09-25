from __future__ import annotations

import base64
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from .support import REPO_ROOT, load_script


VALIDATOR = load_script(
    "env/validate_runtime_secrets.py", "cosiflow_validate_runtime_secrets"
)


def valid_environment() -> dict[str, str]:
    return {
        "AIRFLOW_ADMIN_PASSWORD": "admin-password-unique-0001",
        "AIRFLOW__WEBSERVER__SECRET_KEY": "web-secret-key-unique-0002",
        "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY": "api-secret-key-unique-0003",
        "AIRFLOW__CORE__FERNET_KEY": base64.urlsafe_b64encode(b"f" * 32).decode(),
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USER": "airflow user",
        "POSTGRES_DB": "airflow/db",
        "POSTGRES_PASSWORD": "postgres-password-unique-0004",
        "GCN_DB_PASSWORD": "gcn-password-unique-0005",
    }


class RuntimeSecretValidationTests(unittest.TestCase):
    def test_valid_environment_is_accepted(self):
        with patch.dict(os.environ, valid_environment(), clear=True):
            self.assertEqual(VALIDATOR.validate_environment(), [])

    def test_every_required_secret_is_rejected_when_missing(self):
        environment = valid_environment()
        for name in VALIDATOR.REQUIRED_SECRETS:
            with self.subTest(name=name):
                candidate = dict(environment)
                candidate.pop(name)
                with patch.dict(os.environ, candidate, clear=True):
                    errors = VALIDATOR.validate_environment()
                self.assertIn(f"{name} is required and must not be empty", errors)

    def test_init_scope_does_not_require_runtime_gcn_password(self):
        environment = valid_environment()
        environment.pop("GCN_DB_PASSWORD")
        with patch.dict(os.environ, environment, clear=True):
            self.assertEqual(VALIDATOR.validate_environment("init"), [])

    def test_runtime_scope_does_not_receive_or_require_admin_password(self):
        environment = valid_environment()
        environment.pop("AIRFLOW_ADMIN_PASSWORD")
        with patch.dict(os.environ, environment, clear=True):
            self.assertEqual(VALIDATOR.validate_environment("runtime"), [])

    def test_each_scope_rejects_its_own_service_secret(self):
        cases = (
            ("init", "AIRFLOW_ADMIN_PASSWORD"),
            ("runtime", "GCN_DB_PASSWORD"),
        )
        for scope, name in cases:
            with self.subTest(scope=scope, name=name):
                environment = valid_environment()
                environment.pop(name)
                with patch.dict(os.environ, environment, clear=True):
                    errors = VALIDATOR.validate_environment(scope)
                self.assertIn(f"{name} is required and must not be empty", errors)

    def test_every_postgres_connection_field_is_rejected_when_missing(self):
        environment = valid_environment()
        for name in ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_DB"):
            with self.subTest(name=name):
                candidate = dict(environment)
                candidate.pop(name)
                with patch.dict(os.environ, candidate, clear=True):
                    errors = VALIDATOR.validate_environment()
                self.assertIn(f"{name} is required and must not be empty", errors)

    def test_short_passwords_and_secret_keys_are_rejected(self):
        names = (
            "AIRFLOW_ADMIN_PASSWORD",
            "AIRFLOW__WEBSERVER__SECRET_KEY",
            "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY",
            "POSTGRES_PASSWORD",
            "GCN_DB_PASSWORD",
        )
        for name in names:
            with self.subTest(name=name):
                environment = valid_environment()
                environment[name] = "too-short"
                with patch.dict(os.environ, environment, clear=True):
                    errors = VALIDATOR.validate_environment()
                self.assertIn(f"{name} must contain at least 16 characters", errors)

    def test_revoked_hashes_are_well_formed_and_enforced(self):
        self.assertTrue(VALIDATOR.COMPROMISED_VALUE_HASHES)
        for value_hash in VALIDATOR.COMPROMISED_VALUE_HASHES:
            self.assertRegex(value_hash, r"^[0-9a-f]{64}$")

        revoked_value = "known-revoked-value"
        revoked_hash = __import__("hashlib").sha256(revoked_value.encode()).hexdigest()
        environment = valid_environment()
        environment["POSTGRES_PASSWORD"] = revoked_value
        with (
            patch.dict(os.environ, environment, clear=True),
            patch.object(VALIDATOR, "COMPROMISED_VALUE_HASHES", {revoked_hash}),
        ):
            errors = VALIDATOR.validate_environment()
        self.assertIn("POSTGRES_PASSWORD still uses a revoked legacy value", errors)

    def test_airflow_keys_must_be_distinct(self):
        environment = valid_environment()
        environment["AIRFLOW__CORE__INTERNAL_API_SECRET_KEY"] = environment[
            "AIRFLOW__WEBSERVER__SECRET_KEY"
        ]
        with patch.dict(os.environ, environment, clear=True):
            errors = VALIDATOR.validate_environment()
        self.assertIn(
            "Airflow web, internal API, and Fernet keys must be distinct", errors
        )

    def test_fernet_key_must_decode_to_exactly_32_bytes(self):
        environment = valid_environment()
        for value in ("not-base64!", base64.urlsafe_b64encode(b"x" * 31).decode()):
            with self.subTest(value=value):
                environment["AIRFLOW__CORE__FERNET_KEY"] = value
                with patch.dict(os.environ, environment, clear=True):
                    errors = VALIDATOR.validate_environment()
                self.assertIn(
                    "AIRFLOW__CORE__FERNET_KEY contains an invalid Fernet key", errors
                )

    def test_postgres_port_must_be_in_range(self):
        for port in ("not-a-port", "0", "65536"):
            with self.subTest(port=port):
                environment = valid_environment()
                environment["POSTGRES_PORT"] = port
                with patch.dict(os.environ, environment, clear=True):
                    errors = VALIDATOR.validate_environment()
                self.assertIn(
                    "POSTGRES_PORT must be an integer between 1 and 65535", errors
                )

    def test_sqlalchemy_dsn_escapes_user_password_and_database(self):
        environment = valid_environment()
        environment["POSTGRES_PASSWORD"] = "password:/@ with spaces"
        with patch.dict(os.environ, environment, clear=True):
            dsn = VALIDATOR.sqlalchemy_dsn()
        self.assertEqual(
            dsn,
            "postgresql+psycopg2://airflow%20user:password%3A%2F%40%20with%20spaces"
            "@postgres:5432/airflow%2Fdb",
        )

    def test_command_failure_does_not_print_secret_values(self):
        environment = valid_environment()
        secret = "sensitive-but-invalid"
        environment["AIRFLOW__CORE__FERNET_KEY"] = secret
        result = subprocess.run(
            [sys.executable, str(REPO_ROOT / "env/validate_runtime_secrets.py")],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 2)
        self.assertNotIn(secret, result.stdout)
        self.assertNotIn(secret, result.stderr)


if __name__ == "__main__":
    unittest.main()
