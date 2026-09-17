from __future__ import annotations

import base64
import hashlib
import io
import stat
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from .support import load_script


BOOTSTRAP = load_script("env/bootstrap-secrets.py", "cosiflow_bootstrap_secrets")


class BootstrapSecretTests(unittest.TestCase):
    def test_load_and_update_preserve_comments_and_unmanaged_values(self):
        lines = ["# local settings", "UNMANAGED=value", "POSTGRES_PASSWORD=old"]
        updated = BOOTSTRAP.update_lines(lines, {"POSTGRES_PASSWORD": "new"})
        self.assertEqual(
            updated,
            ["# local settings", "UNMANAGED=value", "POSTGRES_PASSWORD=new", ""],
        )

    def test_missing_secrets_are_generated_with_private_permissions(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / ".env"
            path.write_text("UNMANAGED=preserved\n", encoding="utf-8")
            output = io.StringIO()
            with (
                patch("sys.argv", ["bootstrap-secrets.py", "--env-file", str(path)]),
                redirect_stdout(output),
            ):
                self.assertEqual(BOOTSTRAP.main(), 0)

            _, values = BOOTSTRAP.load_env(path)
            self.assertEqual(values["UNMANAGED"], "preserved")
            for name in BOOTSTRAP.GENERATORS:
                self.assertTrue(values[name], name)
                self.assertNotIn(values[name], output.getvalue())
            self.assertEqual(len(set(values[name] for name in BOOTSTRAP.GENERATORS)), 7)
            self.assertEqual(
                len(base64.urlsafe_b64decode(values["AIRFLOW__CORE__FERNET_KEY"])),
                32,
            )
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_existing_non_revoked_secrets_are_not_replaced(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / ".env"
            originals = {
                name: f"existing-{index:02d}-secret-value"
                for index, name in enumerate(BOOTSTRAP.GENERATORS, start=1)
            }
            path.write_text(
                "\n".join(f"{name}={value}" for name, value in originals.items())
                + "\n",
                encoding="utf-8",
            )
            with (
                patch("sys.argv", ["bootstrap-secrets.py", "--env-file", str(path)]),
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(BOOTSTRAP.main(), 0)
            _, values = BOOTSTRAP.load_env(path)
            self.assertEqual(values, originals)

    def test_revoked_secret_is_replaced_without_printing_its_value(self):
        revoked = "revoked-value-that-must-not-leak"
        revoked_hash = hashlib.sha256(revoked.encode()).hexdigest()
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / ".env"
            path.write_text(f"POSTGRES_PASSWORD={revoked}\n", encoding="utf-8")
            with (
                patch.object(BOOTSTRAP, "REVOKED_HASHES", {revoked_hash}),
                patch("sys.argv", ["bootstrap-secrets.py", "--env-file", str(path)]),
                patch("builtins.print") as print_mock,
            ):
                self.assertEqual(BOOTSTRAP.main(), 0)

            _, values = BOOTSTRAP.load_env(path)
            self.assertNotEqual(values["POSTGRES_PASSWORD"], revoked)
            output = " ".join(str(call) for call in print_mock.call_args_list)
            self.assertNotIn(revoked, output)

    def test_rotate_replaces_every_managed_secret(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / ".env"
            originals = {
                name: f"existing-{index:02d}-secret-value"
                for index, name in enumerate(BOOTSTRAP.GENERATORS, start=1)
            }
            path.write_text(
                "\n".join(f"{name}={value}" for name, value in originals.items())
                + "\n",
                encoding="utf-8",
            )
            with (
                patch(
                    "sys.argv",
                    ["bootstrap-secrets.py", "--env-file", str(path), "--rotate"],
                ),
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(BOOTSTRAP.main(), 0)
            _, values = BOOTSTRAP.load_env(path)
            for name, original in originals.items():
                self.assertNotEqual(values[name], original)


if __name__ == "__main__":
    unittest.main()
