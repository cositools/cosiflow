from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PATCH_SCRIPT = REPO_ROOT / "env" / "patch_airflow_scheduler_warning.py"
SPEC = importlib.util.spec_from_file_location("scheduler_warning_patch", PATCH_SCRIPT)
assert SPEC and SPEC.loader
PATCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PATCH)


class SchedulerWarningPatchTests(unittest.TestCase):
    def make_template(self, content: str) -> Path:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / "main.html"
        path.write_text(content, encoding="utf-8")
        return path

    def test_replaces_fragile_heartbeat_rendering_with_operator_message(self):
        path = self.make_template(
            "before\n" + PATCH.ORIGINAL_WARNING + "after\n"
        )

        self.assertTrue(PATCH.patch_template(path))

        rendered = path.read_text(encoding="utf-8")
        self.assertIn(
            "The Airflow scheduler service is currently unavailable. "
            "Restart the service or try again later.",
            rendered,
        )
        self.assertNotIn("datetime_diff_for_humans", rendered)
        self.assertNotIn(PATCH.ORIGINAL_WARNING, rendered)

    def test_patch_is_idempotent(self):
        path = self.make_template(PATCH.SAFE_WARNING)
        self.assertFalse(PATCH.patch_template(path))
        self.assertEqual(path.read_text(encoding="utf-8"), PATCH.SAFE_WARNING)

    def test_unknown_upstream_template_fails_closed(self):
        path = self.make_template("unexpected upstream template")
        with self.assertRaisesRegex(RuntimeError, "refusing an unsafe patch"):
            PATCH.patch_template(path)


if __name__ == "__main__":
    unittest.main()
