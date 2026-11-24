import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_AVAILABLE = importlib.util.find_spec("airflow") is not None


class Issue18ContractTests(unittest.TestCase):
    def test_removed_paths_module_has_no_supported_internal_consumer(self):
        self.assertFalse((REPO_ROOT / "modules" / "paths.py").exists())
        for source_root in ("modules", "dags", "callbacks", "plugins", "env", "gcn-client"):
            for path in (REPO_ROOT / source_root).rglob("*.py"):
                if "__pycache__" in path.parts:
                    continue
                source = path.read_text(errors="replace")
                self.assertNotIn("COSIFLOW_DATA_ROOT", source, str(path))
                self.assertNotIn("modules.paths", source, str(path))

    def test_cosidag_example_uses_runtime_import_path(self):
        source = (REPO_ROOT / "modules" / "cosidag.py").read_text()
        self.assertIn("# from cosidag import COSIDAG", source)
        self.assertNotIn("from cosiflow.cosidag import COSIDAG", source)

    @unittest.skipUnless(AIRFLOW_AVAILABLE, "Airflow is not installed")
    def test_cosidag_imports_from_mounted_modules_directory(self):
        sys.path.insert(0, str(REPO_ROOT / "modules"))
        try:
            from cosidag import COSIDAG
            from airflow import DAG

            self.assertTrue(issubclass(COSIDAG, DAG))
        finally:
            sys.path.pop(0)


if __name__ == "__main__":
    unittest.main()
