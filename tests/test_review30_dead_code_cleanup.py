from __future__ import annotations

import ast
import os
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_ROOT = REPO_ROOT / "plugins"


def production_python_files():
    for source_root in ("callbacks", "env", "gcn-client", "modules", "plugins"):
        yield from (REPO_ROOT / source_root).rglob("*.py")


def imported_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.asname or alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names)
    return names


class DeadHelperAndImportTests(unittest.TestCase):
    def test_only_public_cfg_compatibility_helper_is_retained(self):
        path = REPO_ROOT / "modules" / "cosidag.py"
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        functions = {
            node.name
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.assertIn("cfg", functions)
        self.assertTrue({"cfg_int", "cfg_float", "cfg_bool"}.isdisjoint(functions))

    def test_cfg_preserves_the_external_fasTP_configuration_contract(self):
        path = REPO_ROOT / "modules" / "cosidag.py"
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        cfg_node = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "cfg"
        )
        namespace = {"os": os}
        compiled = compile(
            ast.Module(body=[cfg_node], type_ignores=[]), str(path), "exec"
        )
        exec(compiled, namespace)
        cfg = namespace["cfg"]

        airflow = types.ModuleType("airflow")
        airflow_models = types.ModuleType("airflow.models")

        class FakeVariable:
            values = {}

            @classmethod
            def get(cls, key, default_var=None):
                return cls.values.get(key, default_var)

        airflow_models.Variable = FakeVariable
        with mock.patch.dict(
            sys.modules,
            {"airflow": airflow, "airflow.models": airflow_models},
        ):
            with mock.patch.dict(os.environ, {"CFG_CONTRACT": "environment"}, clear=False):
                self.assertEqual(cfg("CFG_CONTRACT", "default"), "environment")
                FakeVariable.values["CFG_CONTRACT"] = "airflow-variable"
                self.assertEqual(cfg("CFG_CONTRACT", "default"), "airflow-variable")
                self.assertEqual(cfg("CFG_MISSING", "default"), "default")

    def test_confirmed_unused_imports_are_removed(self):
        expectations = {
            REPO_ROOT / "modules" / "cosidag.py": "Variable",
            REPO_ROOT / "gcn-client" / "app" / "db" / "store.py": "Connection",
            REPO_ROOT / "plugins" / "data_explorer" / "data_explorer_plugin.py": "Environment",
            REPO_ROOT / "modules" / "notification_subscriptions.py": "Iterable",
        }
        for path, name in expectations.items():
            with self.subTest(path=path, name=name):
                self.assertNotIn(name, imported_names(path))


class RouteInventoryTests(unittest.TestCase):
    def test_appbuilder_routes_match_the_explicit_policy_inventory(self):
        from tests.security.auth_support import SHARED_AUTH
        from tests.security.test_endpoint_matrix import discover_endpoint_policies

        discovered, unprotected = discover_endpoint_policies()
        self.assertEqual(unprotected, [])
        self.assertEqual(discovered, SHARED_AUTH.ENDPOINT_POLICIES)
        self.assertEqual(len(discovered), 23)

    def test_no_direct_flask_route_or_root_url_rule_is_registered(self):
        violations = []
        for path in PLUGIN_ROOT.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    for decorator in node.decorator_list:
                        call = decorator if isinstance(decorator, ast.Call) else None
                        function = call.func if call else None
                        if (
                            isinstance(function, ast.Attribute)
                            and function.attr == "route"
                        ):
                            violations.append(f"{path}:{node.lineno}: Blueprint.route")
                if not isinstance(node, ast.Call):
                    continue
                function = node.func
                if not (
                    isinstance(function, ast.Attribute)
                    and function.attr == "add_url_rule"
                ):
                    continue
                first_arg = node.args[0] if node.args else None
                if isinstance(first_arg, ast.Constant) and first_arg.value == "/":
                    violations.append(f"{path}:{node.lineno}: add_url_rule('/')")
        self.assertEqual(violations, [])

    def test_mailhog_uses_only_the_supported_appbuilder_route(self):
        legacy = PLUGIN_ROOT / "mailhog_link" / "mailhog_link_plugin.py"
        current = PLUGIN_ROOT / "mailhog_link" / "mailhog_link_view_plugin.py"
        self.assertFalse(legacy.exists())
        source = current.read_text(encoding="utf-8")
        self.assertIn('route_base = "/mailhog"', source)
        self.assertIn('@expose("/")', source)


class GcnStorageOwnershipTests(unittest.TestCase):
    def test_shared_storage_is_the_only_gcn_identity_insert_owner(self):
        owners = {"inbound": [], "outbound": []}
        needles = {
            "inbound": "insert into gcn_inbound_notices",
            "outbound": "insert into gcn_outbound_notices",
        }
        for path in production_python_files():
            source = " ".join(path.read_text(encoding="utf-8").lower().split())
            for name, needle in needles.items():
                if needle in source:
                    owners[name].append(path.relative_to(REPO_ROOT).as_posix())
        expected = ["plugins/gcn_shared/storage.py"]
        self.assertEqual(owners["inbound"], expected)
        self.assertEqual(owners["outbound"], expected)

    def test_client_and_plugin_delegate_to_shared_storage(self):
        client = (REPO_ROOT / "gcn-client" / "app" / "db" / "store.py").read_text(
            encoding="utf-8"
        )
        plugin = (
            PLUGIN_ROOT / "explore_notices" / "explore_notices_plugin.py"
        ).read_text(encoding="utf-8")
        self.assertIn("from gcn_shared.storage import (", client)
        self.assertIn("insert_shared_inbound_notice(conn, notice)", client)
        self.assertIn("queue_shared_outbound_notice(", client)
        self.assertIn("from gcn_shared import (", plugin)
        self.assertIn("insert_inbound_notice(conn, notice)", plugin)
        self.assertIn("queue_outbound_notice(", plugin)


class RemovedPathContractTests(unittest.TestCase):
    def test_removed_path_contract_cannot_return(self):
        self.assertFalse((REPO_ROOT / "modules" / "paths.py").exists())
        forbidden = (
            "COSIFLOW_DATA_ROOT",
            "PathInfo",
            "build_url_fragment",
            "cosiflow.modules.path",
        )
        violations = []
        for path in production_python_files():
            source = path.read_text(encoding="utf-8", errors="replace")
            for token in forbidden:
                if token in source:
                    violations.append(f"{path.relative_to(REPO_ROOT)}: {token}")
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
