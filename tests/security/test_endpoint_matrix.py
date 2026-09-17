from __future__ import annotations

import ast
import unittest
from pathlib import Path

from .auth_support import SHARED_AUTH
from .support import REPO_ROOT


PLUGIN_SOURCES = tuple((REPO_ROOT / "plugins").rglob("*.py"))


def decorator_name(decorator):
    function = decorator.func if isinstance(decorator, ast.Call) else decorator
    return getattr(function, "id", None) or getattr(function, "attr", None)


def exposed_methods(decorator):
    for keyword in decorator.keywords:
        if keyword.arg == "methods":
            return tuple(item.value for item in keyword.value.elts)
    return ("GET",)


def permission_pair(decorator):
    return tuple(getattr(SHARED_AUTH, argument.id) for argument in decorator.args)


def is_appbuilder_view(class_node):
    return any(
        (getattr(base, "id", None) or getattr(base, "attr", None)) == "BaseView"
        for base in class_node.bases
    )


def discover_endpoint_policies():
    discovered = {}
    exposed_without_permission = []
    for path in PLUGIN_SOURCES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for class_node in (node for node in tree.body if isinstance(node, ast.ClassDef)):
            if not is_appbuilder_view(class_node):
                continue
            for method in (node for node in class_node.body if isinstance(node, ast.FunctionDef)):
                decorators = {
                    decorator_name(decorator): decorator
                    for decorator in method.decorator_list
                    if isinstance(decorator, ast.Call)
                }
                expose = decorators.get("expose")
                if expose is None:
                    continue
                permission = decorators.get("require_cosiflow_permission")
                if permission is None:
                    exposed_without_permission.append(f"{class_node.name}.{method.name}")
                    continue
                for http_method in exposed_methods(expose):
                    discovered[(f"{class_node.name}.{method.name}", http_method)] = (
                        permission_pair(permission)
                    )
    return discovered, exposed_without_permission


class EndpointPolicyTests(unittest.TestCase):
    def test_every_sensitive_route_matches_the_canonical_policy_manifest(self):
        discovered, unprotected = discover_endpoint_policies()
        self.assertEqual(unprotected, [])
        self.assertEqual(discovered, SHARED_AUTH.ENDPOINT_POLICIES)

    def test_mutating_routes_are_post_only(self):
        discovered, _ = discover_endpoint_policies()
        mutating_endpoints = {
            "ExploreNoticesView.inject_inbox",
            "ExploreNoticesView.inject_outbox",
            "ResetCosidagView.reset_all_processed_paths",
            "ResetCosidagView.delete_processed_paths",
            "RefreshDagsView.refresh_dags",
        }
        for endpoint in mutating_endpoints:
            with self.subTest(endpoint=endpoint):
                methods = {
                    method for (name, method) in discovered if name == endpoint
                }
                self.assertEqual(methods, {"POST"})

    def test_role_permission_matrix_has_least_privilege(self):
        self.assertEqual(
            SHARED_AUTH.SCIENTIST_PERMISSIONS,
            {
                (SHARED_AUTH.ACTION_READ, SHARED_AUTH.GCN_NOTICES),
                (SHARED_AUTH.ACTION_READ, SHARED_AUTH.SCIENTIFIC_DATA),
                (SHARED_AUTH.ACTION_READ, SHARED_AUTH.COSIDAG_STATE),
            },
        )
        self.assertEqual(
            SHARED_AUTH.OPERATOR_PERMISSIONS,
            frozenset(SHARED_AUTH.PERMISSION_MANIFEST),
        )
        self.assertTrue(
            SHARED_AUTH.SCIENTIST_PERMISSIONS < SHARED_AUTH.OPERATOR_PERMISSIONS
        )

    def test_mutating_templates_submit_csrf_tokens(self):
        expectations = {
            REPO_ROOT / "plugins/explore_notices/templates/explore_notices.html": 2,
            REPO_ROOT / "plugins/refresh_dags_list/templates/refresh_dags.html": 1,
            REPO_ROOT / "plugins/reset_cosidag_link/templates/reset_cosidag.html": 1,
        }
        for path, minimum_form_tokens in expectations.items():
            with self.subTest(path=path):
                source = path.read_text(encoding="utf-8")
                self.assertGreaterEqual(source.lower().count('method="post"'), 1)
                self.assertGreaterEqual(
                    source.count('name="csrf_token"'), minimum_form_tokens
                )

        reset_template = (
            REPO_ROOT / "plugins/reset_cosidag_link/templates/reset_cosidag.html"
        ).read_text(encoding="utf-8")
        self.assertIn("method: 'POST'", reset_template)
        self.assertIn("'X-CSRFToken': getCsrfToken()", reset_template)


if __name__ == "__main__":
    unittest.main()
