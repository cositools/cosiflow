from __future__ import annotations

import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from .auth_support import SHARED_AUTH
from .support import load_script


with patch.dict(sys.modules, {"shared_auth": SHARED_AUTH}):
    CONFIGURE_RBAC = load_script("env/configure_rbac.py", "cosiflow_configure_rbac")


class Permission:
    def __init__(self, action, resource):
        self.action = SimpleNamespace(name=action)
        self.resource = SimpleNamespace(name=resource)


class Role:
    def __init__(self, name, permissions=()):
        self.name = name
        self.permissions = list(permissions)


class FakeSecurityManager:
    def __init__(self):
        self.permissions = {}
        viewer_base = self.create_permission("can_read", "DAGs")
        operator_base = self.create_permission("can_edit", "DAGs")
        admin_permissions = [
            self.create_permission(SHARED_AUTH.ACTION_MENU_ACCESS, menu_name)
            for menu_name in CONFIGURE_RBAC.MANAGED_MENUS
        ]
        self.roles = {
            "Viewer": Role("Viewer", [viewer_base]),
            "Op": Role("Op", [viewer_base, operator_base]),
            "Admin": Role("Admin", admin_permissions),
        }

    def create_permission(self, action, resource):
        return self.permissions.setdefault((action, resource), Permission(action, resource))

    def get_permission(self, action, resource):
        return self.permissions.get((action, resource))

    def find_role(self, name):
        return self.roles.get(name)

    def add_role(self, name):
        role = Role(name)
        self.roles[name] = role
        return role

    def add_permission_to_role(self, role, permission):
        pair = CONFIGURE_RBAC.permission_pair(permission)
        if pair not in CONFIGURE_RBAC.role_pairs(role):
            role.permissions.append(permission)

    def remove_permission_from_role(self, role, permission):
        pair = CONFIGURE_RBAC.permission_pair(permission)
        role.permissions = [
            item
            for item in role.permissions
            if CONFIGURE_RBAC.permission_pair(item) != pair
        ]


def role_snapshot(security_manager):
    return {
        name: frozenset(CONFIGURE_RBAC.role_pairs(role))
        for name, role in security_manager.roles.items()
    }


class RoleProvisioningTests(unittest.TestCase):
    def test_configure_provisions_expected_roles_without_granting_viewer_access(self):
        security_manager = FakeSecurityManager()
        result = CONFIGURE_RBAC.configure(security_manager)

        viewer_pairs = CONFIGURE_RBAC.role_pairs(security_manager.find_role("Viewer"))
        scientist_pairs = CONFIGURE_RBAC.role_pairs(
            security_manager.find_role("Scientist")
        )
        operator_pairs = CONFIGURE_RBAC.role_pairs(
            security_manager.find_role("Operator")
        )
        admin_pairs = CONFIGURE_RBAC.role_pairs(security_manager.find_role("Admin"))

        self.assertFalse(
            any(str(resource).startswith("COSIflow ") for _, resource in viewer_pairs)
        )
        self.assertEqual(
            {
                pair
                for pair in scientist_pairs
                if str(pair[1]).startswith("COSIflow ")
            },
            set(SHARED_AUTH.SCIENTIST_PERMISSIONS),
        )
        self.assertEqual(
            {
                pair
                for pair in operator_pairs
                if str(pair[1]).startswith("COSIflow ")
            },
            set(SHARED_AUTH.OPERATOR_PERMISSIONS),
        )
        self.assertTrue(set(SHARED_AUTH.PERMISSION_MANIFEST) <= admin_pairs)
        self.assertEqual(result["viewer_cosiflow_permissions"], [])

    def test_configure_is_idempotent(self):
        security_manager = FakeSecurityManager()
        first_result = CONFIGURE_RBAC.configure(security_manager)
        first_snapshot = role_snapshot(security_manager)

        second_result = CONFIGURE_RBAC.configure(security_manager)
        self.assertEqual(role_snapshot(security_manager), first_snapshot)
        self.assertEqual(second_result, first_result)

    def test_reconciliation_removes_stale_managed_access_but_preserves_unrelated_access(self):
        security_manager = FakeSecurityManager()
        stale_custom = security_manager.create_permission(
            SHARED_AUTH.ACTION_EDIT, "COSIflow Deprecated Resource"
        )
        wrong_menu = security_manager.create_permission(
            SHARED_AUTH.ACTION_MENU_ACCESS, "Mailhog"
        )
        unrelated = security_manager.create_permission("can_read", "Other Plugin")
        security_manager.roles["Scientist"] = Role(
            "Scientist", [stale_custom, wrong_menu, unrelated]
        )

        CONFIGURE_RBAC.configure(security_manager)
        pairs = CONFIGURE_RBAC.role_pairs(security_manager.find_role("Scientist"))
        self.assertNotIn(
            (SHARED_AUTH.ACTION_EDIT, "COSIflow Deprecated Resource"), pairs
        )
        self.assertNotIn((SHARED_AUTH.ACTION_MENU_ACCESS, "Mailhog"), pairs)
        self.assertIn(("can_read", "Other Plugin"), pairs)

    def test_verify_rejects_custom_permissions_on_viewer(self):
        security_manager = FakeSecurityManager()
        CONFIGURE_RBAC.configure(security_manager)
        forbidden = security_manager.create_permission(
            SHARED_AUTH.ACTION_READ, SHARED_AUTH.GCN_NOTICES
        )
        security_manager.find_role("Viewer").permissions.append(forbidden)

        with self.assertRaisesRegex(RuntimeError, "Viewer has"):
            CONFIGURE_RBAC.configure(security_manager, verify_only=True)

    def test_verify_rejects_admin_missing_a_cosiflow_permission(self):
        security_manager = FakeSecurityManager()
        CONFIGURE_RBAC.configure(security_manager)
        admin = security_manager.find_role("Admin")
        removed_pair = next(iter(SHARED_AUTH.PERMISSION_MANIFEST))
        removed_permission = security_manager.get_permission(*removed_pair)
        security_manager.remove_permission_from_role(admin, removed_permission)

        with self.assertRaisesRegex(RuntimeError, "Admin is missing"):
            CONFIGURE_RBAC.configure(security_manager, verify_only=True)

    def test_missing_required_base_role_fails(self):
        security_manager = FakeSecurityManager()
        del security_manager.roles["Op"]
        with self.assertRaisesRegex(RuntimeError, "Required Airflow base role"):
            CONFIGURE_RBAC.configure(security_manager)


if __name__ == "__main__":
    unittest.main()
