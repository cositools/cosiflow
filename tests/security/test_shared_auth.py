from __future__ import annotations

import unittest
from unittest.mock import patch

from .auth_support import AUTH_MANAGER_HOLDER, REQUEST, AbortRaised, SHARED_AUTH


class FakeAuthManager:
    def __init__(self, *, logged_in=True, user="user", allowed=False, failure=None):
        self.logged_in = logged_in
        self.user = user
        self.allowed = allowed
        self.failure = failure
        self.authorization_calls = []

    def is_logged_in(self):
        if self.failure == "login":
            raise RuntimeError("login check failed")
        return self.logged_in

    def get_user(self):
        if self.failure == "user":
            raise RuntimeError("user lookup failed")
        return self.user

    def get_user_name(self):
        return str(self.user)

    def get_url_login(self, next):
        return f"/login?next={next}"

    def is_authorized_custom_view(self, *, method, resource_name, user):
        self.authorization_calls.append((method, resource_name, user))
        if self.failure == "authorization":
            raise RuntimeError("authorization backend failed")
        return self.allowed


class SharedAuthorizationTests(unittest.TestCase):
    def setUp(self):
        REQUEST.full_path = "/protected?"

    def set_manager(self, manager):
        AUTH_MANAGER_HOLDER["manager"] = manager

    def protected_view(self, permission, side_effects):
        @SHARED_AUTH.require_cosiflow_permission(*permission)
        def view():
            side_effects.append("executed")
            return "sensitive result"

        return view

    def test_unknown_permission_cannot_be_decorated(self):
        with self.assertRaises(ValueError):
            SHARED_AUTH.require_cosiflow_permission("can_delete", "unknown")

    def test_anonymous_user_is_redirected_before_view_execution(self):
        manager = FakeAuthManager(logged_in=False)
        self.set_manager(manager)
        side_effects = []
        view = self.protected_view(
            (SHARED_AUTH.ACTION_READ, SHARED_AUTH.GCN_NOTICES), side_effects
        )

        self.assertEqual(view(), ("redirect", "/login?next=/protected"))
        self.assertEqual(side_effects, [])
        self.assertEqual(manager.authorization_calls, [])

    def test_denied_user_receives_403_before_view_execution(self):
        manager = FakeAuthManager(allowed=False)
        self.set_manager(manager)
        side_effects = []
        view = self.protected_view(
            (SHARED_AUTH.ACTION_EDIT, SHARED_AUTH.COSIDAG_STATE), side_effects
        )

        with (
            patch.object(SHARED_AUTH.logger, "warning"),
            self.assertRaises(AbortRaised) as raised,
        ):
            view()
        self.assertEqual(raised.exception.status_code, 403)
        self.assertEqual(side_effects, [])

    def test_authorization_backend_error_fails_closed_without_side_effects(self):
        manager = FakeAuthManager(allowed=True, failure="authorization")
        self.set_manager(manager)
        side_effects = []
        view = self.protected_view(
            (SHARED_AUTH.ACTION_EDIT, SHARED_AUTH.DAG_CATALOG), side_effects
        )

        with (
            patch.object(SHARED_AUTH.logger, "exception"),
            self.assertRaises(AbortRaised) as raised,
        ):
            view()
        self.assertEqual(raised.exception.status_code, 403)
        self.assertEqual(side_effects, [])

    def test_authenticated_session_without_user_fails_closed(self):
        manager = FakeAuthManager(user=None, allowed=True)
        self.set_manager(manager)
        side_effects = []
        view = self.protected_view(
            (SHARED_AUTH.ACTION_READ, SHARED_AUTH.SCIENTIFIC_DATA), side_effects
        )

        with (
            patch.object(SHARED_AUTH.logger, "exception"),
            self.assertRaises(AbortRaised) as raised,
        ):
            view()
        self.assertEqual(raised.exception.status_code, 403)
        self.assertEqual(side_effects, [])

    def test_authorized_user_executes_view_after_permission_check(self):
        manager = FakeAuthManager(allowed=True, user="arbitrary-fab-user")
        self.set_manager(manager)
        side_effects = []
        permission = (SHARED_AUTH.ACTION_CREATE, SHARED_AUTH.GCN_OUTBOX)
        view = self.protected_view(permission, side_effects)

        self.assertEqual(view(), "sensitive result")
        self.assertEqual(side_effects, ["executed"])
        self.assertEqual(
            manager.authorization_calls,
            [(permission[0], permission[1], "arbitrary-fab-user")],
        )

    def test_capability_check_is_false_for_anonymous_denied_and_errors(self):
        permission = (SHARED_AUTH.ACTION_READ, SHARED_AUTH.GCN_NOTICES)
        with patch.object(SHARED_AUTH.logger, "exception"):
            for manager in (
                FakeAuthManager(logged_in=False),
                FakeAuthManager(allowed=False),
                FakeAuthManager(allowed=True, failure="authorization"),
            ):
                with self.subTest(manager=vars(manager)):
                    self.set_manager(manager)
                    self.assertFalse(SHARED_AUTH.is_cosiflow_authorized(*permission))

    def test_viewer_scientist_operator_and_admin_permission_matrix(self):
        role_permissions = {
            "Viewer": frozenset(),
            "Scientist": SHARED_AUTH.SCIENTIST_PERMISSIONS,
            "Operator": SHARED_AUTH.OPERATOR_PERMISSIONS,
            "Admin": frozenset(SHARED_AUTH.PERMISSION_MANIFEST),
        }
        for role_name, granted_permissions in role_permissions.items():
            for permission in SHARED_AUTH.PERMISSION_MANIFEST:
                with self.subTest(role=role_name, permission=permission):
                    manager = FakeAuthManager(
                        user=role_name,
                        allowed=permission in granted_permissions,
                    )
                    self.set_manager(manager)
                    side_effects = []
                    view = self.protected_view(permission, side_effects)
                    if permission in granted_permissions:
                        self.assertEqual(view(), "sensitive result")
                        self.assertEqual(side_effects, ["executed"])
                    else:
                        with (
                            patch.object(SHARED_AUTH.logger, "warning"),
                            self.assertRaises(AbortRaised),
                        ):
                            view()
                        self.assertEqual(side_effects, [])


if __name__ == "__main__":
    unittest.main()
