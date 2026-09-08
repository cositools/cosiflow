#!/usr/bin/env python
"""Provision and verify COSIflow FAB permissions without direct SQL writes."""

from __future__ import annotations

import argparse
import json
import os
import sys


PLUGIN_DIR = os.environ.get(
    "AIRFLOW__CORE__PLUGINS_FOLDER",
    os.path.join(os.environ.get("AIRFLOW_HOME", "/home/gamma/airflow"), "plugins"),
)
if PLUGIN_DIR not in sys.path:
    sys.path.insert(0, PLUGIN_DIR)

from shared_auth import (  # noqa: E402
    ACTION_MENU_ACCESS,
    MENU_MANIFEST,
    OPERATOR_PERMISSIONS,
    PERMISSION_MANIFEST,
    SCIENTIST_PERMISSIONS,
)


MANAGED_ROLES = {
    "Scientist": ("Viewer", SCIENTIST_PERMISSIONS),
    "Operator": ("Op", OPERATOR_PERMISSIONS),
}
MANAGED_MENUS = frozenset().union(*MENU_MANIFEST.values())


def permission_pair(permission):
    """Support both FAB legacy and current permission model attribute names."""
    action = getattr(permission, "action", None) or getattr(permission, "permission", None)
    resource = getattr(permission, "resource", None) or getattr(permission, "view_menu", None)
    return getattr(action, "name", None), getattr(resource, "name", None)


def role_pairs(role):
    return {permission_pair(permission) for permission in role.permissions}


def ensure_permission(security_manager, action, resource):
    return security_manager.create_permission(action, resource)


def find_permission(security_manager, action, resource):
    return security_manager.get_permission(action, resource)


def add_pair(security_manager, role, pair):
    permission = ensure_permission(security_manager, *pair)
    if pair not in role_pairs(role):
        security_manager.add_permission_to_role(role, permission)


def remove_pair(security_manager, role, pair):
    permission = find_permission(security_manager, *pair)
    if permission is not None and pair in role_pairs(role):
        security_manager.remove_permission_from_role(role, permission)


def reconcile_role(security_manager, role_name, base_name, cosiflow_pairs):
    role = security_manager.find_role(role_name) or security_manager.add_role(role_name)
    base_role = security_manager.find_role(base_name)
    if base_role is None:
        raise RuntimeError(f"Required Airflow base role is missing: {base_name}")

    for pair in role_pairs(base_role) | set(cosiflow_pairs):
        add_pair(security_manager, role, pair)

    expected_menus = MENU_MANIFEST[role_name]
    for menu_name in MANAGED_MENUS:
        pair = (ACTION_MENU_ACCESS, menu_name)
        if menu_name in expected_menus:
            add_pair(security_manager, role, pair)
        else:
            remove_pair(security_manager, role, pair)

    expected_cosiflow = set(cosiflow_pairs)
    for pair in tuple(role_pairs(role)):
        if str(pair[1]).startswith("COSIflow ") and pair not in expected_cosiflow:
            remove_pair(security_manager, role, pair)


def verify(security_manager):
    errors = []
    viewer = security_manager.find_role("Viewer")
    admin = security_manager.find_role("Admin")
    if viewer is None:
        errors.append("Viewer role is missing")
    elif any(str(resource).startswith("COSIflow ") for _, resource in role_pairs(viewer)):
        errors.append("Viewer has one or more COSIflow permissions")
    if admin is None:
        errors.append("Admin role is missing; FAB Admin semantics cannot be applied")
    else:
        admin_pairs = role_pairs(admin)
        missing_admin_menus = {
            (ACTION_MENU_ACCESS, menu_name)
            for menu_name in MANAGED_MENUS
        } - admin_pairs
        if missing_admin_menus:
            errors.append(
                f"Admin is missing {len(missing_admin_menus)} managed menu permissions"
            )

    for role_name, (base_name, expected_cosiflow) in MANAGED_ROLES.items():
        role = security_manager.find_role(role_name)
        base_role = security_manager.find_role(base_name)
        if role is None:
            errors.append(f"{role_name} role is missing")
            continue
        if base_role is None:
            errors.append(f"{base_name} base role is missing")
            continue

        actual = role_pairs(role)
        missing_base = role_pairs(base_role) - actual
        if missing_base:
            errors.append(f"{role_name} is missing {len(missing_base)} base permissions")

        actual_cosiflow = {
            pair for pair in actual if str(pair[1]).startswith("COSIflow ")
        }
        if actual_cosiflow != set(expected_cosiflow):
            errors.append(
                f"{role_name} COSIflow permissions differ: "
                f"expected={sorted(expected_cosiflow)!r} actual={sorted(actual_cosiflow)!r}"
            )

        actual_managed_menus = {
            resource
            for action, resource in actual
            if action == ACTION_MENU_ACCESS and resource in MANAGED_MENUS
        }
        if actual_managed_menus != set(MENU_MANIFEST[role_name]):
            errors.append(
                f"{role_name} managed menus differ: "
                f"expected={sorted(MENU_MANIFEST[role_name])!r} "
                f"actual={sorted(actual_managed_menus)!r}"
            )

    if errors:
        raise RuntimeError("RBAC verification failed:\n- " + "\n- ".join(errors))


def configure(security_manager, verify_only=False):
    if not verify_only:
        for action, resource in PERMISSION_MANIFEST:
            ensure_permission(security_manager, action, resource)
        for menu_names in MENU_MANIFEST.values():
            for menu_name in menu_names:
                ensure_permission(security_manager, ACTION_MENU_ACCESS, menu_name)
        for role_name, (base_name, permissions) in MANAGED_ROLES.items():
            reconcile_role(security_manager, role_name, base_name, permissions)
    verify(security_manager)
    return {
        "roles": {
            role_name: sorted(role_pairs(security_manager.find_role(role_name)))
            for role_name in MANAGED_ROLES
        },
        "viewer_cosiflow_permissions": [],
    }


def get_security_manager():
    from airflow.www.app import create_app

    app = create_app(testing=False)
    return app, app.appbuilder.sm


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    app, security_manager = get_security_manager()
    with app.app_context():
        result = configure(security_manager, verify_only=args.verify_only)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
