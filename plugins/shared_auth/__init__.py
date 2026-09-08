"""Shared COSIflow authorization policy for Airflow plugin views."""

from __future__ import annotations

import logging
from functools import wraps

from flask import abort, redirect, request

from airflow.www.extensions.init_auth_manager import get_auth_manager


ACTION_READ = "can_read"
ACTION_CREATE = "can_create"
ACTION_EDIT = "can_edit"
ACTION_MENU_ACCESS = "menu_access"

GCN_NOTICES = "COSIflow GCN Notices"
GCN_INBOX = "COSIflow GCN Inbox"
GCN_OUTBOX = "COSIflow GCN Outbox"
SCIENTIFIC_DATA = "COSIflow Scientific Data"
COSIDAG_STATE = "COSIflow COSIDAG State"
DAG_CATALOG = "COSIflow DAG Catalog"
MAIL_SANDBOX = "COSIflow Mail Sandbox"

PERMISSION_MANIFEST = (
    (ACTION_READ, GCN_NOTICES),
    (ACTION_CREATE, GCN_INBOX),
    (ACTION_CREATE, GCN_OUTBOX),
    (ACTION_READ, SCIENTIFIC_DATA),
    (ACTION_READ, COSIDAG_STATE),
    (ACTION_EDIT, COSIDAG_STATE),
    (ACTION_EDIT, DAG_CATALOG),
    (ACTION_READ, MAIL_SANDBOX),
)

SCIENTIST_PERMISSIONS = frozenset(
    {
        (ACTION_READ, GCN_NOTICES),
        (ACTION_READ, SCIENTIFIC_DATA),
        (ACTION_READ, COSIDAG_STATE),
    }
)
OPERATOR_PERMISSIONS = frozenset(PERMISSION_MANIFEST)

MENU_MANIFEST = {
    "Scientist": frozenset({"GCN Notices Explorer", "HEASARC Explorer"}),
    "Operator": frozenset(
        {
            "GCN Notices Explorer",
            "HEASARC Explorer",
            "Reset Cosidag",
            "Refresh DAGs List",
            "Develop Tools",
            "Mailhog",
        }
    ),
}

# This is also the explicit allowlist checked by the route-coverage tests.
ENDPOINT_POLICIES = {
    ("ExploreNoticesView.index", "GET"): (ACTION_READ, GCN_NOTICES),
    ("ExploreNoticesView.notice_detail", "GET"): (ACTION_READ, GCN_NOTICES),
    ("ExploreNoticesView.outbox_detail", "GET"): (ACTION_READ, GCN_NOTICES),
    ("ExploreNoticesView.inject_inbox", "POST"): (ACTION_CREATE, GCN_INBOX),
    ("ExploreNoticesView.inject_outbox", "POST"): (ACTION_CREATE, GCN_OUTBOX),
    ("HEASARCExplorerView.explorer_home", "GET"): (ACTION_READ, SCIENTIFIC_DATA),
    ("HEASARCExplorerView.explorer_folder", "GET"): (ACTION_READ, SCIENTIFIC_DATA),
    ("HEASARCExplorerView.download_file", "GET"): (ACTION_READ, SCIENTIFIC_DATA),
    ("HEASARCExplorerView.image_file", "GET"): (ACTION_READ, SCIENTIFIC_DATA),
    ("HEASARCExplorerView.preview_file", "GET"): (ACTION_READ, SCIENTIFIC_DATA),
    ("ResetCosidagView.reset_cosidag", "GET"): (ACTION_READ, COSIDAG_STATE),
    ("ResetCosidagView.reset_all_processed_paths", "POST"): (ACTION_EDIT, COSIDAG_STATE),
    ("ResetCosidagView.get_processed_folders", "GET"): (ACTION_READ, COSIDAG_STATE),
    ("ResetCosidagView.delete_processed_paths", "POST"): (ACTION_EDIT, COSIDAG_STATE),
    ("RefreshDagsView.refresh_dags", "POST"): (ACTION_EDIT, DAG_CATALOG),
    ("RefreshDagsView.confirm_refresh", "GET"): (ACTION_EDIT, DAG_CATALOG),
    ("MailhogView.redirect_to_mailhog", "GET"): (ACTION_READ, MAIL_SANDBOX),
}

logger = logging.getLogger(__name__)


def _authorization_context():
    """Return the initialized auth manager and current user, or deny closed."""
    auth_manager = get_auth_manager()
    if not auth_manager.is_logged_in():
        return auth_manager, None
    user = auth_manager.get_user()
    if user is None:
        raise RuntimeError("Authenticated session has no Airflow user")
    return auth_manager, user


def is_cosiflow_authorized(action: str, resource: str) -> bool:
    """Check one explicit plugin capability and fail closed on any error."""
    try:
        auth_manager, user = _authorization_context()
        if user is None:
            return False
        return bool(
            auth_manager.is_authorized_custom_view(
                method=action,
                resource_name=resource,
                user=user,
            )
        )
    except Exception:
        logger.exception(
            "cosiflow_authorization_check_failed action=%s resource=%s",
            action,
            resource,
        )
        return False


def cosiflow_capabilities(*permissions):
    """Return template capability flags keyed by ``(action, resource)``."""
    return {
        (action, resource): is_cosiflow_authorized(action, resource)
        for action, resource in permissions
    }


def current_airflow_username() -> str:
    """Return a non-sensitive audit identity without weakening authorization."""
    try:
        return get_auth_manager().get_user_name()
    except Exception:
        return "unknown"


def require_cosiflow_permission(action: str, resource: str):
    """Require an Airflow Auth Manager custom-view permission.

    Anonymous users follow the active Auth Manager login flow. Authenticated
    users without the permission, and all authorization errors, receive 403.
    The wrapped route is never evaluated before authorization succeeds.
    """

    if (action, resource) not in PERMISSION_MANIFEST:
        raise ValueError(f"Unknown COSIflow permission: {action}/{resource}")

    def decorator(view_function):
        @wraps(view_function)
        def decorated_view(*args, **kwargs):
            try:
                auth_manager, user = _authorization_context()
                if user is None:
                    return redirect(
                        auth_manager.get_url_login(next=request.full_path.rstrip("?"))
                    )
                allowed = auth_manager.is_authorized_custom_view(
                    method=action,
                    resource_name=resource,
                    user=user,
                )
            except Exception:
                logger.exception(
                    "cosiflow_authorization_check_failed action=%s resource=%s",
                    action,
                    resource,
                )
                abort(403)

            if not allowed:
                logger.warning(
                    "cosiflow_authorization_denied action=%s resource=%s user=%s",
                    action,
                    resource,
                    current_airflow_username(),
                )
                abort(403)
            return view_function(*args, **kwargs)

        decorated_view._cosiflow_permission = (action, resource)
        return decorated_view

    return decorator


__all__ = [
    "ACTION_CREATE",
    "ACTION_EDIT",
    "ACTION_MENU_ACCESS",
    "ACTION_READ",
    "COSIDAG_STATE",
    "DAG_CATALOG",
    "ENDPOINT_POLICIES",
    "GCN_INBOX",
    "GCN_NOTICES",
    "GCN_OUTBOX",
    "MAIL_SANDBOX",
    "MENU_MANIFEST",
    "OPERATOR_PERMISSIONS",
    "PERMISSION_MANIFEST",
    "SCIENTIFIC_DATA",
    "SCIENTIST_PERMISSIONS",
    "cosiflow_capabilities",
    "current_airflow_username",
    "is_cosiflow_authorized",
    "require_cosiflow_permission",
]
