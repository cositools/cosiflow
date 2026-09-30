"""Airflow UI for COSIflow email notification subscriptions."""

from __future__ import annotations

import logging
import os
import sys

from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, flash, redirect, request, url_for
from flask_appbuilder import BaseView, expose

from shared_auth import (
    ACTION_EDIT,
    ACTION_READ,
    NOTIFICATION_SUBSCRIPTIONS,
    current_airflow_username,
    is_cosiflow_authorized,
    require_cosiflow_permission,
)
from shared_ui import add_shared_templates


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
MODULES_PATH = os.path.join(AIRFLOW_HOME, "modules")
if MODULES_PATH not in sys.path:
    sys.path.append(MODULES_PATH)

from notification_subscriptions import (  # type: ignore  # noqa: E402
    SUPPORTED_EVENTS,
    delete_notification_subscription,
    list_notification_subscriptions,
    list_notification_users,
    save_notification_subscription,
    set_notification_subscription_enabled,
    valid_email_address,
)


LOGGER = logging.getLogger(__name__)
PLUGIN_FOLDER = os.path.dirname(os.path.abspath(__file__))
notification_subscriptions_bp = add_shared_templates(
    Blueprint(
        "notification_subscriptions_bp",
        __name__,
        template_folder=os.path.join(PLUGIN_FOLDER, "templates"),
        url_prefix="/notification-subscriptions",
    )
)


class NotificationSubscriptionsView(BaseView):
    default_view = "index"
    route_base = "/notification-subscriptions"

    @expose("/", methods=["GET"])
    @require_cosiflow_permission(ACTION_READ, NOTIFICATION_SUBSCRIPTIONS)
    def index(self):
        try:
            users = list_notification_users()
            for user in users:
                user["selectable"] = bool(user.get("active")) and valid_email_address(
                    user.get("email")
                )
            return self.render_template(
                "notification_subscriptions.html",
                users=users,
                subscriptions=list_notification_subscriptions(),
                supported_events=SUPPORTED_EVENTS,
                can_edit=is_cosiflow_authorized(
                    ACTION_EDIT, NOTIFICATION_SUBSCRIPTIONS
                ),
            )
        except Exception:
            LOGGER.exception("cosiflow_notification_subscription_read_failed")
            return "Unable to load notification subscriptions.", 500

    @expose("/save", methods=["POST"])
    @require_cosiflow_permission(ACTION_EDIT, NOTIFICATION_SUBSCRIPTIONS)
    def save(self):
        username = current_airflow_username()
        payload = {
            "user_id": request.form.get("user_id"),
            "event_type": request.form.get("event_type"),
            "dag_pattern": request.form.get("dag_pattern", "*"),
            "task_pattern": request.form.get("task_pattern", "*"),
            "operator_pattern": request.form.get("operator_pattern", "*"),
            "enabled": request.form.get("enabled") == "on",
        }
        try:
            subscription_id = save_notification_subscription(payload, username)
            LOGGER.info(
                "cosiflow_mutation user=%s action=%s resource=%s mutation=save_subscription subscription_id=%s result=success",
                username,
                ACTION_EDIT,
                NOTIFICATION_SUBSCRIPTIONS,
                subscription_id,
            )
            flash("Notification subscription saved.", "success")
        except ValueError as exc:
            flash(str(exc), "error")
        except Exception:
            LOGGER.exception(
                "cosiflow_mutation user=%s action=%s resource=%s mutation=save_subscription result=failure",
                username,
                ACTION_EDIT,
                NOTIFICATION_SUBSCRIPTIONS,
            )
            flash("Unable to save the notification subscription.", "error")
        return redirect(url_for("NotificationSubscriptionsView.index"), code=303)

    @expose("/toggle/<int:subscription_id>", methods=["POST"])
    @require_cosiflow_permission(ACTION_EDIT, NOTIFICATION_SUBSCRIPTIONS)
    def toggle(self, subscription_id):
        username = current_airflow_username()
        enabled = request.form.get("enabled") == "true"
        try:
            if not set_notification_subscription_enabled(
                subscription_id, enabled, username
            ):
                flash("Notification subscription was not found.", "warning")
            else:
                LOGGER.info(
                    "cosiflow_mutation user=%s action=%s resource=%s mutation=toggle_subscription subscription_id=%s enabled=%s result=success",
                    username,
                    ACTION_EDIT,
                    NOTIFICATION_SUBSCRIPTIONS,
                    subscription_id,
                    enabled,
                )
                flash("Notification subscription updated.", "success")
        except Exception:
            LOGGER.exception(
                "cosiflow_mutation user=%s action=%s resource=%s mutation=toggle_subscription subscription_id=%s result=failure",
                username,
                ACTION_EDIT,
                NOTIFICATION_SUBSCRIPTIONS,
                subscription_id,
            )
            flash("Unable to update the notification subscription.", "error")
        return redirect(url_for("NotificationSubscriptionsView.index"), code=303)

    @expose("/delete/<int:subscription_id>", methods=["POST"])
    @require_cosiflow_permission(ACTION_EDIT, NOTIFICATION_SUBSCRIPTIONS)
    def delete(self, subscription_id):
        username = current_airflow_username()
        try:
            if not delete_notification_subscription(subscription_id):
                flash("Notification subscription was not found.", "warning")
            else:
                LOGGER.info(
                    "cosiflow_mutation user=%s action=%s resource=%s mutation=delete_subscription subscription_id=%s result=success",
                    username,
                    ACTION_EDIT,
                    NOTIFICATION_SUBSCRIPTIONS,
                    subscription_id,
                )
                flash("Notification subscription deleted.", "success")
        except Exception:
            LOGGER.exception(
                "cosiflow_mutation user=%s action=%s resource=%s mutation=delete_subscription subscription_id=%s result=failure",
                username,
                ACTION_EDIT,
                NOTIFICATION_SUBSCRIPTIONS,
                subscription_id,
            )
            flash("Unable to delete the notification subscription.", "error")
        return redirect(url_for("NotificationSubscriptionsView.index"), code=303)


class NotificationSubscriptionsPlugin(AirflowPlugin):
    name = "notification_subscriptions_plugin"
    flask_blueprints = [notification_subscriptions_bp]
    appbuilder_views = [
        {
            "name": "Notification Subscriptions",
            "category": "Develop Tools",
            "view": NotificationSubscriptionsView(),
        }
    ]
