from airflow.plugins_manager import AirflowPlugin
from flask import redirect
from flask_appbuilder import BaseView, expose
import os
from shared_auth import ACTION_READ, MAIL_SANDBOX, require_cosiflow_permission

class MailhogView(BaseView):
    default_view = "redirect_to_mailhog"
    route_base = "/mailhog"

    @expose("/")
    @require_cosiflow_permission(ACTION_READ, MAIL_SANDBOX)
    def redirect_to_mailhog(self):
        mail_server = os.environ.get('MAILHOG_WEBUI_URL', 'http://localhost:8025')
        return redirect(mail_server)

class MailhogViewPlugin(AirflowPlugin):
    name = "mailhog_view_plugin"
    appbuilder_views = [
        {
            "name": "Mailhog",
            "category": "Develop Tools",
            "view": MailhogView()
        }
    ]
