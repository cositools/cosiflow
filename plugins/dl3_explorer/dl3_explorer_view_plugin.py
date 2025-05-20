from airflow.plugins_manager import AirflowPlugin
from flask import redirect
from flask_appbuilder import BaseView, expose

class DL3ExplorerView(BaseView):
    default_view = "redirect_to_dl3"

    @expose("/")
    def redirect_to_dl3(self):
        return redirect("/dl3browser/")

class DL3ExplorerViewPlugin(AirflowPlugin):
    name = "dl3_explorer_view_plugin"
    appbuilder_views = [
        {
            "name": "DL3 Browser",
            "category": "Results Browser",
            "view": DL3ExplorerView()
        }
    ]
