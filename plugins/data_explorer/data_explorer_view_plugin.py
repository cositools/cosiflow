from airflow.plugins_manager import AirflowPlugin
from flask import redirect
from flask_appbuilder import BaseView, expose

class HEASARCExplorerView(BaseView):
    default_view = "redirect_to_heasarc"

    @expose("/")
    def redirect_to_heasarc(self):
        return redirect("/heasarcbrowser/")

class HEASARCExplorerViewPlugin(AirflowPlugin):
    name = "heasarc_explorer_view_plugin"
    appbuilder_views = [
        {
            "name": "heasarc Browser",
            "category": "Results Browser",
            "view": HEASARCExplorerView()
        }
    ]
