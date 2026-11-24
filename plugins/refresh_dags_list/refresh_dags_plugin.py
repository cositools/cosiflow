import os
import logging
import subprocess
from flask import Blueprint, redirect, flash, url_for
from flask_appbuilder import BaseView, expose
from airflow.plugins_manager import AirflowPlugin
from airflow.models import DagBag
from airflow.utils.session import provide_session
from shared_auth import (
    ACTION_EDIT,
    DAG_CATALOG,
    current_airflow_username,
    require_cosiflow_permission,
)
from shared_ui import add_shared_templates


plugin_folder = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)
refresh_dags_bp = add_shared_templates(
    Blueprint(
        "refresh_dags_bp",
        __name__,
        template_folder=os.path.join(plugin_folder, "templates"),
    )
)


class RefreshDagsView(BaseView):
    """View that executes 'airflow dags list' command and refreshes the DAG bag"""
    
    default_view = "confirm_refresh"
    route_base = "/refresh_dags"

    @expose("/confirm", methods=["GET"])
    @require_cosiflow_permission(ACTION_EDIT, DAG_CATALOG)
    def confirm_refresh(self):
        return self.render_template("refresh_dags.html")

    @expose("/", methods=["POST"])
    @require_cosiflow_permission(ACTION_EDIT, DAG_CATALOG)
    def refresh_dags(self):
        """Execute ``airflow dags list`` and refresh the DAG bag."""
        try:
            result = self._execute_dags_list()
            dagbag_refreshed = self._refresh_dagbag()
            success = result.get("success", False) and dagbag_refreshed
            logger.info(
                "cosiflow_mutation user=%s action=%s resource=%s mutation=refresh_dags result=%s command_returncode=%s",
                current_airflow_username(),
                ACTION_EDIT,
                DAG_CATALOG,
                "success" if success else "failure",
                result.get("returncode"),
            )
            if success:
                flash("DAGs list refreshed successfully!", "success")
            else:
                flash("DAG refresh completed with warnings. Check the Airflow logs.", "warning")
        except Exception:
            logger.exception(
                "cosiflow_mutation user=%s action=%s resource=%s mutation=refresh_dags result=failure",
                current_airflow_username(),
                ACTION_EDIT,
                DAG_CATALOG,
            )
            flash("Unable to refresh the DAG catalog.", "error")
        return redirect(url_for("RefreshDagsView.confirm_refresh"), code=303)

    def _execute_dags_list(self):
        """Execute 'airflow dags list' command and return the output"""
        try:
            # Get AIRFLOW_HOME from environment
            airflow_home = os.environ.get('AIRFLOW_HOME', '/home/gamma/airflow')
            
            # Execute the command
            result = subprocess.run(
                ['airflow', 'dags', 'list'],
                capture_output=True,
                text=True,
                timeout=30,
                env={**os.environ, 'AIRFLOW_HOME': airflow_home}
            )
            
            return {
                'success': result.returncode == 0,
                'returncode': result.returncode,
            }
        except subprocess.TimeoutExpired:
            return {
                'success': False,
                'returncode': None,
            }
        except Exception:
            logger.exception("cosiflow_dag_list_command_failed")
            return {
                'success': False,
                'returncode': None,
            }

    @provide_session
    def _refresh_dagbag(self, session=None):
        """Force refresh of the DAG bag to update the list in the UI"""
        try:
            # Get the DAGs folder path
            dag_folder = os.environ.get(
                'AIRFLOW__CORE__DAGS_FOLDER',
                os.path.join(os.environ.get('AIRFLOW_HOME', '/home/gamma/airflow'), 'dags')
            )
            
            # Create a new DagBag instance to force refresh
            dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
            
            # Force parsing of DAGs
            dagbag.collect_dags()
            
            # Sync with database
            dagbag.sync_to_db()
            
            return True
        except Exception:
            logger.exception("cosiflow_dagbag_refresh_failed")
            return False


class RefreshDagsPlugin(AirflowPlugin):
    name = "refresh_dags_plugin"
    flask_blueprints = [refresh_dags_bp]
    appbuilder_views = [
        {
            "name": "Refresh DAGs List",
            "category": "Develop Tools",
            "view": RefreshDagsView()
        }
    ]
