import os
import traceback
import json
from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app, jsonify
from flask_login import login_required
from flask_appbuilder import BaseView, expose
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable, DagModel
from airflow.utils.session import provide_session

# Define the absolute path to the plugin folder
plugin_folder = os.path.dirname(os.path.abspath(__file__))

# Blueprint to register templates and route
reset_cosidag_bp = Blueprint(
    "reset_cosidag_bp",
    __name__,
    template_folder=os.path.join(plugin_folder, "templates"),
    url_prefix="/reset_cosidag"
)

@provide_session
def get_dag_ids(session=None):
    dags = session.query(DagModel.dag_id).filter(DagModel.is_active == True).all()
    return sorted([d.dag_id for d in dags])

class ResetCosidagView(BaseView):
    default_view = "reset_cosidag"
    route_base = "/reset_cosidag"

    @expose("/", methods=['GET', 'POST'])
    @login_required
    def reset_cosidag(self):
        try:
            if request.method == 'POST':
                dag_id = request.form.get('dag_id')
                if dag_id:
                    variable_key = f"COSIDAG_PROCESSED::{dag_id}"
                    try:
                        # Reset variable to empty list
                        Variable.set(variable_key, [], serialize_json=True)
                        flash(f"Successfully reset processed folders for {dag_id}. Variable {variable_key} set to [].", "success")
                    except Exception as e:
                        flash(f"Error resetting variable: {str(e)}", "error")
                else:
                    flash("No DAG ID selected.", "error")
                return redirect(self.get_url_for('reset_cosidag'))
            
            dag_ids = get_dag_ids()
            
            # Using self.render_template automatically uses the correct appbuilder layout
            return self.render_template("reset_cosidag.html", dag_ids=dag_ids)
        except Exception as e:
            return f"<h1>Error in Reset Cosidag Plugin</h1><pre>{traceback.format_exc()}</pre>", 500

    @expose("/get_processed_folders/<dag_id>", methods=['GET'])
    @login_required
    def get_processed_folders(self, dag_id):
        try:
            variable_key = f"COSIDAG_PROCESSED::{dag_id}"
            # Get the variable, default to empty list string if not found
            val_str = Variable.get(variable_key, default_var="[]")
            try:
                val = json.loads(val_str)
                if not isinstance(val, list):
                    val = []
            except json.JSONDecodeError:
                val = []
            
            return jsonify({"folders": val})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

# Single Plugin Class
class ResetCosidagPlugin(AirflowPlugin):
    name = "reset_cosidag_plugin"
    flask_blueprints = [reset_cosidag_bp] # Register blueprint for templates path
    appbuilder_views = [
        {
            "name": "Reset Cosidag",
            "category": "Develop tools",
            "view": ResetCosidagView()
        }
    ]
