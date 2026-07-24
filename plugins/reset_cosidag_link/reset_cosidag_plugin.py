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
                        flash(f"Successfully reset processed paths for {dag_id}. Variable {variable_key} set to [].", "success")
                    except Exception as e:
                        flash(f"Error resetting variable: {str(e)}", "error")
                else:
                    flash("No DAG ID selected.", "error")
                return redirect(url_for('ResetCosidagView.reset_cosidag'))
            
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
            
            return jsonify({"folders": val, "paths": val})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @expose("/delete_processed_paths/<dag_id>", methods=['POST'])
    @login_required
    def delete_processed_paths(self, dag_id):
        try:
            payload = request.get_json(silent=True) or {}
            selected_paths = payload.get("paths")
            if selected_paths is None:
                selected_paths = request.form.getlist("paths")

            if not isinstance(selected_paths, list):
                return jsonify({"error": "Invalid paths payload."}), 400

            selected_set = {str(path) for path in selected_paths}
            if not selected_set:
                return jsonify({"error": "No processed paths selected."}), 400

            variable_key = f"COSIDAG_PROCESSED::{dag_id}"
            val_str = Variable.get(variable_key, default_var="[]")
            try:
                current_paths = json.loads(val_str)
                if not isinstance(current_paths, list):
                    current_paths = []
            except json.JSONDecodeError:
                current_paths = []

            remaining_paths = [path for path in current_paths if str(path) not in selected_set]
            removed_count = len(current_paths) - len(remaining_paths)
            Variable.set(variable_key, remaining_paths, serialize_json=True)

            return jsonify({
                "success": True,
                "removed_count": removed_count,
                "paths": remaining_paths,
                "folders": remaining_paths,
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

# Single Plugin Class
class ResetCosidagPlugin(AirflowPlugin):
    name = "reset_cosidag_plugin"
    flask_blueprints = [reset_cosidag_bp] # Register blueprint for templates path
    appbuilder_views = [
        {
            "name": "Reset Cosidag",
            "category": "Develop Tools",
            "view": ResetCosidagView()
        }
    ]
