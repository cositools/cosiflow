import os
import json
import logging
from flask import Blueprint, request, flash, redirect, url_for, jsonify
from flask_appbuilder import BaseView, expose
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable, DagModel
from airflow.utils.session import provide_session
from shared_auth import (
    ACTION_EDIT,
    ACTION_READ,
    COSIDAG_STATE,
    current_airflow_username,
    is_cosiflow_authorized,
    require_cosiflow_permission,
)
from shared_ui import add_shared_templates

# Define the absolute path to the plugin folder
plugin_folder = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)

# Blueprint to register templates and route
reset_cosidag_bp = add_shared_templates(
    Blueprint(
        "reset_cosidag_bp",
        __name__,
        template_folder=os.path.join(plugin_folder, "templates"),
        url_prefix="/reset_cosidag"
    )
)

@provide_session
def get_dag_ids(session=None):
    dags = session.query(DagModel.dag_id).filter(DagModel.is_active == True).all()
    return sorted([d.dag_id for d in dags])


@provide_session
def is_active_dag(dag_id, session=None):
    return (
        session.query(DagModel.dag_id)
        .filter(DagModel.dag_id == dag_id, DagModel.is_active == True)
        .first()
        is not None
    )

class ResetCosidagView(BaseView):
    default_view = "reset_cosidag"
    route_base = "/reset_cosidag"

    @expose("/", methods=["GET"])
    @require_cosiflow_permission(ACTION_READ, COSIDAG_STATE)
    def reset_cosidag(self):
        try:
            dag_ids = get_dag_ids()
            return self.render_template(
                "reset_cosidag.html",
                dag_ids=dag_ids,
                can_edit_state=is_cosiflow_authorized(ACTION_EDIT, COSIDAG_STATE),
            )
        except Exception:
            logger.exception("cosiflow_cosidag_state_read_failed")
            return "Unable to load COSIDAG state.", 500

    @expose("/reset", methods=["POST"])
    @require_cosiflow_permission(ACTION_EDIT, COSIDAG_STATE)
    def reset_all_processed_paths(self):
        dag_id = request.form.get("dag_id", "").strip()
        if not dag_id or not is_active_dag(dag_id):
            return jsonify({"error": "Unknown or inactive DAG."}), 404

        try:
            Variable.set(f"COSIDAG_PROCESSED::{dag_id}", [], serialize_json=True)
            logger.info(
                "cosiflow_mutation user=%s action=%s resource=%s dag_id=%s mutation=reset_all result=success",
                current_airflow_username(),
                ACTION_EDIT,
                COSIDAG_STATE,
                dag_id,
            )
            flash(f"Successfully reset processed paths for {dag_id}.", "success")
        except Exception:
            logger.exception(
                "cosiflow_mutation user=%s action=%s resource=%s dag_id=%s mutation=reset_all result=failure",
                current_airflow_username(),
                ACTION_EDIT,
                COSIDAG_STATE,
                dag_id,
            )
            flash("Unable to reset processed paths.", "error")
        return redirect(url_for("ResetCosidagView.reset_cosidag"), code=303)

    @expose("/get_processed_folders/<dag_id>", methods=['GET'])
    @require_cosiflow_permission(ACTION_READ, COSIDAG_STATE)
    def get_processed_folders(self, dag_id):
        if not is_active_dag(dag_id):
            return jsonify({"error": "Unknown or inactive DAG."}), 404
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
        except Exception:
            logger.exception("cosiflow_cosidag_state_read_failed dag_id=%s", dag_id)
            return jsonify({"error": "Unable to read processed paths."}), 500

    @expose("/delete_processed_paths/<dag_id>", methods=['POST'])
    @require_cosiflow_permission(ACTION_EDIT, COSIDAG_STATE)
    def delete_processed_paths(self, dag_id):
        if not is_active_dag(dag_id):
            return jsonify({"error": "Unknown or inactive DAG."}), 404
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

            logger.info(
                "cosiflow_mutation user=%s action=%s resource=%s dag_id=%s mutation=delete_paths removed_count=%s result=success",
                current_airflow_username(),
                ACTION_EDIT,
                COSIDAG_STATE,
                dag_id,
                removed_count,
            )

            return jsonify({
                "success": True,
                "removed_count": removed_count,
                "paths": remaining_paths,
                "folders": remaining_paths,
            })
        except Exception:
            logger.exception(
                "cosiflow_mutation user=%s action=%s resource=%s dag_id=%s mutation=delete_paths result=failure",
                current_airflow_username(),
                ACTION_EDIT,
                COSIDAG_STATE,
                dag_id,
            )
            return jsonify({"error": "Unable to delete processed paths."}), 500

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
