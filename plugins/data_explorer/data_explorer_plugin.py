import os
import traceback
from pathlib import Path
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from flask import Blueprint, render_template, send_from_directory, redirect, url_for, session
from flask_login import login_required, current_user

# Get from the env variable COSI_DATA_DIR the path to the data directory if it is not set, use the default path
DL0_FOLDER = os.environ.get("COSI_DATA_DIR", "/home/gamma/workspace/data")

# Definiamo il percorso assoluto alla cartella del plugin
plugin_folder = os.path.dirname(os.path.abspath(__file__))

# Blueprint con percorso assoluto a templates e static
heasarc_explorer_bp = Blueprint(
    "heasarc_explorer_bp",
    __name__,
    template_folder=os.path.join(plugin_folder, "templates"),
    static_folder=os.path.join(plugin_folder, "static"),
    url_prefix='/heasarcbrowser'
)

@heasarc_explorer_bp.route('/')
def explorer_home():
    if not current_user.is_authenticated:
        return redirect('/login/?next=/heasarcbrowser/')
    try:
        folders = sorted([f for f in os.listdir(DL0_FOLDER) if os.path.isdir(os.path.join(DL0_FOLDER, f))])
        return render_template("explorer.html", folders=folders, current_path=DL0_FOLDER)
    except PermissionError:
        abort(403)
    except Exception as e:
        error_traceback = traceback.format_exc()
        return f"Error loading folders: {e}\n\nTraceback:\n{error_traceback}", 500

@heasarc_explorer_bp.route('/folder/<path:foldername>')
@login_required
def explorer_folder(foldername):
    try:
        folder_path = os.path.join(DL0_FOLDER, foldername)
        if not os.path.commonpath([DL0_FOLDER, folder_path]).startswith(DL0_FOLDER):
            abort(403)

        # Show all files in the folder, not only pdfs
        files   = sorted([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
        folders = sorted([f for f in os.listdir(folder_path) if not os.path.isfile(os.path.join(folder_path, f))])
        return render_template("explorer.html", folders=folders, files=files, foldername=foldername, current_path=folder_path)
    except PermissionError:
        abort(403)
    except Exception as e:
        error_traceback = traceback.format_exc()
        return f"Error loading files: {e}\n\nTraceback:\n{error_traceback}", 500

@heasarc_explorer_bp.route('/download/<path:filepath>')
@login_required
def download_file(filepath):
    abs_path = os.path.join(DL0_FOLDER, filepath)
    folder, filename = os.path.split(abs_path)
    return send_from_directory(folder, filename, as_attachment=True)

class DummyOperator(BaseOperator):
    def execute(self, context):
        pass

class heasarcExplorerPlugin(AirflowPlugin):
    name = "heasarc_explorer_plugin"
    operators = [DummyOperator]
    flask_blueprints = [heasarc_explorer_bp]