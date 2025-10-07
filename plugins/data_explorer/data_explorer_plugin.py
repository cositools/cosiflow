import os
import traceback
import base64
import mimetypes
from pathlib import Path
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from flask import Blueprint, render_template, send_from_directory, redirect, url_for, session, jsonify, abort
from jinja2 import Environment
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
        
        # Check if the folder path is within the allowed directory
        if not os.path.commonpath([DL0_FOLDER, folder_path]).startswith(DL0_FOLDER):
            abort(403)
        
        # Check if the directory exists
        if not os.path.exists(folder_path):
            return render_template("explorer.html", 
                                folders=[], 
                                files=[], 
                                foldername=foldername, 
                                current_path=folder_path,
                                error_message=f"Directory '{foldername}' does not exist.")
        
        # Check if the path is actually a directory
        if not os.path.isdir(folder_path):
            return render_template("explorer.html", 
                                folders=[], 
                                files=[], 
                                foldername=foldername, 
                                current_path=folder_path,
                                error_message=f"'{foldername}' is not a directory.")

        # Show all files in the folder, not only pdfs
        files   = sorted([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
        folders = sorted([f for f in os.listdir(folder_path) if not os.path.isfile(os.path.join(folder_path, f))])
        
        # Add parent directory to folders list if we're not at root
        if foldername:
            parent_path = os.path.dirname(foldername)
            if parent_path and parent_path != foldername:  # Not at root
                folders.insert(0, "..")  # Add parent directory indicator
        
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

@heasarc_explorer_bp.route('/preview/<path:filepath>')
@login_required
def preview_file(filepath):
    print(f"Preview request for: {filepath}")  # Debug logging
    try:
        abs_path = os.path.join(DL0_FOLDER, filepath)
        
        # Security check - ensure path is within allowed directory
        if not os.path.commonpath([DL0_FOLDER, abs_path]).startswith(DL0_FOLDER):
            return jsonify({"error": "Access denied"}), 403
        
        # Check if file exists
        if not os.path.exists(abs_path):
            return jsonify({"error": "File not found"}), 404
        
        if not os.path.isfile(abs_path):
            return jsonify({"error": "Not a file"}), 400
        
        # Get file info
        file_size = os.path.getsize(abs_path)
        mime_type, _ = mimetypes.guess_type(abs_path)
        
        # Determine content type
        content_type = get_content_type(abs_path, mime_type)
        
        # For large files, don't load them
        if file_size > 10 * 1024 * 1024:  # 10MB limit
            return jsonify({
                "content_type": "binary",
                "size": file_size,
                "mime_type": mime_type or "application/octet-stream"
            })
        
        if content_type == "image":
            # Load image as base64
            with open(abs_path, 'rb') as f:
                content = base64.b64encode(f.read()).decode('utf-8')
            return jsonify({
                "content_type": "image",
                "content": content,
                "mime_type": mime_type or "application/octet-stream",
                "size": file_size
            })
        
        elif content_type == "text":
            # Load text file
            try:
                with open(abs_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                # Limit text preview to first 50KB
                if len(content) > 50000:
                    content = content[:50000] + "\n... (truncated)"
                return jsonify({
                    "content_type": "text",
                    "content": content,
                    "mime_type": mime_type or "text/plain",
                    "size": file_size
                })
            except UnicodeDecodeError:
                return jsonify({
                    "content_type": "binary",
                    "size": file_size,
                    "mime_type": mime_type or "application/octet-stream"
                })
        
        else:
            return jsonify({
                "content_type": "binary",
                "size": file_size,
                "mime_type": mime_type or "application/octet-stream"
            })
    
    except Exception as e:
        return jsonify({"error": f"Error loading file: {str(e)}"}), 500

def get_content_type(filepath, mime_type):
    """Determine content type based on file extension and mime type"""
    if not mime_type:
        mime_type = "application/octet-stream"
    
    # Image types
    if mime_type.startswith('image/'):
        return "image"
    
    # Text types
    if (mime_type.startswith('text/') or 
        mime_type in ['application/json', 'application/xml', 'application/javascript']):
        return "text"
    
    # Check by extension for common text files
    ext = os.path.splitext(filepath)[1].lower()
    text_extensions = ['.txt', '.py', '.js', '.html', '.css', '.json', '.xml', '.yaml', '.yml', '.md', '.log', '.csv']
    if ext in text_extensions:
        return "text"
    
    # Check by extension for common image files
    image_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg', '.webp']
    if ext in image_extensions:
        return "image"
    
    return "binary"


# Register template function
@heasarc_explorer_bp.app_template_global()
def get_file_icon(filename):
    """Get appropriate icon for file type - template global function"""
    ext = os.path.splitext(filename)[1].lower()
    
    # Image files
    if ext in ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg', '.webp']:
        return "🖼️"
    
    # Text files
    if ext in ['.txt', '.md', '.log']:
        return "📄"
    
    # Code files
    if ext in ['.py']:
        return "🐍"
    elif ext in ['.js']:
        return "📜"
    elif ext in ['.html', '.htm']:
        return "🌐"
    elif ext in ['.css']:
        return "🎨"
    elif ext in ['.json']:
        return "📋"
    elif ext in ['.xml', '.yaml', '.yml']:
        return "⚙️"
    
    # Data files
    if ext in ['.csv']:
        return "📊"
    elif ext in ['.hdf5', '.h5']:
        return "🗃️"
    elif ext in ['.fits', '.fit']:
        return "🔭"
    
    # Archive files
    if ext in ['.zip', '.tar', '.gz', '.rar']:
        return "📦"
    
    # Default
    return "📄"

class DummyOperator(BaseOperator):
    def execute(self, context):
        pass

class heasarcExplorerPlugin(AirflowPlugin):
    name = "heasarc_explorer_plugin"
    operators = [DummyOperator]
    flask_blueprints = [heasarc_explorer_bp]