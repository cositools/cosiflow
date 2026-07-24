import os
import traceback
import base64
import mimetypes
import struct
import zlib
from pathlib import Path
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from flask import Blueprint, render_template, send_from_directory, redirect, url_for, session, jsonify, abort
from flask_appbuilder import BaseView, expose
from jinja2 import Environment
from flask_login import login_required, current_user

# Get from the env variable COSI_DATA_DIR the path to the data directory if it is not set, use the default path
DL0_FOLDER = os.environ.get("COSI_DATA_DIR", "/home/gamma/workspace/data")

# Definiamo il percorso assoluto alla cartella del plugin
plugin_folder = os.path.dirname(os.path.abspath(__file__))

# Blueprint con percorso assoluto a templates e static
# Usato solo per registrare il path dei template
heasarc_explorer_bp = Blueprint(
    "heasarc_explorer_bp",
    __name__,
    template_folder=os.path.join(plugin_folder, "templates"),
    static_folder=os.path.join(plugin_folder, "static"),
    url_prefix='/heasarcbrowser'
)


TOP_LEVEL_MENU_ORDER = (
    "HEASARC Explorer",
    "GCN Notices Explorer",
    "Develop Tools",
)


def _reorder_top_level_menu(menu_items):
    """Reorder selected top-level entries without moving Airflow core menus."""
    desired_position = {
        name.casefold(): position
        for position, name in enumerate(TOP_LEVEL_MENU_ORDER)
    }
    target_indices = [
        index
        for index, item in enumerate(menu_items)
        if str(getattr(item, "name", "")).casefold() in desired_position
    ]
    target_items = sorted(
        (menu_items[index] for index in target_indices),
        key=lambda item: desired_position[str(item.name).casefold()],
    )
    for index, item in zip(target_indices, target_items):
        menu_items[index] = item


@heasarc_explorer_bp.record_once
def _apply_top_level_menu_order(state):
    """Apply custom menu ordering after Airflow registers all plugin views."""
    appbuilder = getattr(state.app, "appbuilder", None)
    menu = getattr(appbuilder, "menu", None)
    menu_items = getattr(menu, "menu", None)
    if isinstance(menu_items, list):
        _reorder_top_level_menu(menu_items)


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

def get_file_icon(filename):
    """Get appropriate icon for file type"""
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


PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
PNG_TEXT_CHUNKS = {b"tEXt", b"zTXt", b"iTXt"}
MAX_PNG_TEXT_SIZE = 1024 * 1024


def _decompress_png_text(payload):
    """Decompress a PNG text payload while enforcing a one-megabyte limit."""
    decompressor = zlib.decompressobj()
    text = decompressor.decompress(payload, MAX_PNG_TEXT_SIZE + 1)
    if len(text) > MAX_PNG_TEXT_SIZE or decompressor.unconsumed_tail:
        return None
    text += decompressor.flush()
    return text if len(text) <= MAX_PNG_TEXT_SIZE else None


def _decode_png_text_chunk(chunk_type, payload):
    """Decode one valid PNG tEXt, zTXt, or iTXt chunk."""
    try:
        keyword, remainder = payload.split(b"\0", 1)
        key = keyword.decode("latin-1")
        if not key:
            return None

        if chunk_type == b"tEXt":
            return key, remainder.decode("latin-1")

        if chunk_type == b"zTXt":
            if not remainder or remainder[0] != 0:
                return None
            text = _decompress_png_text(remainder[1:])
            return (key, text.decode("latin-1")) if text is not None else None

        if len(remainder) < 2:
            return None
        compression_flag, compression_method = remainder[0], remainder[1]
        language, translated_and_text = remainder[2:].split(b"\0", 1)
        translated_keyword, text = translated_and_text.split(b"\0", 1)
        # Language and translated keyword are intentionally parsed but are not
        # exposed; the English keyword is the stable API key used by the UI.
        del language, translated_keyword
        if compression_flag == 1:
            if compression_method != 0:
                return None
            text = _decompress_png_text(text)
            if text is None:
                return None
        elif compression_flag != 0:
            return None
        return key, text.decode("utf-8")
    except (UnicodeDecodeError, ValueError, zlib.error):
        return None


def get_image_metadata(filepath):
    """Read textual PNG metadata without optional image-processing packages."""
    if Path(filepath).suffix.lower() != ".png":
        return {}

    metadata = {}
    try:
        with open(filepath, "rb") as image:
            if image.read(len(PNG_SIGNATURE)) != PNG_SIGNATURE:
                return {}

            for _ in range(4096):
                length_bytes = image.read(4)
                if len(length_bytes) != 4:
                    break
                chunk_length = struct.unpack(">I", length_bytes)[0]
                chunk_type = image.read(4)
                if len(chunk_type) != 4:
                    break

                if chunk_length > MAX_PNG_TEXT_SIZE:
                    image.seek(chunk_length + 4, os.SEEK_CUR)
                    continue

                payload = image.read(chunk_length)
                crc_bytes = image.read(4)
                if len(payload) != chunk_length or len(crc_bytes) != 4:
                    break

                expected_crc = struct.unpack(">I", crc_bytes)[0]
                actual_crc = zlib.crc32(chunk_type)
                actual_crc = zlib.crc32(payload, actual_crc) & 0xFFFFFFFF
                if expected_crc != actual_crc:
                    continue

                if chunk_type in PNG_TEXT_CHUNKS:
                    decoded = _decode_png_text_chunk(chunk_type, payload)
                    if decoded is not None:
                        key, value = decoded
                        metadata[key] = value
                if chunk_type == b"IEND":
                    break
        return metadata
    except (OSError, struct.error):
        # A malformed or unsupported image must not prevent the preview itself.
        return {}


def get_plot_preview_title(filepath, metadata):
    """Resolve the preview heading from embedded metadata with safe fallbacks."""
    preview_title = str(metadata.get("PreviewTitle", "")).strip()
    if preview_title:
        return preview_title

    embedded_title = str(metadata.get("Title", "")).strip()
    for separator in (" — ", "\n"):
        if separator in embedded_title:
            title_detail = embedded_title.rsplit(separator, 1)[-1].strip()
            if title_detail:
                return title_detail

    description = str(
        metadata.get("Description")
        or metadata.get("Caption")
        or ""
    ).strip()
    if description:
        first_sentence = description.split(". ", 1)[0].rstrip(".").strip()
        if first_sentence:
            return first_sentence

    if embedded_title:
        return embedded_title

    filename = Path(filepath).stem.replace("_", " ").replace("-", " ").strip()
    return filename.title() or "Plot"


class HEASARCExplorerView(BaseView):
    default_view = "explorer_home"
    route_base = "/heasarcbrowser"

    @expose('/')
    def explorer_home(self):
        if not current_user.is_authenticated:
            return redirect('/login/?next=/heasarcbrowser/')
        try:
            folders = sorted([f for f in os.listdir(DL0_FOLDER) if os.path.isdir(os.path.join(DL0_FOLDER, f))])
            # Use self.render_template instead of flask.render_template
            return self.render_template("explorer.html", folders=folders, current_path=DL0_FOLDER, get_file_icon=get_file_icon)
        except PermissionError:
            abort(403)
        except Exception as e:
            error_traceback = traceback.format_exc()
            # If self.render_template fails (e.g. during an error), try to return a simple error response
            # But the error 'appbuilder is undefined' suggests we might have issues even getting there if render_template is called
            # Let's ensure we are using self.render_template which injects appbuilder
            return f"Error loading folders: {e}\n\nTraceback:\n{error_traceback}", 500

    @expose('/folder/<path:foldername>')
    @login_required
    def explorer_folder(self, foldername):
        try:
            folder_path = os.path.join(DL0_FOLDER, foldername)
            
            # Check if the folder path is within the allowed directory
            if not os.path.commonpath([DL0_FOLDER, folder_path]).startswith(DL0_FOLDER):
                abort(403)
            
            # Check if the directory exists
            if not os.path.exists(folder_path):
                return self.render_template("explorer.html", 
                                    folders=[], 
                                    files=[], 
                                    foldername=foldername, 
                                    current_path=folder_path,
                                    error_message=f"Directory '{foldername}' does not exist.",
                                    get_file_icon=get_file_icon)
            
            # Check if the path is actually a directory
            if not os.path.isdir(folder_path):
                return self.render_template("explorer.html", 
                                    folders=[], 
                                    files=[], 
                                    foldername=foldername, 
                                    current_path=folder_path,
                                    error_message=f"'{foldername}' is not a directory.",
                                    get_file_icon=get_file_icon)

            # Show all files in the folder, not only pdfs
            files   = sorted([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
            folders = sorted([f for f in os.listdir(folder_path) if not os.path.isfile(os.path.join(folder_path, f))])
            
            # Add parent directory to folders list if we're not at root
            if foldername:
                parent_path = os.path.dirname(foldername)
                if parent_path and parent_path != foldername:  # Not at root
                    folders.insert(0, "..")  # Add parent directory indicator
            
            return self.render_template("explorer.html", folders=folders, files=files, foldername=foldername, current_path=folder_path, get_file_icon=get_file_icon)
        except PermissionError:
            abort(403)
        except Exception as e:
            error_traceback = traceback.format_exc()
            return f"Error loading files: {e}\n\nTraceback:\n{error_traceback}", 500

    @expose('/download/<path:filepath>')
    @login_required
    def download_file(self, filepath):
        abs_path = os.path.join(DL0_FOLDER, filepath)
        folder, filename = os.path.split(abs_path)
        return send_from_directory(folder, filename, as_attachment=True)

    @expose('/preview/<path:filepath>')
    @login_required
    def preview_file(self, filepath):
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
                if content_type == "image":
                    metadata = get_image_metadata(abs_path)
                    return jsonify({
                        "content_type": "image_metadata",
                        "size": file_size,
                        "mime_type": mime_type or "application/octet-stream",
                        "preview_title": get_plot_preview_title(abs_path, metadata),
                        "caption": (
                            metadata.get("Description")
                            or metadata.get("Caption")
                        ),
                        "metadata": metadata,
                    })
                return jsonify({
                    "content_type": "binary",
                    "size": file_size,
                    "mime_type": mime_type or "application/octet-stream"
                })
            
            if content_type == "image":
                # Load image as base64
                with open(abs_path, 'rb') as f:
                    content = base64.b64encode(f.read()).decode('utf-8')
                metadata = get_image_metadata(abs_path)
                caption = metadata.get("Description") or metadata.get("Caption")
                return jsonify({
                    "content_type": "image",
                    "content": content,
                    "mime_type": mime_type or "application/octet-stream",
                    "size": file_size,
                    "preview_title": get_plot_preview_title(abs_path, metadata),
                    "caption": caption,
                    "metadata": metadata,
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

class DummyOperator(BaseOperator):
    def execute(self, context):
        pass

class heasarcExplorerPlugin(AirflowPlugin):
    name = "heasarc_explorer_plugin"
    operators = [DummyOperator]
    flask_blueprints = [heasarc_explorer_bp]
    appbuilder_views = [
        {
            # The name of the view, which will be displayed in the menu
            "name": "HEASARC Explorer",
            # Which Category to put the link in, if you don't want one, set to an empty string
            "category": "",
            "view": HEASARCExplorerView()
        }
    ]
