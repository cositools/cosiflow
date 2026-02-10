from flask import Blueprint, redirect
import os

# Blueprint empty (no custom routing, we use only the link)
mailhog_bp = Blueprint(
    "mailhog_bp",
    __name__,
    url_prefix=""
)

@mailhog_bp.route('/')
def redirect_to_mailhog():
    # use the environment variable MAILHOG_WEBUI_URL if it is set, otherwise use the default value
    mail_server = os.environ.get('MAILHOG_WEBUI_URL', 'http://localhost:8025')
    return redirect(mail_server, code=302)