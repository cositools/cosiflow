from flask import Blueprint, redirect
import os

# Blueprint vuoto (non serve routing personalizzato, usiamo solo il link)
mailhog_bp = Blueprint(
    "mailhog_bp",
    __name__,
    url_prefix="/mailhog"
)

@mailhog_bp.route('/')
def redirect_to_mailhog():
    mail_server = os.environ.get('MAILHOG_WEBUI_URL', '"http://localhost:8025"')
    return redirect(mail_server, code=302)