"""Install the COSIflow navbar template and logo into the Airflow package."""

from __future__ import annotations

import importlib.util
import os
import shutil
from html import escape
from pathlib import Path


def main() -> None:
    airflow_spec = importlib.util.find_spec("airflow")
    if airflow_spec is None or airflow_spec.origin is None:
        raise RuntimeError("The installed Airflow package could not be located")

    source_dir = Path(__file__).resolve().parent / "branding"
    airflow_www = Path(airflow_spec.origin).resolve().parent / "www"
    navbar_target = airflow_www / "templates" / "appbuilder" / "navbar.html"
    main_target = airflow_www / "templates" / "airflow" / "main.html"
    logo_dir = airflow_www / "static" / "cosiflow"

    for target in (navbar_target, main_target):
        if not target.is_file():
            raise RuntimeError(f"Unsupported Airflow layout: {target} does not exist")

    git_ref = os.environ.get("COSIFLOW_GIT_REF", "unknown")
    git_sha = os.environ.get("COSIFLOW_GIT_SHA", "unknown")
    git_tag = os.environ.get("COSIFLOW_GIT_TAG", "")
    github_url = os.environ.get("COSIFLOW_GITHUB_URL", "https://github.com/cositools/cosiflow").rstrip("/")

    if git_tag:
        version_label = git_tag
        version_url = f"{github_url}/tree/{git_tag}"
    elif git_sha != "unknown":
        version_label = f"{git_ref}:{git_sha[:12]}"
        version_url = f"{github_url}/commit/{git_sha}"
    else:
        version_label = git_ref
        version_url = github_url

    footer = (source_dir / "footer.html").read_text()
    footer = footer.replace("__COSIFLOW_VERSION_LABEL__", escape(version_label))
    footer = footer.replace("__COSIFLOW_VERSION_URL__", escape(version_url, quote=True))

    main_template = main_target.read_text()
    airflow_favicon = (
        '<link rel="icon" type="image/png" '
        'href="{{ url_for(\'static\', filename=\'pin_32.png\') }}">'
    )
    cosiflow_favicon = (
        '<link rel="icon" type="image/webp" '
        'href="{{ url_for(\'static\', filename=\'cosiflow/cosiflow-logo.webp\') }}">'
    )
    if main_template.count(airflow_favicon) != 1:
        raise RuntimeError(f"Unsupported Airflow favicon layout in {main_target}")
    main_template = main_template.replace(airflow_favicon, cosiflow_favicon)

    footer_start = main_template.find("{% block footer %}")
    footer_end = main_template.find("{% endblock %}", footer_start)
    if footer_start == -1 or footer_end == -1:
        raise RuntimeError(f"Unsupported Airflow footer layout in {main_target}")
    footer_end += len("{% endblock %}")

    logo_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_dir / "navbar.html", navbar_target)
    shutil.copyfile(source_dir / "cosiflow-logo.webp", logo_dir / "cosiflow-logo.webp")
    main_target.write_text(main_template[:footer_start] + footer.rstrip() + main_template[footer_end:])


if __name__ == "__main__":
    main()
