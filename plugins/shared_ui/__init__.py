"""Shared Jinja resources for COSIflow Airflow plugins."""

from pathlib import Path

from jinja2 import ChoiceLoader, FileSystemLoader


SHARED_TEMPLATES_FOLDER = Path(__file__).resolve().parent / "templates"


def add_shared_templates(blueprint):
    """Make shared templates available without replacing plugin templates."""
    plugin_loader = blueprint.jinja_loader
    shared_loader = FileSystemLoader(str(SHARED_TEMPLATES_FOLDER))
    blueprint.jinja_loader = ChoiceLoader([plugin_loader, shared_loader])
    return blueprint
