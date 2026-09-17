from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from .support import load_script


class AbortRaised(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


AUTH_MANAGER_HOLDER = {}
REQUEST = SimpleNamespace(full_path="/protected?")


def _abort(status_code):
    raise AbortRaised(status_code)


def _redirect(location):
    return "redirect", location


def load_shared_auth():
    flask = ModuleType("flask")
    flask.abort = _abort
    flask.redirect = _redirect
    flask.request = REQUEST

    airflow = ModuleType("airflow")
    airflow_www = ModuleType("airflow.www")
    airflow_extensions = ModuleType("airflow.www.extensions")
    auth_extension = ModuleType("airflow.www.extensions.init_auth_manager")
    auth_extension.get_auth_manager = lambda: AUTH_MANAGER_HOLDER["manager"]

    modules = {
        "flask": flask,
        "airflow": airflow,
        "airflow.www": airflow_www,
        "airflow.www.extensions": airflow_extensions,
        "airflow.www.extensions.init_auth_manager": auth_extension,
    }
    with patch.dict(sys.modules, modules):
        return load_script(
            "plugins/shared_auth/__init__.py", "cosiflow_shared_auth_under_test"
        )


SHARED_AUTH = load_shared_auth()
