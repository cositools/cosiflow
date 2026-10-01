#!/usr/bin/env python3
"""Replace Airflow's fragile stale-scheduler warning with a stable message."""

from __future__ import annotations

import argparse
import sysconfig
from pathlib import Path


ORIGINAL_WARNING = """  {% if scheduler_job is defined and (not scheduler_job or not scheduler_job.is_alive()) %}
    {% call show_message(category='warning', dismissible=false) %}
      <p>The scheduler does not appear to be running.
      {% if scheduler_job %}
      Last heartbeat was received
      <time class="scheduler-last-heartbeat"
        title="{{ scheduler_job.latest_heartbeat.isoformat() }}"
        datetime="{{ scheduler_job.latest_heartbeat.isoformat() }}"
        data-datetime-convert="false"
      >{{ macros.datetime_diff_for_humans(scheduler_job.latest_heartbeat) }}</time>.
      {% endif %}
      </p>
      <p>The DAGs list may not update, and new tasks will not be scheduled.</p>
    {% endcall %}
  {% endif %}
"""

SAFE_WARNING = """  {% if scheduler_job is defined and (not scheduler_job or not scheduler_job.is_alive()) %}
    {% call show_message(category='warning', dismissible=false) %}
      <p>The Airflow scheduler service is currently unavailable. Restart the service or try again later.</p>
      <p>The DAGs list may not update, and new tasks will not be scheduled.</p>
    {% endcall %}
  {% endif %}
"""


def default_template_path() -> Path:
    return (
        Path(sysconfig.get_paths()["purelib"])
        / "airflow"
        / "www"
        / "templates"
        / "airflow"
        / "main.html"
    )


def patch_template(path: Path) -> bool:
    source = path.read_text(encoding="utf-8")
    if SAFE_WARNING in source:
        return False
    if ORIGINAL_WARNING not in source:
        raise RuntimeError(
            "Airflow scheduler warning template changed; refusing an unsafe patch"
        )
    path.write_text(source.replace(ORIGINAL_WARNING, SAFE_WARNING, 1), encoding="utf-8")
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("template", nargs="?", type=Path, default=default_template_path())
    args = parser.parse_args()
    patch_template(args.template)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
