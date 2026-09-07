"""Pure heartbeat-state classification shared by the UI and security tests."""

from __future__ import annotations

from datetime import datetime, timezone


def classify_heartbeat(row, *, now=None, degraded_after=30.0, offline_after=90.0):
    now = now or datetime.now(timezone.utc)
    status = str(row.get("status") or "").strip().lower()
    if status in {"failed", "error", "dead"}:
        return "Failed"
    updated_at = row.get("updated_at")
    if updated_at is None:
        return "Starting/Unknown"
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    age_seconds = max(0.0, (now - updated_at).total_seconds())
    if age_seconds > offline_after:
        return "Offline"
    if age_seconds > degraded_after or status in {"warning", "idle", "degraded", "locked"}:
        return "Degraded"
    if status == "running":
        return "Running"
    return "Starting/Unknown"


def aggregate_status(heartbeats):
    statuses = {str(row.get("status") or "Starting/Unknown") for row in heartbeats}
    for status in ("Failed", "Offline", "Degraded", "Running", "Starting/Unknown"):
        if status in statuses:
            return status
    return "Starting/Unknown"
