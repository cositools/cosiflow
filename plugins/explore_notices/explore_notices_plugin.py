import json
import os
from datetime import datetime

import pymysql
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, redirect, request
from flask_appbuilder import BaseView, expose
from flask_login import current_user, login_required


plugin_folder = os.path.dirname(os.path.abspath(__file__))

explore_notices_bp = Blueprint(
    "explore_notices_bp",
    __name__,
    template_folder=os.path.join(plugin_folder, "templates"),
    url_prefix="/explore-notices",
)


def _connect():
    return pymysql.connect(
        host=os.environ.get("GCN_DB_HOST", "gcn-mysql"),
        port=int(os.environ.get("GCN_DB_PORT", "3306")),
        database=os.environ.get("GCN_DB_NAME", "gcn"),
        user=os.environ.get("GCN_DB_USER", "gcn_user"),
        password=os.environ.get("GCN_DB_PASSWORD", "gcn_password"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=int(os.environ.get("GCN_DB_CONNECT_TIMEOUT", "10")),
    )


def _json_pretty(value):
    if value in (None, ""):
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, sort_keys=True)
    try:
        return json.dumps(json.loads(value), indent=2, sort_keys=True)
    except Exception:
        return str(value)


def _fetch_notices(limit, topic, validation_status, content_type):
    where = []
    params = []
    if topic:
        where.append("topic LIKE %s")
        params.append(f"%{topic}%")
    if validation_status:
        where.append("validation_status = %s")
        params.append(validation_status)
    if content_type:
        where.append("content_type = %s")
        params.append(content_type)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
        SELECT
          id, received_at, source, topic, kafka_offset, content_type,
          parse_status, validation_status, mission, instrument,
          alert_type, alert_tense, event_name, trig_id, isotime,
          trigger_time, ra_deg, dec_deg
        FROM gcn_inbound_notices
        {where_sql}
        ORDER BY received_at DESC
        LIMIT %s
    """
    params.append(limit)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def _fetch_notice(notice_id):
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM gcn_inbound_notices WHERE id = %s", (notice_id,))
            return cur.fetchone()


def _fetch_heartbeats():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT component, updated_at, status, details_json
                FROM gcn_client_heartbeats
                ORDER BY component
                """
            )
            rows = cur.fetchall()
    for row in rows:
        row["details_pretty"] = _json_pretty(row.get("details_json"))
    return rows


class ExploreNoticesView(BaseView):
    default_view = "index"
    route_base = "/explore-notices"

    @expose("/")
    @login_required
    def index(self):
        if not current_user.is_authenticated:
            return redirect("/login/?next=/explore-notices/")
        limit = min(max(int(request.args.get("limit", "100")), 1), 500)
        topic = request.args.get("topic", "").strip()
        validation_status = request.args.get("validation_status", "").strip()
        content_type = request.args.get("content_type", "").strip()
        error = None
        notices = []
        heartbeats = []
        try:
            notices = _fetch_notices(limit, topic, validation_status, content_type)
            heartbeats = _fetch_heartbeats()
        except Exception as exc:
            error = str(exc)
        return self.render_template(
            "explore_notices.html",
            notices=notices,
            heartbeats=heartbeats,
            error=error,
            filters={
                "limit": limit,
                "topic": topic,
                "validation_status": validation_status,
                "content_type": content_type,
            },
            now=datetime.utcnow(),
        )

    @expose("/notice/<int:notice_id>")
    @login_required
    def notice_detail(self, notice_id):
        error = None
        notice = None
        try:
            notice = _fetch_notice(notice_id)
            if notice:
                notice["payload_json_pretty"] = _json_pretty(notice.get("payload_json"))
                notice["validation_errors_pretty"] = _json_pretty(notice.get("validation_errors"))
                notice["event_ids_pretty"] = _json_pretty(notice.get("event_ids"))
                notice["classification_pretty"] = _json_pretty(notice.get("classification_json"))
        except Exception as exc:
            error = str(exc)
        return self.render_template(
            "explore_notice_detail.html",
            notice=notice,
            error=error,
        )


class ExploreNoticesPlugin(AirflowPlugin):
    name = "explore_notices_plugin"
    flask_blueprints = [explore_notices_bp]
    appbuilder_views = [
        {
            "name": "Explore Notices",
            "category": "Results Browser",
            "view": ExploreNoticesView(),
        }
    ]
