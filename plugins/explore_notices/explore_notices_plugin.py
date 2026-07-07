import json
import os
import re
from datetime import datetime
from urllib.parse import urlencode

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


def _normalize_topics(values):
    topics = []
    seen = set()
    for value in values:
        for topic in value.split(","):
            topic = topic.strip()
            if topic and topic not in seen:
                topics.append(topic)
                seen.add(topic)
    return topics


def _bounded_int(value, default, minimum, maximum):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return min(max(number, minimum), maximum)


def _notice_where(topics, validation_status, content_type):
    where = []
    params = []
    if topics:
        topic_clauses = []
        for topic in topics:
            topic_clauses.append("topic = %s")
            params.append(topic)
        where.append(f"({' OR '.join(topic_clauses)})")
    if validation_status:
        where.append("validation_status = %s")
        params.append(validation_status)
    if content_type:
        where.append("content_type = %s")
        params.append(content_type)
    return f"WHERE {' AND '.join(where)}" if where else "", params


def _fetch_notice_count(topics, validation_status, content_type):
    where_sql, params = _notice_where(topics, validation_status, content_type)
    sql = f"SELECT COUNT(*) AS total FROM gcn_inbound_notices {where_sql}"
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row["total"] or 0)


def _fetch_notices(limit, offset, topics, validation_status, content_type):
    where_sql, params = _notice_where(topics, validation_status, content_type)
    sql = f"""
        SELECT
          id, received_at, source, topic, kafka_offset, content_type,
          parse_status, validation_status, mission, instrument,
          alert_type, alert_tense, event_name, trig_id, isotime,
          trigger_time, ra_deg, dec_deg, raw_payload
        FROM gcn_inbound_notices
        {where_sql}
        ORDER BY received_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            notices = cur.fetchall()
    for notice in notices:
        _enrich_notice_display(notice)
    return notices


def _parse_classic_text(raw_payload):
    fields = {}
    for line in str(raw_payload or "").splitlines():
        match = re.match(r"^\s*([A-Z0-9_]+):\s*(.*?)\s*$", line)
        if match and match.group(1) not in fields:
            fields[match.group(1)] = match.group(2)
    return fields


def _topic_parts(topic):
    suffix = str(topic or "").rsplit(".", 1)[-1]
    parts = suffix.split("_")
    if len(parts) < 2:
        return None, None
    return parts[0], parts[1]


def _parse_degrees(raw_payload, field):
    match = re.search(rf"^\s*{re.escape(field)}:\s*([+-]?\d+(?:\.\d+)?)d", str(raw_payload or ""), re.MULTILINE)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _parse_classic_trigger_time(fields):
    date_match = re.search(r"\b(\d{2})/(\d{2})/(\d{2})\b", fields.get("GRB_DATE", ""))
    time_match = re.search(r"\{(\d{2}:\d{2}:\d{2}(?:\.\d+)?)\}", fields.get("GRB_TIME", ""))
    if not date_match or not time_match:
        return None
    year, month, day = date_match.groups()
    return f"20{year}-{month}-{day} {time_match.group(1)}"


def _clean_trigger_id(value):
    if value in (None, ""):
        return None
    return str(value).split(",", 1)[0].strip()


def _format_position(ra_deg, dec_deg):
    if ra_deg is None or dec_deg is None:
        return ""
    return f"{float(ra_deg):.4f}, {float(dec_deg):.4f}"


def _enrich_notice_display(notice):
    fields = _parse_classic_text(notice.get("raw_payload"))
    topic_mission, topic_instrument = _topic_parts(notice.get("topic"))
    notice_type = fields.get("NOTICE_TYPE")
    trigger_num = _clean_trigger_id(fields.get("TRIGGER_NUM") or notice.get("trig_id"))
    inferred_ra = _parse_degrees(notice.get("raw_payload"), "GRB_RA")
    inferred_dec = _parse_degrees(notice.get("raw_payload"), "GRB_DEC")

    if not notice.get("mission"):
        notice["mission"] = topic_mission
    if not notice.get("instrument"):
        notice["instrument"] = topic_instrument
    if not notice.get("event_name") and notice_type:
        notice["event_name"] = notice_type
    if trigger_num:
        notice["trig_id"] = trigger_num
    if notice.get("ra_deg") is None and inferred_ra is not None:
        notice["ra_deg"] = inferred_ra
    if notice.get("dec_deg") is None and inferred_dec is not None:
        notice["dec_deg"] = inferred_dec

    event_parts = [value for value in [notice.get("event_name"), notice.get("trig_id")] if value]
    notice["event_display"] = " / ".join(str(value) for value in event_parts)
    notice["time_display"] = notice.get("trigger_time") or notice.get("isotime") or _parse_classic_trigger_time(fields) or ""
    notice["position_display"] = _format_position(notice.get("ra_deg"), notice.get("dec_deg"))


def _fetch_topics():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT topic, COUNT(*) AS notice_count
                FROM gcn_inbound_notices
                GROUP BY topic
                ORDER BY topic
                """
            )
            return cur.fetchall()


def _fetch_notice(notice_id):
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM gcn_inbound_notices WHERE id = %s", (notice_id,))
            notice = cur.fetchone()
    if notice:
        _enrich_notice_display(notice)
    return notice


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


def _page_url(page, filters):
    query = []
    for topic in filters["topics"]:
        query.append(("topic", topic))
    if filters["validation_status"]:
        query.append(("validation_status", filters["validation_status"]))
    if filters["content_type"]:
        query.append(("content_type", filters["content_type"]))
    query.extend(
        [
            ("limit", filters["limit"]),
            ("page", page),
        ]
    )
    return f"?{urlencode(query)}"


def _pagination(page, limit, total_count, filters):
    total_pages = max(1, (total_count + limit - 1) // limit)
    page = min(max(page, 1), total_pages)
    start = ((page - 1) * limit) + 1 if total_count else 0
    end = min(page * limit, total_count)
    return {
        "page": page,
        "limit": limit,
        "total_count": total_count,
        "total_pages": total_pages,
        "start": start,
        "end": end,
        "first_url": _page_url(1, filters),
        "prev_url": _page_url(max(page - 1, 1), filters),
        "next_url": _page_url(min(page + 1, total_pages), filters),
        "last_url": _page_url(total_pages, filters),
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }


class ExploreNoticesView(BaseView):
    default_view = "index"
    route_base = "/explore-notices"

    @expose("/")
    @login_required
    def index(self):
        if not current_user.is_authenticated:
            return redirect("/login/?next=/explore-notices/")
        limit = _bounded_int(request.args.get("limit"), 15, 1, 500)
        page = _bounded_int(request.args.get("page"), 1, 1, 1000000)
        topics = _normalize_topics(request.args.getlist("topic"))
        validation_status = request.args.get("validation_status", "").strip()
        content_type = request.args.get("content_type", "").strip()
        filters = {
            "limit": limit,
            "topics": topics,
            "validation_status": validation_status,
            "content_type": content_type,
        }
        error = None
        notices = []
        heartbeats = []
        available_topics = []
        pagination = _pagination(1, limit, 0, filters)
        try:
            total_count = _fetch_notice_count(topics, validation_status, content_type)
            pagination = _pagination(page, limit, total_count, filters)
            offset = (pagination["page"] - 1) * limit
            notices = _fetch_notices(limit, offset, topics, validation_status, content_type)
            heartbeats = _fetch_heartbeats()
            available_topics = _fetch_topics()
        except Exception as exc:
            error = str(exc)
        return self.render_template(
            "explore_notices.html",
            notices=notices,
            heartbeats=heartbeats,
            available_topics=available_topics,
            pagination=pagination,
            error=error,
            filters=filters,
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
