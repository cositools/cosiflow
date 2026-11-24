from datetime import datetime, date
import re


_DATE_OPERATORS = frozenset({"<", "<=", "==", ">=", ">"})


def _looks_like_date_folder(name: str) -> bool:
    return bool(
        re.match(r"^\d{8}(?:_|$)", name) or re.match(r"^\d{4}-\d{2}-\d{2}(?:_|$)", name)
    )


def _parse_date_string(s: str) -> date:
    """Parse 'YYYYMMDD' or 'YYYY-MM-DD' to datetime.date."""
    s = s.strip()
    if re.match(r"^\d{8}$", s):
        return datetime.strptime(s, "%Y%m%d").date()
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_date_query(q: str):
    """
    Parse a query like '>=2025-11-01' to (op, date).
    op ∈ {'==', '>=', '<=', '>', '<'}; if missing → '=='.
    """
    if not isinstance(q, str):
        raise ValueError(f"date query must be a string, got {q!r}")
    q = q.strip()
    if not q:
        raise ValueError("date query must not be empty")
    op = "=="
    for candidate in ("==", ">=", "<=", ">", "<"):
        if q.startswith(candidate):
            op = candidate
            q = q[len(candidate):].strip()
            break
    d = _parse_date_string(q)
    return op, d


def normalize_date_filters(
    date_filters=None,
    *,
    legacy_date=None,
    legacy_queries=None,
) -> tuple[tuple[str, date], ...]:
    """Validate structured or legacy date filters and return parsed values.

    Structured filters use ``[{"operator": ">=", "date": "2026-09-01"}]``.
    Legacy ``date`` and ``date_queries`` remain accepted by code during the UI
    migration, but malformed values always fail closed.
    """
    if date_filters is not None:
        if not isinstance(date_filters, list):
            raise ValueError("date_filters must be a list of objects")
        parsed: list[tuple[str, date]] = []
        for index, item in enumerate(date_filters):
            if not isinstance(item, dict):
                raise ValueError(f"date_filters[{index}] must be an object")
            if set(item) != {"operator", "date"}:
                raise ValueError(
                    f"date_filters[{index}] must contain only 'operator' and 'date'"
                )
            operator = item["operator"]
            if operator not in _DATE_OPERATORS:
                raise ValueError(
                    f"date_filters[{index}].operator must be one of "
                    f"{sorted(_DATE_OPERATORS)!r}"
                )
            raw_date = item["date"]
            if not isinstance(raw_date, str) or not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}", raw_date
            ):
                raise ValueError(
                    f"date_filters[{index}].date must use ISO YYYY-MM-DD"
                )
            parsed.append((operator, _parse_date_string(raw_date)))
        return tuple(parsed)

    queries = legacy_queries
    if queries is None and legacy_date not in (None, ""):
        if not isinstance(legacy_date, str):
            raise ValueError("date must be a YYYYMMDD or YYYY-MM-DD string")
        queries = [f"=={legacy_date}"]
    if queries in (None, ""):
        return ()
    if isinstance(queries, str):
        queries = [queries]
    if not isinstance(queries, list) or not all(isinstance(item, str) for item in queries):
        raise ValueError("date_queries must be a string or list of strings")
    return tuple(_parse_date_query(item) for item in queries)


def serialize_date_filters(filters) -> list[dict[str, str]]:
    """Return validated date filters in the Trigger UI representation."""
    return [
        {"operator": operator, "date": value.isoformat()}
        for operator, value in filters
    ]


def _apply_date_queries(d: date, queries) -> bool:
    """Return True if ``d`` satisfies every already validated query."""
    if not queries:
        return True

    if isinstance(queries, str) or (
        isinstance(queries, list) and queries and isinstance(queries[0], str)
    ):
        queries = normalize_date_filters(legacy_queries=queries)

    for op, target in queries:
        if op == "==" and not (d == target):
            return False
        if op == ">=" and not (d >= target):
            return False
        if op == "<=" and not (d <= target):
            return False
        if op == ">" and not (d > target):
            return False
        if op == "<" and not (d < target):
            return False

    return True
