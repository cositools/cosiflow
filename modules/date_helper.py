from datetime import datetime, date
import re
import os


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
    q = q.strip()
    op = "=="
    for candidate in ("==", ">=", "<=", ">", "<"):
        if q.startswith(candidate):
            op = candidate
            q = q[len(candidate):].strip()
            break
    d = _parse_date_string(q)
    return op, d


def _apply_date_queries(d: date, queries) -> bool:
    """Return True if the date d satisfies ALL queries."""
    if not queries:
        return True

    if isinstance(queries, str):
        queries = [queries]

    for q in queries:
        try:
            op, target = _parse_date_query(q)
        except Exception as e:
            print(f"[COSIDAG] _apply_date_queries: parse error for {q!r}: {e}; ignoring condition")
            continue

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
