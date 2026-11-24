from __future__ import annotations


MAX_PAGE_SIZE = 100
MAX_RESULT_WINDOW = 10000
MAX_TOPIC_FILTERS = 20
MAX_DIMENSION_VALUES = 500
NOTICE_PREVIEW_BYTES = 8192


def normalize_topics(values) -> list[str]:
    topics: list[str] = []
    seen: set[str] = set()
    for value in values:
        for raw_topic in str(value).split(","):
            topic = raw_topic.strip()
            if len(topic) > 255:
                raise ValueError("Each topic filter must be at most 255 characters")
            if topic and topic not in seen:
                topics.append(topic)
                seen.add(topic)
                if len(topics) > MAX_TOPIC_FILTERS:
                    raise ValueError(
                        f"At most {MAX_TOPIC_FILTERS} topic filters are allowed"
                    )
    return topics


def query_int(value, default: int, minimum: int, maximum: int, name: str) -> int:
    if value in (None, ""):
        return default
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= number <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return number


def query_text(value, maximum: int, name: str) -> str:
    text = str(value or "").strip()
    if len(text) > maximum:
        raise ValueError(f"{name} must be at most {maximum} characters")
    return text


def validate_result_window(page: int, limit: int) -> int:
    end = (page - 1) * limit + limit
    if end > MAX_RESULT_WINDOW:
        raise ValueError(
            f"The requested result window exceeds {MAX_RESULT_WINDOW} rows"
        )
    return end
