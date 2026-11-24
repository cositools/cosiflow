"""Compatibility imports for shared schema and topic validation."""

from gcn_shared.validation import (  # noqa: F401
    CosiNoticeValidator,
    enforce_prototype_safety,
    topic_is_test,
    topic_kind,
)

__all__ = [
    "CosiNoticeValidator",
    "enforce_prototype_safety",
    "topic_is_test",
    "topic_kind",
]
