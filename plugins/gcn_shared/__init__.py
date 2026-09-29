"""Shared GCN payload and persistence contracts.

This package intentionally has no Airflow dependency.  Both the GCN client and
the Notices Explorer import it so manual and worker-driven ingestion cannot
drift apart.
"""

from .payloads import prepare_inbound_notice, prepare_outbound_notice
from .storage import IdempotencyConflictError, insert_inbound_notice, queue_outbound_notice
from .validation import CosiNoticeValidator, enforce_prototype_safety

__all__ = [
    "CosiNoticeValidator",
    "IdempotencyConflictError",
    "enforce_prototype_safety",
    "insert_inbound_notice",
    "prepare_inbound_notice",
    "prepare_outbound_notice",
    "queue_outbound_notice",
]
