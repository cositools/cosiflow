"""Compatibility wrapper for the shared inbound preparation path."""

from gcn_shared.payloads import prepare_inbound_notice


def parse_notice_payload(*args, **kwargs):
    return prepare_inbound_notice(*args, **kwargs)


__all__ = ["parse_notice_payload"]
