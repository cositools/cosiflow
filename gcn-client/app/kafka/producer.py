from __future__ import annotations

import json
from typing import Any

from app.config import Settings


class GcnProducer:
    def __init__(self, settings: Settings):
        if not settings.client_id or not settings.client_secret:
            raise RuntimeError("GCN_CLIENT_ID and GCN_CLIENT_SECRET are required when producer is enabled")
        from gcn_kafka import Producer

        self._producer = Producer(
            client_id=settings.client_id,
            client_secret=settings.client_secret,
            domain=settings.gcn_domain,
        )

    def publish(self, topic: str, payload: dict[str, Any]) -> dict[str, Any]:
        value = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        self._producer.produce(topic, value=value)
        self._producer.flush()
        return {"topic": topic}
