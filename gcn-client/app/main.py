from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import signal
import threading

from dotenv import load_dotenv

from app.config import load_settings
from app.db.store import NoticeStore
from app.logging_config import configure_logging
from app.schemas.validator import CosiNoticeValidator
from app.services.inbound_service import InboundService
from app.services.outbox_service import OutboxService

logger = logging.getLogger(__name__)


def _run_supervised(store: NoticeStore, components) -> None:
    outcomes: queue.Queue[tuple[str, BaseException | None]] = queue.Queue()

    def runner(name, target):
        try:
            target()
        except BaseException as exc:
            logger.exception("GCN component %s failed", name)
            store.heartbeat(name, "failed", {"error_class": type(exc).__name__})
            store.lifecycle_event("component_failed", {"component": name, "error_class": type(exc).__name__})
            outcomes.put((name, exc))
        else:
            outcomes.put((name, None))

    for name, target in components:
        threading.Thread(target=runner, args=(name, target), name=name, daemon=True).start()

    name, exc = outcomes.get()
    if exc is not None:
        raise RuntimeError(f"GCN component {name} failed") from exc
    raise RuntimeError(f"GCN component {name} exited unexpectedly")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="COSIflow GCN client")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db")
    sub.add_parser("run")
    sub.add_parser("run-inbound")
    sub.add_parser("run-outbox")
    sub.add_parser("process-outbox-once")
    sub.add_parser("healthcheck")

    consume = sub.add_parser("consume-inbound-once")
    consume.add_argument("--seconds", type=float, default=30.0)
    consume.add_argument("--max-messages", type=int, default=1)

    inject = sub.add_parser("inject-inbound")
    inject.add_argument("--file", required=True)
    inject.add_argument("--topic", required=True)
    inject.add_argument("--source", default="injection")

    queue_parser = sub.add_parser("queue-outbound")
    queue_parser.add_argument("--file", required=True)
    queue_parser.add_argument("--topic")
    queue_parser.add_argument("--idempotency-key")

    args = parser.parse_args()
    settings = load_settings()
    configure_logging(settings.log_level)
    store = NoticeStore(settings)

    if args.command == "init-db":
        store.init_schema()
        return
    if args.command == "healthcheck":
        store.assert_healthy(
            float(os.getenv("GCN_HEARTBEAT_DEGRADED_SECONDS", "30")),
            float(os.getenv("GCN_HEARTBEAT_OFFLINE_SECONDS", "90")),
        )
        return

    if settings.init_db_on_start:
        store.init_schema()

    validator = CosiNoticeValidator(settings.schema_root, settings.cosi_alert_schema)
    inbound = InboundService(settings, store, validator)
    outbox = OutboxService(settings, store, validator)

    if args.command == "run":
        store.lifecycle_event("started", {"pid": os.getpid()})

        def handle_signal(signum, _frame):
            store.lifecycle_event("stopping", {"signal": signum})
            raise SystemExit(128 + signum)

        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)
        _run_supervised(
            store,
            (("inbound", inbound.run_forever), ("outbox", outbox.run_forever)),
        )
    elif args.command == "run-inbound":
        inbound.run_forever()
    elif args.command == "consume-inbound-once":
        count = inbound.consume_for(args.seconds, args.max_messages)
        logger.info("Stored %s inbound notice(s)", count)
        print(count)
    elif args.command == "run-outbox":
        outbox.run_forever()
    elif args.command == "process-outbox-once":
        count = outbox.process_once()
        logger.info("Processed %s outbound notice(s)", count)
    elif args.command == "inject-inbound":
        print(inbound.inject(_read_text(args.file), topic=args.topic, source=args.source))
    elif args.command == "queue-outbound":
        payload = json.loads(_read_text(args.file))
        print(outbox.queue_payload(payload, topic=args.topic, idempotency_key=args.idempotency_key))


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


if __name__ == "__main__":
    main()
