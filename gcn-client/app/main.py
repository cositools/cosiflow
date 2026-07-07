from __future__ import annotations

import argparse
import json
import logging
import threading

from dotenv import load_dotenv

from app.config import load_settings
from app.db.store import NoticeStore
from app.logging_config import configure_logging
from app.schemas.validator import CosiNoticeValidator
from app.services.inbound_service import InboundService
from app.services.outbox_service import OutboxService

logger = logging.getLogger(__name__)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="COSIflow GCN client prototype")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db")
    sub.add_parser("run")
    sub.add_parser("run-inbound")
    sub.add_parser("run-outbox")
    sub.add_parser("process-outbox-once")

    consume = sub.add_parser("consume-inbound-once")
    consume.add_argument("--seconds", type=float, default=30.0)
    consume.add_argument("--max-messages", type=int, default=1)

    inject = sub.add_parser("inject-inbound")
    inject.add_argument("--file", required=True)
    inject.add_argument("--topic", required=True)
    inject.add_argument("--source", default="injection")

    queue = sub.add_parser("queue-outbound")
    queue.add_argument("--file", required=True)
    queue.add_argument("--topic")
    queue.add_argument("--idempotency-key")

    args = parser.parse_args()
    settings = load_settings()
    configure_logging(settings.log_level)
    store = NoticeStore(settings)
    validator = CosiNoticeValidator(settings.schema_root, settings.cosi_alert_schema)

    if args.command == "init-db":
        store.init_schema()
        return

    if settings.init_db_on_start:
        store.init_schema()

    inbound = InboundService(settings, store, validator)
    outbox = OutboxService(settings, store, validator)

    if args.command == "run":
        threads = [
            threading.Thread(target=inbound.run_forever, name="inbound", daemon=True),
            threading.Thread(target=outbox.run_forever, name="outbox", daemon=True),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
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
        raw = _read_text(args.file)
        notice_id = inbound.inject(raw, topic=args.topic, source=args.source)
        print(notice_id)
    elif args.command == "queue-outbound":
        payload = json.loads(_read_text(args.file))
        notice_id = outbox.queue_payload(payload, topic=args.topic, idempotency_key=args.idempotency_key)
        print(notice_id)


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


if __name__ == "__main__":
    main()
