#!/usr/bin/env python
"""
tests/kafka_push.py — Kafka event publisher / mock injector for local testing.

Real mode  : publishes events using confluent-kafka (reads KAFKA_BOOTSTRAP_SERVERS
             and KAFKA_TOPIC_INGESTION from .env — same config the app uses).
Mock mode  : skips Kafka entirely, calls consumer.process_message() directly
             (needs DB + Temporal running, but NO Kafka/Zookeeper required).

Usage
-----
# Push a single event file (real Kafka):
  python tests/kafka_push.py --file tests/kafka_events/create/create_story.json

# Push all event files in a directory (real Kafka):
  python tests/kafka_push.py --file tests/kafka_events/create/ --delay 1

# Push all fixture files (real Kafka):
  python tests/kafka_push.py --all

# Push a single event file (mock — no Kafka needed):
  python tests/kafka_push.py --file tests/kafka_events/create/create_story.json --mock

# Push all event files in a directory (mock):
  python tests/kafka_push.py --file tests/kafka_events/create/ --mock --delay 1

# Push all fixture files (mock):
  python tests/kafka_push.py --all --mock

# Push all with a 1-second delay between messages:
  python tests/kafka_push.py --all --delay 1
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path

# ── project root on sys.path so `app.*` imports work ─────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("kafka_push")

FIXTURE_ROOT = PROJECT_ROOT / "tests" / "kafka_events"


# ── helpers ───────────────────────────────────────────────────────────────────

def load_event(path: Path) -> str:
    """Load a JSON or JSON5 file and return a compact JSON string."""
    try:
        import json5  # noqa: PLC0415
        return json.dumps(json5.loads(path.read_text(encoding="utf-8")), separators=(",", ":"))
    except ImportError:
        return json.dumps(json.loads(path.read_text(encoding="utf-8")), separators=(",", ":"))


def collect_fixtures() -> list[Path]:
    """Return all *.json files under tests/kafka_events/, sorted."""
    files = sorted(FIXTURE_ROOT.rglob("*.json"))
    if not files:
        logger.warning(f"No .json fixture files found under {FIXTURE_ROOT}")
    return files


# ── real Kafka push (confluent-kafka — same lib the app uses) ─────────────────

def push_via_kafka(payload: str, topic: str, broker: str) -> bool:
    """Produce *payload* to *topic* on *broker*. Returns True on success."""
    import threading  # noqa: PLC0415

    try:
        from confluent_kafka import Producer  # noqa: PLC0415
    except ImportError:
        logger.error("confluent-kafka is not installed. Run: pip install confluent-kafka")
        return False

    done = threading.Event()
    result: list[str | None] = [None]  # None = success, str = error message

    def _on_delivery(err, _msg):
        result[0] = str(err) if err else None
        done.set()

    producer = Producer({
        "bootstrap.servers": broker,
        "socket.timeout.ms": 3000,       # connect timeout per attempt
        "message.timeout.ms": 8000,      # total delivery deadline
        "log_level": 0,                  # silence rdkafka internal stderr
        "error_cb": lambda err: None,    # suppress global error_cb stderr
    }, logger=logging.getLogger("rdkafka"))

    producer.produce(topic, value=payload.encode("utf-8"), callback=_on_delivery)

    # Poll in a tight loop until the delivery callback fires or we time out
    deadline = time.monotonic() + 10
    while not done.is_set() and time.monotonic() < deadline:
        producer.poll(0.1)

    if not done.is_set():
        print(
            f"\n❌  Kafka not reachable at {broker} — timed out waiting for delivery.\n"
            "   Is Kafka running? Or use --mock to bypass Kafka entirely.",
            flush=True,
        )
        return False

    if result[0] is not None:
        print(
            f"\n❌  Delivery failed: {result[0]}\n"
            "   Is Kafka running? Or use --mock to bypass Kafka entirely.",
            flush=True,
        )
        return False

    return True


# ── mock push — bypass Kafka, call process_message() directly ─────────────────

async def push_via_mock(payload: str) -> None:
    """Directly invoke IngestionConsumer.process_message() — no Kafka needed."""
    from app.kafka.consumer import IngestionConsumer  # noqa: PLC0415
    from app.database.db import db  # noqa: PLC0415

    consumer = IngestionConsumer()
    await consumer.initialize()   # connects DB pool + Temporal client + DLQ producer
    try:
        await consumer.process_message(payload)
    finally:
        # Producer.produce() only enqueues locally — without an explicit flush,
        # this short-lived script can exit before librdkafka's background thread
        # actually sends a DLQ message, silently losing it.
        if consumer.dlq_producer:
            await asyncio.to_thread(consumer.dlq_producer.flush, 10)
        await db.disconnect()


# ── orchestration ─────────────────────────────────────────────────────────────

def run_single(path: Path, args: argparse.Namespace) -> None:
    logger.info(f"📨  Event file : {path.relative_to(PROJECT_ROOT)}")
    payload = load_event(path)

    if args.mock:
        logger.info("🔧  Mock mode — calling process_message() directly (no Kafka)")
        asyncio.run(push_via_mock(payload))
        logger.info("✅  Done")
    else:
        from app.config import settings  # noqa: PLC0415
        topic  = args.topic  or settings.KAFKA_TOPIC_INGESTION
        broker = args.broker or settings.KAFKA_BOOTSTRAP_SERVERS
        logger.info(f"📡  Pushing to topic '{topic}' on {broker}")
        if push_via_kafka(payload, topic, broker):
            logger.info("✅  Message delivered successfully")
        else:
            sys.exit(1)


def run_file_list(files: list[Path], args: argparse.Namespace) -> None:
    logger.info(f"📂  Found {len(files)} fixture file(s)")
    ok, fail = 0, 0

    # Resolve Kafka settings once (only needed in real mode)
    topic = broker = None
    if not args.mock:
        from app.config import settings  # noqa: PLC0415
        topic  = args.topic  or settings.KAFKA_TOPIC_INGESTION
        broker = args.broker or settings.KAFKA_BOOTSTRAP_SERVERS
        logger.info(f"📡  Topic: '{topic}'  Broker: {broker}")

    for idx, path in enumerate(files, start=1):
        logger.info(f"\n[{idx}/{len(files)}] {path.relative_to(PROJECT_ROOT)}")
        try:
            payload = load_event(path)
        except Exception as exc:
            logger.error(f"  ❌ Failed to load: {exc}")
            fail += 1
            continue

        if args.mock:
            try:
                asyncio.run(push_via_mock(payload))
                logger.info("  ✅ Processed via mock")
                ok += 1
            except Exception as exc:
                logger.error(f"  ❌ Mock error: {exc}")
                fail += 1
        else:
            if push_via_kafka(payload, topic, broker):
                logger.info("  ✅ Delivered")
                ok += 1
            else:
                fail += 1

        if args.delay > 0 and idx < len(files):
            logger.info(f"  ⏳ Waiting {args.delay}s…")
            time.sleep(args.delay)

    logger.info(f"\n📊  Summary: {ok} succeeded, {fail} failed")
    if fail:
        sys.exit(1)


def run_all(args: argparse.Namespace) -> None:
    files = collect_fixtures()
    if not files:
        sys.exit(1)
    run_file_list(files, args)


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafka_push",
        description="Push Kafka test events (real broker or mock mode).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--file", "-f", metavar="FILE",
                     help="Path to a single JSON/JSON5 event file or a directory of files.")
    src.add_argument("--all", "-a", action="store_true",
                     help="Push every file under tests/kafka_events/.")

    parser.add_argument("--mock", "-m", action="store_true",
                        help="Bypass Kafka; call consumer.process_message() directly.")
    parser.add_argument("--topic", default=None,
                        help="Kafka topic override (default: from .env KAFKA_TOPIC_INGESTION).")
    parser.add_argument("--broker", default=None,
                        help="Kafka broker override (default: from .env KAFKA_BOOTSTRAP_SERVERS).")
    parser.add_argument("--delay", type=float, default=0.0, metavar="SECONDS",
                        help="Seconds between messages when using --all or directory (default: 0).")
    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.file:
        path = Path(args.file)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        if not path.exists():
            logger.error(f"File/Directory not found: {path}")
            sys.exit(1)
        if path.is_dir():
            files = sorted(path.rglob("*.json"))
            if not files:
                logger.error(f"No .json files found under directory: {path}")
                sys.exit(1)
            run_file_list(files, args)
        else:
            run_single(path, args)
    else:
        run_all(args)


if __name__ == "__main__":
    main()
