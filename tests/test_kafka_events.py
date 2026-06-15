import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import json5
import pytest

from app.kafka import consumer as consumer_module


FIXTURE_ROOT = Path(__file__).resolve().parent / "kafka_events"


def _load_fixture(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    return json5.loads(text)


class _FakeConn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def acquire(self):
        return _FakeConn()


@pytest.mark.parametrize(
    ("fixture_path", "expected_event_type"),
    [
        (FIXTURE_ROOT / "create" / "create_discussion.json", "create"),
        # (FIXTURE_ROOT / "create" / "create_story.json", "create"),
        # (FIXTURE_ROOT / "update" / "update_discussion.json", "update"),
        # (FIXTURE_ROOT / "update" / "update_story.json", "update"),
        # (FIXTURE_ROOT / "delete" / "delete_discussion.json", "delete"),
        # (FIXTURE_ROOT / "delete" / "delete_story.json", "delete"),
    ],
)
def test_process_message_routes_fixture_events(monkeypatch, fixture_path, expected_event_type):
    event = _load_fixture(fixture_path)
    payload = json.dumps(event)

    consumer = consumer_module.IngestionConsumer()

    insert_mock = AsyncMock()
    delete_mock = AsyncMock()
    trigger_mock = AsyncMock()

    monkeypatch.setattr(consumer_module.settings, "PROCESSING_MODE", "real-time", raising=False)
    monkeypatch.setattr(consumer_module.db, "pool", SimpleNamespace(acquire=_FakePool().acquire), raising=False)
    monkeypatch.setattr(consumer_module, "insert_or_update_submission", insert_mock, raising=False)
    monkeypatch.setattr(consumer_module, "delete_submission", delete_mock, raising=False)
    monkeypatch.setattr(consumer, "_trigger_realtime_workflow", trigger_mock, raising=False)

    asyncio.run(consumer.process_message(payload))

    if expected_event_type in {"create", "update"}:
        insert_mock.assert_awaited_once()
        trigger_mock.assert_awaited_once_with(str(event["submissionId"]), event["tenantCode"], event["submissionType"])
        delete_mock.assert_not_awaited()
    else:
        assert delete_mock.await_count == 1
        assert delete_mock.await_args.args[1:] == (str(event["submissionId"]), event["tenantCode"])
        insert_mock.assert_not_awaited()
        trigger_mock.assert_not_awaited()
