"""
Unit tests (in-memory fakes) of where the feedback process steps come from: the persona's active upload config
(app/database/feedback_operations.get_feedback_process_steps), the story/discussion dispatch (get_process_steps),
and the call sites that used settings.get_process_config for feedback (the Temporal activities). The real-Postgres
lookup is in tests/test_feedback_config_pg.py.
"""
import asyncio
import json
from unittest.mock import AsyncMock

import pytest

import app.temporal.activities as activities
from app.config import settings
from app.database import feedback_operations as ops
from seed_support import mitra_config_v1
from conftest import FakeConn, install_fake_db

PROGRAM = "dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a"
EXTRACTION = [{"name": "feedback_metadata_extraction"}]
SUB_ROW = {"session_type": "Youth_Voice", "source": "mitra", "program_id": PROGRAM}


def _conn(sub=SUB_ROW, config_row="default"):
    conn = FakeConn()
    if config_row == "default":
        # the seed default is no steps (extraction is opt-in); these tests need a config that has one
        config_row = {"version": 3, "config": {**mitra_config_v1("Youth"), "process_steps": EXTRACTION}}
    conn.fetchrow = AsyncMock(side_effect=[sub, config_row])
    return conn


def _steps(conn):
    return asyncio.run(ops.get_feedback_process_steps(conn, "184884", "mitra"))


def test_steps_are_the_active_config_versions_process_steps():
    conn = _conn()
    assert _steps(conn) == [{"name": "feedback_metadata_extraction"}]
    first, second = conn.fetchrow.await_args_list
    # the submission is looked up by (submission_id, tenant) ...
    assert first.args[1:] == ("184884", "mitra")
    # ... and its persona (session_type + source, same tenant) picks the config; only the ACTIVE version is read
    assert "v.is_active" in second.args[0] and "c.config_type" in second.args[0]
    assert second.args[1:5] == ("feedback_upload", "mitra", "Youth_Voice", "mitra")
    assert second.args[5] == PROGRAM


def test_the_source_of_the_submission_selects_the_config_source():
    conn = _conn(sub={**SUB_ROW, "source": "google_forms"})
    _steps(conn)
    assert conn.fetchrow.await_args_list[1].args[2] == "google_forms"


def test_config_returned_as_json_text_is_accepted():
    conn = _conn(config_row={"version": 1, "config": json.dumps({**mitra_config_v1("Youth"), "process_steps": EXTRACTION})})
    assert _steps(conn) == [{"name": "feedback_metadata_extraction"}]


def test_empty_steps_list_is_valid():
    config = {**mitra_config_v1("Youth"), "process_steps": []}
    assert _steps(_conn(config_row={"version": 1, "config": config})) == []


def test_story_style_steps_are_returned_as_configured():
    steps = [{"name": "feedback_metadata_extraction", "llm_model": "m", "max_tokens": 100, "llm_timeout_seconds": 30}]
    config = {**mitra_config_v1("Youth"), "process_steps": steps}
    assert _steps(_conn(config_row={"version": 1, "config": config})) == steps


def test_unknown_submission_is_an_error_not_an_empty_list():
    with pytest.raises(ValueError, match="No stored feedback submission '184884'"):
        _steps(_conn(sub=None))


def test_persona_without_an_active_config_is_an_error():
    with pytest.raises(ValueError, match="No active mitra feedback_upload config for session_type 'Youth_Voice'"):
        _steps(_conn(config_row=None))


def test_config_without_process_steps_is_an_error_with_the_migrate_hint():
    config = mitra_config_v1("Youth")
    del config["process_steps"]
    with pytest.raises(ValueError, match="missing 'process_steps'.*tools/feedback_config.py migrate"):
        _steps(_conn(config_row={"version": 1, "config": config}))


@pytest.mark.parametrize("bad, needle", [
    (None, "must be a list"),
    ("feedback_metadata_extraction", "must be a list"),
    ([{"columns": ["x"]}], "non-empty string 'name'"),
    ([{"name": ""}], "non-empty string 'name'"),
    (["feedback_metadata_extraction"], "must be an object"),
])
def test_invalid_process_steps_is_an_error(bad, needle):
    config = {**mitra_config_v1("Youth"), "process_steps": bad}
    with pytest.raises(ValueError, match=needle):
        _steps(_conn(config_row={"version": 1, "config": config}))


# dispatcher -----------------------------------------------------------------------------------

def test_get_process_steps_dispatches_by_type(monkeypatch):
    monkeypatch.setattr(settings, "PROCESS_CONFIG_STORY", '[{"name": "story_step"}]')
    monkeypatch.setattr(settings, "PROCESS_CONFIG_DISCUSSION", '[{"name": "discussion_step"}]')
    conn = _conn()

    async def run():
        assert await ops.get_process_steps(conn, "story", "1", "mitra") == [{"name": "story_step"}]
        assert await ops.get_process_steps(conn, "discussion", "1", "mitra") == [{"name": "discussion_step"}]
        assert await ops.get_process_steps(conn, "survey", "1", "mitra") == []
        conn.fetchrow.assert_not_awaited()  # story / discussion / unknown never touch the config tables
        assert await ops.get_process_steps(conn, " Feedback ", "1", "mitra") == [{"name": "feedback_metadata_extraction"}]
    asyncio.run(run())


# call sites -----------------------------------------------------------------------------------

# db.pool is per event loop: the fake pool is installed inside the running loop.

def test_pending_batch_activity_uses_the_persona_steps_for_feedback_and_settings_for_story(monkeypatch):
    monkeypatch.setattr(settings, "PROCESS_CONFIG_STORY", '[{"name": "story_step"}]')
    steps = AsyncMock(return_value=[{"name": "from_config"}])
    monkeypatch.setattr(ops, "get_feedback_process_steps", steps)

    async def run():
        conn = install_fake_db(monkeypatch, activities)
        conn.fetch = AsyncMock(return_value=[
            {"submission_id": "f1", "tenant_code": "mitra", "submission_type": "feedback"},
            {"submission_id": "s1", "tenant_code": "mitra", "submission_type": "story"},
        ])
        rows = await activities.fetch_pending_submissions_activity({"limit": 10})
        assert [(r["submission_id"], r["process_steps"]) for r in rows] == [
            ("f1", [{"name": "from_config"}]), ("s1", [{"name": "story_step"}])]
        steps.assert_awaited_once_with(conn, "f1", "mitra", {})
    asyncio.run(run())


def test_failed_retry_activity_skips_a_feedback_submission_without_usable_steps(monkeypatch):
    import app.database.operations as operations
    rows = [
        {"submission_id": "f1", "tenant_code": "mitra", "submission_type": "feedback", "process_status": None,
         "session_id": "x", "updated_at": None},
        {"submission_id": "f2", "tenant_code": "mitra", "submission_type": "feedback", "process_status": None,
         "session_id": "y", "updated_at": None},
    ]
    monkeypatch.setattr(operations, "fetch_retryable_failed_submissions", AsyncMock(return_value=rows))
    monkeypatch.setattr(ops, "get_feedback_process_steps",
                        AsyncMock(side_effect=[ValueError("no config"), [{"name": "feedback_metadata_extraction"}]]))

    async def run():
        install_fake_db(monkeypatch, activities)
        out = await activities.fetch_failed_submissions_activity({})
        assert [r["submission_id"] for r in out] == ["f2"]
        assert out[0]["process_steps"] == [{"name": "feedback_metadata_extraction"}]
    asyncio.run(run())
