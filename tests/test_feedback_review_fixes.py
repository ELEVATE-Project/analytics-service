"""
Unit tests (in-memory fakes, no database, no network) of the review fixes around the per-persona feedback config:
one bad persona config must not stall other submissions (batch sweep, real-time trigger, bulk retry), the startup
check, the DLQ reason, the validators' bounds, the stage-report privacy rule and the config cache bound.
The real-Postgres counterpart is tests/test_feedback_review_fixes_pg.py.
"""
import asyncio
import json
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

import app.api.routes.uploads as uploads_routes
import app.database.feedback_config_health as health
import app.database.feedback_operations as ops
import app.kafka.consumer as consumer_module
import app.temporal.activities as activities
from app.api.models.submissions import StepRetryRequest
from app.api.services.feedback_uploads import report_unexpected_stages
from app.config import settings
from app.services.feedback_config_cache import FeedbackConfigCache
from app.services.feedback_config_validation import (
    MAX_CSV_UPLOAD_BYTES_LIMIT,
    validate_ingestion_schema_config,
    validate_max_csv_upload_bytes,
)
from app.services.feedback_constants import DEFAULT_INGESTION_SCHEMA
from seed_support import mitra_config_v1
from app.services.ingestion_validation import validate_ingestion_schema
from conftest import FakeConn, FakePool, install_fake_db
from test_feedback_consumer import _active_config, _consumer, _feedback_event, _patch_active_config, _trigger_setup
from test_feedback_uploads import make_df, new_rows

PROGRAM = "dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a"


# 1. batch sweep: one bad persona config does not starve the others ----------------------------------

def _pending_rows():
    return [
        {"submission_id": "story-old", "tenant_code": "mitra", "submission_type": "story"},
        {"submission_id": "fb-bad", "tenant_code": "mitra", "submission_type": "feedback"},
        {"submission_id": "fb-good", "tenant_code": "mitra", "submission_type": "feedback"},
    ]


def test_pending_sweep_skips_marks_failed_and_logs_a_feedback_row_whose_config_is_bad(monkeypatch, caplog):
    monkeypatch.setattr(settings, "PROCESS_CONFIG_STORY", '[{"name": "story_step"}]')
    monkeypatch.setattr(ops, "get_feedback_process_steps", AsyncMock(
        side_effect=[ValueError("Feedback config of session_type 'Youth_Voice' (mitra) is missing 'process_steps'"),
                     [{"name": "from_config"}]]))
    status = AsyncMock()
    monkeypatch.setattr(ops, "update_submission_status", status)

    async def run():
        conn = install_fake_db(monkeypatch, activities)
        conn.fetch = AsyncMock(return_value=_pending_rows())
        with caplog.at_level(logging.ERROR):
            rows = await activities.fetch_pending_submissions_activity({"limit": 10})
        # the older story row and the healthy feedback row are returned; the bad one is left out
        assert [r["submission_id"] for r in rows] == ["story-old", "fb-good"]
        # ... and it is marked failed with the reason (identifiers + config problem, no content)
        status.assert_awaited_once()
        args = status.await_args.args
        assert args[1:4] == ("fb-bad", "mitra", "failed")
        assert args[4]["status"] == "failed" and "missing 'process_steps'" in args[4]["error_message"]
        assert "retry_attempt" not in args[4]  # the bulk retry picks it up once the config is fixed
        assert "fb-bad" in caplog.text and "mitra" in caplog.text
    asyncio.run(run())


def test_pending_sweep_does_not_swallow_a_database_error(monkeypatch):
    monkeypatch.setattr(ops, "get_feedback_process_steps", AsyncMock(side_effect=RuntimeError("connection lost")))

    async def run():
        conn = install_fake_db(monkeypatch, activities)
        conn.fetch = AsyncMock(return_value=_pending_rows())
        with pytest.raises(RuntimeError, match="connection lost"):
            await activities.fetch_pending_submissions_activity({"limit": 10})
    asyncio.run(run())


def test_pending_sweep_without_limit_uses_the_batch_size_and_a_story_value_error_still_raises(monkeypatch):
    monkeypatch.setattr(settings, "BATCH_SIZE", 7)
    monkeypatch.setattr(ops, "get_process_steps", AsyncMock(side_effect=ValueError("story problem")))
    monkeypatch.setattr(activities, "get_process_steps", ops.get_process_steps)

    async def run():
        conn = install_fake_db(monkeypatch, activities)
        conn.fetch = AsyncMock(return_value=[{"submission_id": "s", "tenant_code": "mitra", "submission_type": "story"}])
        with pytest.raises(ValueError, match="story problem"):  # story behaviour unchanged
            await activities.fetch_pending_submissions_activity(None)
        assert conn.fetch.await_args.args[1] == 7
    asyncio.run(run())


# 11. the persona lookup is memoized per call --------------------------------------------------------

def _persona_conn(config):
    """fetchrow answers the submission lookup, then (at most once per persona) the config lookup."""
    conn = FakeConn()
    calls = {"submission": 0, "config": 0}

    async def fetchrow(sql, *args):
        if "FROM feedback_submissions" in sql:
            calls["submission"] += 1
            return {"session_type": "Youth_Voice", "source": "mitra", "program_id": PROGRAM}
        calls["config"] += 1
        return None if config is None else {"version": 1, "config": config}

    conn.fetchrow = fetchrow
    conn.calls = calls
    return conn


def test_the_persona_config_is_read_once_per_sweep_and_not_shared_between_calls():
    conn = _persona_conn({**mitra_config_v1("Youth"), "process_steps": [{"name": "feedback_metadata_extraction"}]})

    async def run():
        memo = {}
        for sid in ("1", "2", "3"):
            assert await ops.get_feedback_process_steps(conn, sid, "mitra", memo) == [{"name": "feedback_metadata_extraction"}]
        assert conn.calls == {"submission": 3, "config": 1}
        # a result is a copy: changing it does not change what the next row gets
        first = await ops.get_feedback_process_steps(conn, "4", "mitra", memo)
        first.append({"name": "x"})
        assert await ops.get_feedback_process_steps(conn, "5", "mitra", memo) == [{"name": "feedback_metadata_extraction"}]
        # no memo: nothing is remembered
        await ops.get_feedback_process_steps(conn, "6", "mitra")
        await ops.get_feedback_process_steps(conn, "7", "mitra")
        assert conn.calls["config"] == 3
    asyncio.run(run())


def test_a_persona_config_error_is_memoized_too_and_names_each_submission():
    conn = _persona_conn(None)

    async def run():
        memo = {}
        for sid in ("1", "2"):
            with pytest.raises(ValueError, match=f"No active mitra feedback_upload config.*submission '{sid}'"):
                await ops.get_feedback_process_steps(conn, sid, "mitra", memo)
        assert conn.calls["config"] == 1
    asyncio.run(run())


class _CountingPool(FakePool):
    def __init__(self, conn):
        super().__init__(conn)
        self.acquired = 0

    def acquire(self):
        self.acquired += 1
        return self.conn


def test_bulk_retry_sweep_only_takes_a_connection_for_feedback_rows_and_reads_each_persona_once(monkeypatch):
    rows = [
        {"submission_id": "s1", "session_id": "x", "tenant_code": "mitra", "submission_type": "story",
         "process_status": None, "updated_at": 1},
        {"submission_id": "f1", "session_id": "x", "tenant_code": "mitra", "submission_type": "feedback",
         "process_status": None, "updated_at": 2},
        {"submission_id": "f2", "session_id": "y", "tenant_code": "mitra", "submission_type": "feedback",
         "process_status": None, "updated_at": 3},
    ]
    monkeypatch.setattr(settings, "PROCESS_CONFIG_STORY", '[{"name": "story_step"}]')
    monkeypatch.setattr(settings, "BATCH_SIZE", 100)
    monkeypatch.setattr(uploads_routes, "fetch_retryable_failed_submissions", AsyncMock(side_effect=[rows]))
    client = MagicMock()
    client.start_workflow = AsyncMock()
    monkeypatch.setattr(uploads_routes, "get_temporal_client", AsyncMock(return_value=client))
    conn = _persona_conn({**mitra_config_v1("Youth"), "process_steps": [{"name": "feedback_metadata_extraction"}]})
    pool = _CountingPool(conn)

    async def run():
        monkeypatch.setattr(uploads_routes.db, "pool", pool, raising=False)  # db.pool is per event loop
        await uploads_routes._dispatch_all_retry_workflows("mitra", "2026-01-01T00:00:00+00:00", "sweep")
    asyncio.run(run())

    assert pool.acquired == 2          # one for the page of rows, one for the config lookups of the feedback rows
    assert conn.calls["config"] == 1   # f1 and f2 share a persona
    assert client.start_workflow.await_count == 3


def test_bulk_retry_sweep_with_only_story_rows_takes_one_connection(monkeypatch):
    rows = [{"submission_id": "s1", "session_id": "x", "tenant_code": "mitra", "submission_type": "story",
             "process_status": None, "updated_at": 1}]
    monkeypatch.setattr(settings, "PROCESS_CONFIG_STORY", '[{"name": "story_step"}]')
    monkeypatch.setattr(settings, "BATCH_SIZE", 100)
    monkeypatch.setattr(uploads_routes, "fetch_retryable_failed_submissions", AsyncMock(side_effect=[rows]))
    client = MagicMock()
    client.start_workflow = AsyncMock()
    monkeypatch.setattr(uploads_routes, "get_temporal_client", AsyncMock(return_value=client))
    pool = _CountingPool(FakeConn())

    async def run():
        monkeypatch.setattr(uploads_routes.db, "pool", pool, raising=False)
        await uploads_routes._dispatch_all_retry_workflows("mitra", "2026-01-01T00:00:00+00:00", "sweep")
    asyncio.run(run())
    assert pool.acquired == 1 and client.start_workflow.await_count == 1


# 8. retry-step: a server-side config problem is a 409 -------------------------------------------------

def test_retry_step_returns_409_when_the_persona_config_cannot_give_steps(monkeypatch):
    row = {"submission_id": "f1", "session_id": "x", "status": "failed", "submission_type": "feedback",
           "process_status": None}
    monkeypatch.setattr(uploads_routes, "fetch_failed_submission_for_retry", AsyncMock(return_value=row))
    monkeypatch.setattr(uploads_routes, "get_process_steps", AsyncMock(side_effect=ValueError("no config")))
    body = StepRetryRequest(submission_id="f1", session_id="x", tenant_code="mitra")

    async def run():
        install_fake_db(monkeypatch, uploads_routes)
        await uploads_routes.retry_step(body, None)
    with pytest.raises(HTTPException) as info:
        asyncio.run(run())
    assert info.value.status_code == 409
    assert "Cannot resolve the process steps of submission 'f1': no config" in info.value.detail


# 2. real-time trigger: a bad config marks the row failed, a database error propagates -------------------

def test_realtime_trigger_marks_the_stored_row_failed_when_the_config_is_bad(monkeypatch, caplog):
    async def run():
        consumer, conn, status_mock = _trigger_setup(monkeypatch, [])
        consumer.steps_mock.side_effect = ValueError("missing 'process_steps'")
        failed = AsyncMock()
        monkeypatch.setattr(consumer_module.feedback_operations, "update_submission_status", failed)
        with caplog.at_level(logging.ERROR):
            await consumer._trigger_realtime_workflow("184884", "mitra", "feedback")  # does not raise: no retry / DLQ
        failed.assert_awaited_once()
        assert failed.await_args.args[1:4] == ("184884", "mitra", "failed")
        assert "missing 'process_steps'" in failed.await_args.args[4]["error_message"]
        assert "184884" in caplog.text and "mitra" in caplog.text
        status_mock.assert_not_awaited()
        consumer.temporal_client.start_workflow.assert_not_awaited()
    asyncio.run(run())


def test_realtime_trigger_lets_a_database_error_propagate_for_the_retry(monkeypatch):
    async def run():
        consumer, _, _ = _trigger_setup(monkeypatch, [])
        consumer.steps_mock.side_effect = ConnectionError("db down")
        with pytest.raises(ConnectionError):
            await consumer._trigger_realtime_workflow("184884", "mitra", "feedback")
    asyncio.run(run())


# 3. DLQ reason: operator error, replayable after the migrate -----------------------------------------------

@pytest.mark.parametrize("loader", [
    AsyncMock(return_value=None),
    AsyncMock(side_effect=ValueError("Feedback config of session_type 'Youth_Voice' (mitra) is missing 'ingestion_schema'")),
])
def test_dlq_reason_for_a_missing_or_invalid_config_says_it_can_be_replayed_after_migrate(monkeypatch, loader):
    async def run():
        consumer, _, insert_mock, _, _, dlq_mock = _consumer(monkeypatch)
        _patch_active_config(monkeypatch, loader=loader)
        await consumer.process_message(json.dumps(_feedback_event()))
        insert_mock.assert_not_awaited()
        args = dlq_mock.await_args.args
        assert "python tools/feedback_config.py migrate" in args[1] and "replay" in args[1]
        assert args[2] == {"submissionId": "184884", "tenantCode": "mitra", "sessionId": "s" * 32}
    asyncio.run(run())


def test_a_genuinely_bad_event_does_not_get_the_operator_hint(monkeypatch):
    async def run():
        consumer, _, _, _, _, dlq_mock = _consumer(monkeypatch)
        consumer_module.validate_ingestion_schema.return_value = ["'data.language' is missing"]
        await consumer.process_message(json.dumps(_feedback_event()))
        assert "feedback_config.py migrate" not in dlq_mock.await_args.args[1]
    asyncio.run(run())


# 13. tags.programId must be a UUID ---------------------------------------------------------------------------

def test_non_uuid_program_id_goes_straight_to_the_dlq_with_identifiers_and_no_config_query(monkeypatch):
    async def run():
        consumer, _, insert_mock, _, _, dlq_mock = _consumer(monkeypatch)
        calls = _patch_active_config(monkeypatch)
        event = _feedback_event()
        event["tags"]["programId"] = "not-a-uuid"
        await consumer.process_message(json.dumps(event))
        assert calls == []  # the lookup (a uuid column: asyncpg.DataError) is never attempted
        insert_mock.assert_not_awaited()
        args = dlq_mock.await_args.args
        assert "'tags.programId' must be a UUID" in args[1]
        assert args[2] == {"submissionId": "184884", "tenantCode": "mitra", "sessionId": "s" * 32}
    asyncio.run(run())


def test_validator_rejects_a_non_uuid_program_id_even_with_a_schema(monkeypatch):
    event = _feedback_event()
    event["tags"]["programId"] = "12345"
    problems = validate_ingestion_schema(event, "feedback", "create", feedback_schema=DEFAULT_INGESTION_SCHEMA)
    assert any("'tags.programId' must be a UUID" in p for p in problems)
    event["tags"]["programId"] = PROGRAM
    assert not any("tags.programId" in p for p in
                   validate_ingestion_schema(event, "feedback", "create", feedback_schema=DEFAULT_INGESTION_SCHEMA))


# 5. unsupported eventType message ---------------------------------------------------------------------------------

@pytest.mark.parametrize("schema", [None, DEFAULT_INGESTION_SCHEMA])
def test_unsupported_feedback_event_type_names_the_missing_section(schema):
    problems = validate_ingestion_schema(_feedback_event("foo"), "feedback", "foo", feedback_schema=schema)
    assert problems == ["No ingestion schema section defined for eventType 'foo'"]


# 6 + 7. validators ------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("missing", ["submissionId", "tenantCode"])
def test_delete_section_must_require_submission_id_and_tenant_code(missing):
    schema = json.loads(json.dumps(DEFAULT_INGESTION_SCHEMA))
    schema["delete"]["required"].remove(missing)
    with pytest.raises(ValueError, match=f"delete.required' must include: {missing}"):
        validate_ingestion_schema_config(schema)


def test_delete_section_without_a_required_list_is_rejected_and_extra_paths_are_allowed():
    schema = json.loads(json.dumps(DEFAULT_INGESTION_SCHEMA))
    schema["delete"]["required"].append("sessionId")
    assert validate_ingestion_schema_config(schema) is schema
    del schema["delete"]["required"]
    with pytest.raises(ValueError, match="delete' needs a 'required' list"):
        validate_ingestion_schema_config(schema)


def test_max_csv_upload_bytes_has_an_upper_bound():
    assert MAX_CSV_UPLOAD_BYTES_LIMIT == 2 ** 40
    assert validate_max_csv_upload_bytes(2 ** 40) == 2 ** 40
    with pytest.raises(ValueError, match="at most"):
        validate_max_csv_upload_bytes(2 ** 40 + 1)
    with pytest.raises(ValueError, match="at most"):
        validate_max_csv_upload_bytes(10 ** 30)  # file.read(cap + 1) would overflow


# 9. stage report privacy -------------------------------------------------------------------------------------------

def test_stage_report_never_lists_text_that_is_not_a_stage_key():
    config = {**mitra_config_v1("Youth"), "expect_stages": ["STATE_IDENTIFICATION"]}
    leaked = "My name is Asha and I live near the old mill"
    rows = new_rows([("O", "", "hello", "hello"), ("B", "STATE_IDENTIFICATION", "q", "q"),
                     ("R", leaked, "a", "a"), ("B", "MYSTERY_STAGE", "q2", "q2"),
                     ("R", "yes, 12 years", "a2", "a2")], submission_id="100", session="a" * 32)
    report = report_unexpected_stages(make_df(rows), config)
    flat = json.dumps(report)
    assert "Asha" not in flat and "mill" not in flat and "12 years" not in flat
    assert report["unexpected_stages"] == {"MYSTERY_STAGE": 1}
    assert report["unexpected_stage_count"] == 1
    assert report["other_stage_values"] == 2              # a count, no text
    assert report["sessions_with_unexpected_stages"] == 1


def test_stage_report_key_pattern_boundaries_and_no_counter_when_all_are_keys():
    config = {**mitra_config_v1("Youth"), "expect_stages": ["X"]}
    ok, too_long = "A" * 64, "A" * 65
    rows = new_rows([("O", "", "h", "h"), ("B", ok, "q", "q"), ("B", too_long, "q", "q"), ("B", "HAS-DASH", "q", "q")],
                    submission_id="1", session="a" * 32)
    report = report_unexpected_stages(make_df(rows), config)
    assert list(report["unexpected_stages"]) == [ok] and report["other_stage_values"] == 2
    rows = new_rows([("O", "", "h", "h"), ("B", "KEY_ONE", "q", "q")], submission_id="2", session="b" * 32)
    assert "other_stage_values" not in report_unexpected_stages(make_df(rows), config)


# 12. cache bound ----------------------------------------------------------------------------------------------------

def test_config_cache_is_bounded_and_evicts_the_oldest_entries():
    async def run():
        loader = AsyncMock(side_effect=lambda t, p, s, src: {"config_version_id": f"{t}-{p}"})
        cache = FeedbackConfigCache(loader, ttl_seconds=60, max_entries=3)
        for n in range(5):
            await cache.get("t", str(n), "S", "mitra")
        assert len(cache._entries) == 3 and cache.evictions == 2
        assert list(k[1] for k in cache._entries) == ["2", "3", "4"]  # the oldest two were evicted
        loads = cache.loads
        await cache.get("t", "4", "S", "mitra")      # still cached
        assert cache.loads == loads
        await cache.get("t", "0", "S", "mitra")      # evicted: loaded again
        assert cache.loads == loads + 1
        # errors and misses count against the same bound
        bad = FeedbackConfigCache(AsyncMock(side_effect=ValueError("bad")), ttl_seconds=60, max_entries=2)
        for n in range(4):
            with pytest.raises(ValueError):
                await bad.get("t", str(n), "S", "mitra")
        assert len(bad._entries) == 2
    asyncio.run(run())


def test_config_cache_default_bound_is_1024_and_must_be_positive():
    assert FeedbackConfigCache(AsyncMock())._max_entries == 1024
    with pytest.raises(ValueError):
        FeedbackConfigCache(AsyncMock(), max_entries=0)


# 3. startup check ------------------------------------------------------------------------------------------------------

def _config_rows(*configs):
    return [{"session_type": st, "config_source": "mitra", "tenant_code": "mitra", "config": cfg}
            for st, cfg in configs]


def test_startup_check_logs_an_error_per_invalid_config_and_nothing_for_valid_ones(caplog):
    old = mitra_config_v1("Parent")
    for key in ("max_csv_upload_bytes", "ingestion_schema", "process_steps"):
        del old[key]
    bad_type = {**mitra_config_v1("Educator"), "max_csv_upload_bytes": "100"}
    conn = FakeConn()
    conn.fetch = AsyncMock(return_value=_config_rows(
        ("Youth_Voice", mitra_config_v1("Youth")), ("Parent_Voice", old), ("Educator_Voice", json.dumps(bad_type))))
    with caplog.at_level(logging.ERROR):
        invalid = asyncio.run(health.check_feedback_configs(conn))
    assert [i["session_type"] for i in invalid] == ["Parent_Voice", "Educator_Voice"]
    assert set(invalid[0]["problems"]) == {"max_csv_upload_bytes", "ingestion_schema", "process_steps"}
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 2
    assert "Parent_Voice" in errors[0].getMessage() and "feedback_config.py migrate" in errors[0].getMessage()
    assert "Youth_Voice" not in caplog.text


def test_startup_check_never_raises(caplog):
    conn = FakeConn()
    conn.fetch = AsyncMock(side_effect=RuntimeError("relation config does not exist"))
    with caplog.at_level(logging.WARNING):
        asyncio.run(health.run_startup_checks(conn))
    assert "could not run" in caplog.text


def test_removed_env_vars_are_warned_by_name_only(tmp_path, caplog):
    env_file = tmp_path / ".env"
    env_file.write_text("PROCESS_CONFIG_FEEDBACK=[secret-steps]\nOTHER=1\nFEEDBACK_KAFKA_SCHEMA={\"x\": \"secret-schema\"}\n")
    environ = {"FEEDBACK_MAX_CSV_UPLOAD_BYTES": "secret-123", "PATH": "/bin"}
    with caplog.at_level(logging.WARNING):
        names = health.warn_removed_env_vars(environ, env_file)
    assert sorted(names) == ["FEEDBACK_KAFKA_SCHEMA", "FEEDBACK_MAX_CSV_UPLOAD_BYTES", "PROCESS_CONFIG_FEEDBACK"]
    for name in names:
        assert name in caplog.text
    for value in ("secret-steps", "secret-schema", "secret-123"):
        assert value not in caplog.text


def test_no_warning_when_the_removed_env_vars_are_absent(tmp_path, caplog):
    env_file = tmp_path / ".env"
    env_file.write_text("OTHER=1\n")
    with caplog.at_level(logging.WARNING):
        assert health.warn_removed_env_vars({"PATH": "/bin"}, env_file) == []
        assert health.warn_removed_env_vars({}, tmp_path / "missing.env") == []
    assert caplog.text == ""


def test_initialize_schema_runs_the_startup_check_after_the_seeds():
    import inspect
    from app.database import db as db_module
    source = inspect.getsource(db_module.Database.initialize_schema)
    assert source.index("await conn.execute(seed_config_sql)") < source.index("await check_feedback_config_health(conn)")
