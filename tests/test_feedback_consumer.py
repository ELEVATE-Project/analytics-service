"""
Unit tests (in-memory fakes only) for the feedback consumer + DB-write branch:
branch selection, transaction use, DLQ on update, create-reconcile (no duplicate skip), the
success-at-ingest rule, and that story/discussion paths are untouched. The real-Postgres behaviour
(reconcile, no-downgrade, parent links, cascade) is covered by tests/test_feedback_consumer_pg.py.
"""
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import json5
import pytest

import app.kafka.consumer as consumer_module
from app.database import operations
from app.database.operations import FeedbackUpdateNotSupportedError, insert_or_update_submission
from conftest import FakeConn, install_fake_db

FIXTURES = Path(__file__).resolve().parent / "kafka_events"
PROGRAM_ID = "dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a"
LEADER_ID = "5f3d1f0f-f472-47af-8c55-66e10aa48c43"


def _fixture(*parts):
    return json5.loads(FIXTURES.joinpath(*parts).read_text(encoding="utf-8"))


def _feedback_event(event_type="create", **overrides):
    event = {
        "submissionId": "184884", "submissionType": "feedback", "sessionId": "s" * 32,
        "tenantCode": "mitra", "eventType": event_type, "eventPublishedAt": "2026-10-15T08:54:43.000Z",
        "tags": {"programId": PROGRAM_ID, "programName": "1M Voice", "leaderCategoryId": LEADER_ID,
                 "leaderCategoryName": "Youth", "state": None, "district": None, "source": "mitra"},
        "data": {
            "source": "mitra", "language": "hi", "completionStatus": "IN_PROGRESS",
            "submissionDate": "2026-10-15T08:54:43.000Z",
            "messages": [
                {"sequence": 1, "questionKey": "AGE_IDENTIFICATION", "isFollowUp": False, "message": "Age?",
                 "translatedMessage": None, "messageAt": "2026-10-15T08:54:43.000Z", "parentSequence": None},
                {"sequence": 2, "questionKey": None, "isFollowUp": False, "message": "17",
                 "translatedMessage": None, "messageAt": None, "parentSequence": 1},
            ],
        },
    }
    event.update(overrides)
    return event


# consumer helpers -----------------------------------------------------------------------------

def _consumer(monkeypatch, insert_result=None, mode="real-time"):
    consumer = consumer_module.IngestionConsumer()
    conn = install_fake_db(monkeypatch, consumer_module)
    conn.fetchval.return_value = 1  # "a submission with this id already exists"
    insert_mock = AsyncMock(return_value=insert_result or {"id": "u", "status": "pending", "changed": True})
    delete_mock = AsyncMock(return_value=True)
    trigger_mock = AsyncMock()
    dlq_mock = AsyncMock()
    monkeypatch.setattr(consumer_module, "insert_or_update_submission", insert_mock)
    monkeypatch.setattr(consumer_module, "delete_submission", delete_mock)
    # the validation layer is owned elsewhere: these tests are about what the consumer does after it passes
    monkeypatch.setattr(consumer_module, "validate_ingestion_schema", MagicMock(return_value=[]))
    monkeypatch.setattr(consumer, "_trigger_realtime_workflow", trigger_mock)
    monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
    monkeypatch.setattr(consumer_module.settings, "PROCESSING_MODE", mode)
    return consumer, conn, insert_mock, delete_mock, trigger_mock, dlq_mock


def test_feedback_create_on_existing_session_is_not_skipped_as_duplicate(monkeypatch):
    async def run():
        consumer, conn, insert_mock, _, trigger_mock, dlq_mock = _consumer(monkeypatch)
        await consumer.process_message(json.dumps(_feedback_event()))
        insert_mock.assert_awaited_once()  # reconciled, even though "the row exists"
        conn.fetchval.assert_not_awaited()  # no duplicate pre-check for feedback
        trigger_mock.assert_awaited_once_with("184884", "mitra", "feedback")
        dlq_mock.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize("fixture", ["create_story.json", "create_discussion.json"])
def test_story_and_discussion_create_still_skip_duplicates(monkeypatch, fixture):
    async def run():
        consumer, conn, insert_mock, _, trigger_mock, _ = _consumer(monkeypatch)
        await consumer.process_message(json.dumps(_fixture("create", fixture)))
        conn.fetchval.assert_awaited_once()
        insert_mock.assert_not_awaited()
        trigger_mock.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize("fixture", ["create_story.json", "create_discussion.json"])
def test_story_and_discussion_new_create_unchanged_flow(monkeypatch, fixture):
    async def run():
        consumer, conn, insert_mock, _, trigger_mock, _ = _consumer(monkeypatch)
        conn.fetchval.return_value = None
        event = _fixture("create", fixture)
        await consumer.process_message(json.dumps(event))
        insert_mock.assert_awaited_once()
        trigger_mock.assert_awaited_once_with(str(event["submissionId"]), event["tenantCode"], event["submissionType"])
    asyncio.run(run())


def test_feedback_unchanged_redelivery_does_not_trigger_processing(monkeypatch):
    async def run():
        consumer, _, insert_mock, _, trigger_mock, dlq_mock = _consumer(
            monkeypatch, insert_result={"id": "u", "status": "success", "changed": False})
        await consumer.process_message(json.dumps(_feedback_event()))
        insert_mock.assert_awaited_once()
        trigger_mock.assert_not_awaited()
        dlq_mock.assert_not_awaited()
    asyncio.run(run())


def test_feedback_unchanged_but_still_pending_is_processed(monkeypatch):
    async def run():
        consumer, _, _, _, trigger_mock, _ = _consumer(
            monkeypatch, insert_result={"id": "u", "status": "pending", "changed": False})
        await consumer.process_message(json.dumps(_feedback_event()))
        trigger_mock.assert_awaited_once()
    asyncio.run(run())


def test_feedback_create_in_batch_mode_only_queues(monkeypatch):
    async def run():
        consumer, _, insert_mock, _, trigger_mock, _ = _consumer(monkeypatch, mode="batch")
        await consumer.process_message(json.dumps(_feedback_event()))
        insert_mock.assert_awaited_once()
        trigger_mock.assert_not_awaited()
    asyncio.run(run())


def test_feedback_update_goes_to_dlq_with_identifiers_and_writes_nothing(monkeypatch):
    async def run():
        consumer, _, insert_mock, delete_mock, trigger_mock, dlq_mock = _consumer(monkeypatch)
        event = _feedback_event("update")
        raw = json.dumps(event)
        await consumer.process_message(raw)
        insert_mock.assert_not_awaited()
        trigger_mock.assert_not_awaited()
        delete_mock.assert_not_awaited()
        dlq_mock.assert_awaited_once()
        args = dlq_mock.await_args.args
        assert args[0] == raw
        assert "not supported" in args[1] and "update" in args[1]
        assert args[2] == {"submissionId": "184884", "tenantCode": "mitra", "sessionId": "s" * 32}
    asyncio.run(run())


def test_feedback_delete_uses_delete_submission(monkeypatch):
    async def run():
        consumer, _, insert_mock, delete_mock, trigger_mock, dlq_mock = _consumer(monkeypatch)
        await consumer.process_message(json.dumps(_feedback_event("delete")))
        delete_mock.assert_awaited_once()
        assert delete_mock.await_args.args[1:] == ("184884", "mitra")
        insert_mock.assert_not_awaited()
        trigger_mock.assert_not_awaited()
        dlq_mock.assert_not_awaited()
    asyncio.run(run())


def test_story_update_is_not_dlqd(monkeypatch):
    async def run():
        consumer, _, insert_mock, _, trigger_mock, dlq_mock = _consumer(monkeypatch)
        await consumer.process_message(json.dumps(_fixture("update", "update_story.json")))
        insert_mock.assert_awaited_once()
        trigger_mock.assert_awaited_once()
        dlq_mock.assert_not_awaited()
    asyncio.run(run())


# success-at-ingest ----------------------------------------------------------------------------

def _trigger_setup(monkeypatch, process_config):
    """process_config is applied to every type via the real settings.get_process_config (JSON env fields)."""
    consumer = consumer_module.IngestionConsumer()
    conn = install_fake_db(monkeypatch, consumer_module)
    consumer.temporal_client = MagicMock()
    consumer.temporal_client.start_workflow = AsyncMock()
    raw = json.dumps(process_config)
    for field in ("PROCESS_CONFIG_STORY", "PROCESS_CONFIG_DISCUSSION", "PROCESS_CONFIG_FEEDBACK"):
        monkeypatch.setattr(consumer_module.settings, field, raw)
    status_mock = AsyncMock()
    monkeypatch.setattr(consumer_module, "update_submission_status", status_mock)
    return consumer, conn, status_mock


def test_feedback_without_process_steps_is_marked_success_at_ingestion(monkeypatch):
    async def run():
        consumer, conn, status_mock = _trigger_setup(monkeypatch, [])
        consumer.temporal_client = None  # no Temporal needed (and none is contacted)
        monkeypatch.setattr(consumer_module.Client, "connect", AsyncMock(side_effect=AssertionError("Temporal contacted")))
        await consumer._trigger_realtime_workflow("184884", "mitra", "feedback")
        status_mock.assert_awaited_once()
        args = status_mock.await_args.args
        assert args[1:4] == ("184884", "mitra", "success")
        assert args[4]["status"] == "success" and args[4]["steps"] == []
    asyncio.run(run())


@pytest.mark.parametrize("sub_type", ["story", "discussion"])
def test_story_and_discussion_without_process_steps_still_left_pending(monkeypatch, sub_type):
    async def run():
        consumer, _, status_mock = _trigger_setup(monkeypatch, [])
        await consumer._trigger_realtime_workflow("1", "mitra", sub_type)
        status_mock.assert_not_awaited()
        consumer.temporal_client.start_workflow.assert_not_awaited()
    asyncio.run(run())


def test_feedback_with_process_steps_starts_workflow_not_success(monkeypatch):
    async def run():
        steps = [{"name": "pii_and_abusive"}]
        consumer, _, status_mock = _trigger_setup(monkeypatch, steps)
        await consumer._trigger_realtime_workflow("184884", "mitra", "feedback")
        consumer.temporal_client.start_workflow.assert_awaited_once()
        # only the 'processing' transition, never a direct 'success'
        assert [c.args[3] for c in status_mock.await_args_list] == ["processing"]
    asyncio.run(run())


# insert_or_update_submission branch selection -------------------------------------------------

def _ops_conn():
    conn = FakeConn()
    conn.fetchrow = AsyncMock(side_effect=[
        {"id": "sub-uuid", "status": "pending", "session_id": "s" * 32, "inserted": True},  # submissions
        {"completion_status": "IN_PROGRESS", "inserted": True},                              # feedback_submissions
    ])
    conn.fetch = AsyncMock(side_effect=[
        [{"sequence": 1, "inserted": True}, {"sequence": 2, "inserted": True}],  # feedbacks upsert
        [],                                                                      # unresolved parents
        [{"id": "child"}],                                                       # parent_id update
    ])
    return conn


def test_feedback_branch_runs_in_one_transaction_and_writes_feedback_tables():
    async def run():
        conn = _ops_conn()
        res = await insert_or_update_submission(conn, _feedback_event())
        conn.transaction.assert_called_once()
        sqls = [c.args[0] for c in conn.fetchrow.await_args_list + conn.fetch.await_args_list]
        assert any("INSERT INTO submissions" in s for s in sqls)
        assert any("INSERT INTO feedback_submissions" in s for s in sqls)
        assert any("INSERT INTO feedbacks" in s and "unnest" in s for s in sqls)  # one batched statement
        assert sum("INSERT INTO feedbacks" in s for s in sqls) == 1
        assert any("UPDATE feedbacks" in s and "parent_id" in s for s in sqls)
        assert not any("story_submissions" in s or "discussion_submissions" in s for s in sqls)
        assert res["changed"] is True and res["status"] == "pending"
        assert res["messages_inserted"] == 2 and res["parents_set"] == 1
        # submissions row is typed 'feedback'; state/district stay NULL when the tags carry none
        sub_args = conn.fetchrow.await_args_list[0].args
        assert sub_args[4] == "feedback" and sub_args[5] is None and sub_args[6] is None
    asyncio.run(run())


def test_feedback_nothing_written_reports_unchanged():
    async def run():
        conn = FakeConn()
        conn.fetchrow = AsyncMock(side_effect=[
            None,  # submissions upsert matched but changed nothing -> no RETURNING row
            {"id": "sub-uuid", "status": "success", "session_id": "s" * 32, "inserted": False},  # re-read
            None,  # feedback_submissions unchanged
        ])
        conn.fetch = AsyncMock(side_effect=[[], [], []])
        res = await insert_or_update_submission(conn, _feedback_event())
        assert res["changed"] is False and res["status"] == "success"
        # no status reset when nothing changed
        assert not any("SET status = 'pending'" in c.args[0] for c in conn.execute.await_args_list)
    asyncio.run(run())


def test_feedback_content_change_resets_status_to_pending():
    async def run():
        conn = FakeConn()
        conn.fetchrow = AsyncMock(side_effect=[
            None,
            {"id": "sub-uuid", "status": "success", "session_id": "s" * 32, "inserted": False},
            None,
        ])
        conn.fetch = AsyncMock(side_effect=[[{"sequence": 2, "inserted": True}], [], []])
        res = await insert_or_update_submission(conn, _feedback_event())
        assert res["changed"] is True and res["status"] == "pending"
        assert any("SET status = 'pending'" in c.args[0] for c in conn.execute.await_args_list)
    asyncio.run(run())


def test_feedback_update_event_raises_before_any_write():
    async def run():
        conn = FakeConn()
        with pytest.raises(FeedbackUpdateNotSupportedError) as exc:
            await insert_or_update_submission(conn, _feedback_event("update"))
        assert "184884" in str(exc.value) and "mitra" in str(exc.value)
        conn.transaction.assert_not_called()
        conn.execute.assert_not_awaited()
        conn.fetchrow.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize("mutate, needle", [
    (lambda e: e["data"].update(messages=[]), "non-empty list"),
    (lambda e: e["data"]["messages"][1].update(message="   "), "message must be a non-empty string"),
    (lambda e: e["data"]["messages"][1].update(sequence=1), "more than once"),
    (lambda e: e["data"]["messages"][1].update(parentSequence=2), "lower than its own sequence"),
    (lambda e: e["data"]["messages"][1].update(parentSequence="1"), "parentSequence must be an integer"),
    (lambda e: e["data"].update(completionStatus="DONE"), "completionStatus"),
    (lambda e: e["data"].update(source="csv"), "data.source"),
    (lambda e: e.pop("sessionId"), "sessionId"),
])
def test_feedback_branch_rejects_malformed_events_before_writing(mutate, needle):
    async def run():
        conn = FakeConn()
        event = _feedback_event()
        mutate(event)
        with pytest.raises(ValueError, match=needle):
            await insert_or_update_submission(conn, event)
        conn.execute.assert_not_awaited()
        conn.fetchrow.assert_not_awaited()
    asyncio.run(run())


def test_story_and_discussion_never_take_the_feedback_branch(monkeypatch):
    async def run():
        feedback_branch = AsyncMock()
        monkeypatch.setattr(operations, "_ingest_feedback_submission", feedback_branch)
        for fixture in ("create_story.json", "create_discussion.json"):
            conn = FakeConn()
            conn.fetchrow.return_value = {"id": "u", "status": "pending"}
            await insert_or_update_submission(conn, _fixture("create", fixture))
            sqls = [c.args[0] for c in conn.execute.await_args_list]
            assert any(("story_submissions" in s) or ("discussion_submissions" in s) for s in sqls)
            assert not any("feedback" in s for s in sqls)
        feedback_branch.assert_not_awaited()
        await insert_or_update_submission(_ops_conn(), _feedback_event())  # sanity: stub not hit via real path
    asyncio.run(run())


def test_is_feedback_type():
    assert operations.is_feedback_type("feedback") and operations.is_feedback_type(" Feedback ")
    assert not operations.is_feedback_type("story") and not operations.is_feedback_type("discussion")
    assert not operations.is_feedback_type(None)


# Google Forms (Phase 2, slice 2c) -------------------------------------------------------------

GF_ID = "6f1d2c3a-8b4e-4f7a-9c1d-2e5b7a9f0c11"
GF_REF = "2026-10-02T10:09:20:" + "ab" * 32
STORED_ID = "0e9c8a77-1d2b-4c3d-8e4f-5a6b7c8d9e0f"


def _gforms_event(**data_overrides):
    event = _feedback_event(submissionId=GF_ID, sessionId=GF_ID)
    event["tags"]["source"] = "google_forms"
    event["data"].update(source="google_forms", language="en", completionStatus="COMPLETED", sourceRef=GF_REF)
    event["data"].update(data_overrides)
    return event


def _stored_row(status="success"):
    return {"id": "stored-uuid", "submission_id": STORED_ID, "tenant_code": "mitra", "status": status}


def _violation(constraint="feedback_submissions_tenant_code_source_ref_key", table="feedback_submissions"):
    return asyncpg.exceptions.UniqueViolationError.new(
        {"M": "duplicate key value violates unique constraint", "n": constraint, "t": table, "C": "23505"})


def _gforms_new_conn():
    conn = FakeConn()
    conn.fetchrow = AsyncMock(side_effect=[
        None,                                                                              # lookup by source_ref: new
        {"id": "sub-uuid", "status": "pending", "session_id": GF_ID, "inserted": True},    # submissions
        {"completion_status": "COMPLETED", "inserted": True},                              # feedback_submissions
    ])
    conn.fetchval = AsyncMock(return_value=None)                                           # submission id not taken
    conn.fetch = AsyncMock(side_effect=[
        [{"sequence": 1, "inserted": True}, {"sequence": 2, "inserted": True}], [], [{"id": "child"}],
    ])
    return conn


def test_gforms_new_response_is_looked_up_then_stored_with_source_ref():
    async def run():
        conn = _gforms_new_conn()
        res = await insert_or_update_submission(conn, _gforms_event())
        first_sql = conn.fetchrow.await_args_list[0].args[0]
        assert "feedback_submissions" in first_sql and "source_ref" in first_sql
        assert conn.fetchrow.await_args_list[0].args[1:] == ("mitra", GF_REF)
        fs_args = conn.fetchrow.await_args_list[2].args
        assert "INSERT INTO feedback_submissions" in fs_args[0] and "source_ref" in fs_args[0]
        assert fs_args[3] == "google_forms" and fs_args[-1] == GF_REF
        assert res["changed"] is True and res["submission_id"] == GF_ID and res["messages_inserted"] == 2
        conn.transaction.assert_called_once()
    asyncio.run(run())


def test_mitra_event_writes_null_source_ref():
    async def run():
        conn = _ops_conn()
        await insert_or_update_submission(conn, _feedback_event())
        fs_args = conn.fetchrow.await_args_list[1].args
        assert "INSERT INTO feedback_submissions" in fs_args[0] and fs_args[-1] is None
        conn.fetchval.assert_not_awaited()  # no google_forms lookup for mitra
    asyncio.run(run())


def test_gforms_already_stored_response_is_a_noop_with_stored_identity():
    async def run():
        conn = FakeConn()
        conn.fetchrow = AsyncMock(return_value=_stored_row("success"))
        res = await insert_or_update_submission(conn, _gforms_event())  # event carries a different, fresh uuid
        assert res == {"id": "stored-uuid", "submission_id": STORED_ID, "tenant_code": "mitra", "status": "success",
                       "changed": False, "messages_inserted": 0, "messages_updated": 0, "parents_set": 0}
        conn.transaction.assert_not_called()
        conn.execute.assert_not_awaited()
        conn.fetch.assert_not_awaited()
        assert conn.fetchrow.await_count == 1
    asyncio.run(run())


def test_gforms_new_ref_with_taken_submission_id_is_rejected_before_writing():
    async def run():
        conn = FakeConn()
        conn.fetchrow = AsyncMock(return_value=None)
        conn.fetchval = AsyncMock(return_value=1)
        with pytest.raises(ValueError, match="never updates an existing submission"):
            await insert_or_update_submission(conn, _gforms_event())
        conn.transaction.assert_not_called()
        conn.execute.assert_not_awaited()
    asyncio.run(run())


def test_gforms_lost_race_unique_violation_is_a_clean_noop():
    async def run():
        conn = FakeConn()
        # asyncpg raises on the losing insert, so an item that is an exception is raised rather than returned
        async def fetchrow(*args, **kwargs):
            item = fetchrow.items.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        fetchrow.items = [None, {"id": "sub-uuid", "status": "pending", "session_id": GF_ID, "inserted": True},
                          _violation(), _stored_row("pending")]
        conn.fetchrow = fetchrow
        conn.fetchval = AsyncMock(return_value=None)
        res = await insert_or_update_submission(conn, _gforms_event())
        assert res["changed"] is False and res["submission_id"] == STORED_ID and res["status"] == "pending"
        assert res["messages_inserted"] == 0
        conn.transaction.assert_called_once()  # entered once; the violation left it, which rolls it back
    asyncio.run(run())


def test_gforms_payload_row_that_already_existed_rolls_back_as_stored():
    async def run():
        conn = FakeConn()
        conn.fetchrow = AsyncMock(side_effect=[
            None,
            {"id": "sub-uuid", "status": "pending", "session_id": GF_ID, "inserted": False},
            None,                       # feedback_submissions DO UPDATE matched, changed nothing
            _stored_row("success"),     # re-lookup
        ])
        conn.fetchval = AsyncMock(return_value=None)
        res = await insert_or_update_submission(conn, _gforms_event())
        assert res["changed"] is False and res["submission_id"] == STORED_ID
        conn.fetch.assert_not_awaited()  # never reached the feedbacks upsert
    asyncio.run(run())


def test_other_unique_violations_are_not_swallowed():
    async def run():
        conn = FakeConn()
        async def fetchrow(*args, **kwargs):
            if "INSERT INTO feedback_submissions" in args[0]:
                raise _violation("feedback_submissions_submission_id_tenant_code_key")
            if "INSERT INTO submissions" in args[0]:
                return {"id": "sub-uuid", "status": "pending", "session_id": GF_ID, "inserted": True}
            return None
        conn.fetchrow = fetchrow
        conn.fetchval = AsyncMock(return_value=None)
        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
            await insert_or_update_submission(conn, _gforms_event())
    asyncio.run(run())


@pytest.mark.parametrize("mutate, needle", [
    (lambda e: e["data"].pop("sourceRef"), "sourceRef must be a non-empty string"),
    (lambda e: e["data"].update(sourceRef="  "), "sourceRef must be a non-empty string"),
    (lambda e: e["data"].update(sourceRef=None), "sourceRef must be a non-empty string"),
    (lambda e: e.update(sessionId="x" * 32), "same value as submissionId and sessionId"),
])
def test_gforms_branch_rejects_bad_identity_before_any_query(mutate, needle):
    async def run():
        conn = FakeConn()
        event = _gforms_event()
        mutate(event)
        with pytest.raises(ValueError, match=needle):
            await insert_or_update_submission(conn, event)
        conn.fetchrow.assert_not_awaited()
        conn.fetchval.assert_not_awaited()
        conn.transaction.assert_not_called()
    asyncio.run(run())


def test_mitra_event_with_source_ref_is_rejected_before_any_query():
    async def run():
        conn = FakeConn()
        event = _feedback_event()
        event["data"]["sourceRef"] = GF_REF
        with pytest.raises(ValueError, match="sourceRef must be absent or null"):
            await insert_or_update_submission(conn, event)
        conn.fetchrow.assert_not_awaited()
    asyncio.run(run())


def test_mitra_event_with_null_source_ref_is_accepted():
    async def run():
        conn = _ops_conn()
        event = _feedback_event()
        event["data"]["sourceRef"] = None
        res = await insert_or_update_submission(conn, event)
        assert res["changed"] is True
    asyncio.run(run())


# consumer behaviour for Google Forms events

def test_consumer_new_gforms_response_is_processed_under_its_own_id(monkeypatch):
    async def run():
        consumer, conn, insert_mock, _, trigger_mock, dlq_mock = _consumer(
            monkeypatch, insert_result={"id": "u", "submission_id": GF_ID, "status": "pending", "changed": True})
        await consumer.process_message(json.dumps(_gforms_event()))
        conn.fetchval.assert_not_awaited()  # no submission-id duplicate pre-check for feedback
        insert_mock.assert_awaited_once()
        trigger_mock.assert_awaited_once_with(GF_ID, "mitra", "feedback")
        dlq_mock.assert_not_awaited()
    asyncio.run(run())


def test_consumer_already_stored_gforms_response_is_unchanged_not_dlqd(monkeypatch):
    async def run():
        consumer, conn, insert_mock, _, trigger_mock, dlq_mock = _consumer(
            monkeypatch,
            insert_result={"id": "u", "submission_id": STORED_ID, "status": "success", "changed": False})
        await consumer.process_message(json.dumps(_gforms_event()))
        conn.fetchval.assert_not_awaited()
        insert_mock.assert_awaited_once()
        trigger_mock.assert_not_awaited()
        dlq_mock.assert_not_awaited()
    asyncio.run(run())


def test_consumer_already_stored_but_pending_gforms_response_is_processed_under_the_stored_id(monkeypatch):
    async def run():
        consumer, _, _, _, trigger_mock, _ = _consumer(
            monkeypatch,
            insert_result={"id": "u", "submission_id": STORED_ID, "status": "pending", "changed": False})
        await consumer.process_message(json.dumps(_gforms_event()))
        trigger_mock.assert_awaited_once_with(STORED_ID, "mitra", "feedback")  # not the event's fresh uuid
    asyncio.run(run())
