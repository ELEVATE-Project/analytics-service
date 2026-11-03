"""
Tests for the `feedback` report type on POST /v1/upload/ (slice 1e): file-level checks in
handle_upload, session-level processing + DLQ in process_csv_inline, size cap, `source`
validation, and (opt-in) the real-Postgres lookups in app/database/feedback_operations.py.

Everything is in-memory fakes (same conventions as tests/unit_testing.py) except the last section,
which is skipped unless FEEDBACK_TEST_DATABASE_URL points at a scratch database that already has
schema.sql applied.
"""
import asyncio
import json
import os
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.exceptions import register_exception_handlers
from app.api.router import api_router
import app.api.services.uploads as uploads_service_module
import app.database.feedback_operations as feedback_ops
import app.database.operations as operations_module
from app.config import settings
from app.services.feedback_constants import PERSONAS, mitra_config_v1
from tests.conftest import FakeConn, install_fake_db, settings_override

YOUTH = PERSONAS["Youth"]
PROGRAM = {"id": YOUTH["program_id"], "name": "1M Voice"}
LEADER = {"id": YOUTH["leader_category_id"], "name": "Youth"}
RESOLVED = {"program": PROGRAM, "leader": LEADER, "tenant_code": "mitra"}
ACTIVE_CONFIG = {
    "config_id": YOUTH["config_id"],
    "config_version_id": "11111111-1111-1111-1111-111111111111",
    "version": 1,
    "config": mitra_config_v1("Youth"),
}

HEADER = ("bot_id,bot_name,session_status,session,submission_id,language,sequence,conversation_row_id,"
          "message,translated_message,created_at,updated_at,stage,receiver_id,sender_id,audio_file,file_url,other_params")
TS = "2026-10-15 08:54:43+05:30"


def _row(bot, sess, sid, seq, msg, tr, stage, sender, status="COMPLETED", lang="en"):
    return f"{bot},Youth,{status},{sess},{sid},{lang},{seq},{seq},{msg},{tr},{TS},{TS},{stage},1,{sender},,,"


def _session(sess, sid, bot="90", status="COMPLETED", answer="17"):
    return [
        _row(bot, sess, sid, 1, answer, "", "AGE_IDENTIFICATION", 120, status),
        _row(bot, sess, sid, 2, "Which language do you speak?", "Which language do you speak?", "LANG_Q", 1, status),
        _row(bot, sess, sid, 3, "English", "", "LANG_Q", 120, status),
        _row(bot, sess, sid, 4, "Thanks", "Thanks", "THANK_YOU", 1, status),
    ]


def _csv(rows: List[str], header: str = HEADER) -> bytes:
    return ("\n".join([header] + rows) + "\n").encode("utf-8")


GOOD_FILE = _csv(_session("a" * 32, "1001") + _session("b" * 32, "1002"))


def _client() -> TestClient:
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(api_router)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def client():
    return _client()


def _headers():
    return {"Authorization": f"Bearer {settings.AUTH_TOKEN}"}


def _form(**overrides):
    data = {"report_type": "feedback", "program_name": "1M Voice", "leader_category": "Youth", "tenant_code": "mitra"}
    data.update(overrides)
    return data


def _files(content: bytes = GOOD_FILE, name: str = "youth.csv"):
    return {"file": (name, content, "text/csv")}


class Stores:
    """Handles on every side-effecting call a rejected upload must NOT make."""

    def __init__(self, insert, upload, lookup, config):
        self.insert, self.upload, self.lookup, self.config = insert, upload, lookup, config


def _patch_upload_pipeline(monkeypatch, resolved: Optional[dict] = RESOLVED, active: Optional[dict] = ACTIVE_CONFIG) -> Stores:
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=False))
    insert = AsyncMock(return_value=7)
    monkeypatch.setattr(operations_module, "insert_upload_record", insert)
    # claim returns None -> handle_upload leaves the record 'pending' and schedules no background task
    monkeypatch.setattr(operations_module, "try_claim_for_processing", AsyncMock(return_value=None))
    upload = MagicMock(return_value="path/to/youth.csv")
    monkeypatch.setattr(uploads_service_module, "upload_csv", upload)
    lookup = AsyncMock(return_value=resolved)
    config = AsyncMock(return_value=active)
    monkeypatch.setattr(feedback_ops, "get_program_and_leader", lookup)
    monkeypatch.setattr(feedback_ops, "get_active_feedback_config", config)
    return Stores(insert, upload, lookup, config)


def _assert_nothing_stored(stores: Stores):
    stores.upload.assert_not_called()
    stores.insert.assert_not_called()


# FILE LEVEL (handle_upload)
def test_feedback_good_persona_file_accepted(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"message": "Successfully uploaded to cloud", "id": 7, "status": "pending"}

    stores.lookup.assert_awaited_once_with("1M Voice", "Youth", "mitra")
    stores.config.assert_awaited_once_with(PROGRAM["id"], "90", "mitra")
    stores.upload.assert_called_once()
    kwargs = stores.insert.await_args.kwargs
    assert kwargs["report_type"] == "feedback"
    meta = kwargs["meta_data"]
    assert meta["source"] == "mitra"
    assert meta["persona"] == "Youth"
    assert meta["bot_id"] == "90"
    assert meta["config_version_id"] == ACTIVE_CONFIG["config_version_id"]
    assert meta["config_version"] == 1
    assert meta["tenant_code"] == "mitra"


def test_feedback_background_task_gets_parsed_string_dataframe(monkeypatch):
    """handle_upload hands the dtype=str frame to the background task ("NA" stays "NA")."""
    _patch_upload_pipeline(monkeypatch)
    monkeypatch.setattr(operations_module, "try_claim_for_processing", AsyncMock(return_value="success"))
    rows = _session("a" * 32, "1001", answer="NA")
    tasks = MagicMock()

    async def run():
        return await uploads_service_module.handle_upload(
            "feedback", "1M Voice", "Youth", "mitra", "na.csv", _csv(rows), tasks, source="mitra")

    out = asyncio.run(run())
    assert out["status"] == "in_progress"
    fn, record_id, file_bytes, df = tasks.add_task.call_args.args
    assert fn is uploads_service_module.process_csv_inline
    assert df.loc[0, "message"] == "NA"


def test_feedback_unknown_program_or_leader_rejected_nothing_stored(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, resolved=None)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(leader_category="Nobody"), files=_files())
    assert resp.status_code == 404
    assert "Nobody" in resp.json()["detail"]
    stores.config.assert_not_called()  # never fabricated, never continued
    _assert_nothing_stored(stores)


def test_feedback_wrong_persona_bot_rejected_whole(client, monkeypatch):
    # A Parent-bot (88) file uploaded as Youth: Youth has no config for bot 88.
    stores = _patch_upload_pipeline(monkeypatch, active=None)
    parent_file = _csv(_session("a" * 32, "1001", bot="88"))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(parent_file))
    assert resp.status_code == 400
    assert resp.json()["errors"] == ["bot_id 88 is not configured for Youth"]
    stores.config.assert_awaited_once_with(PROGRAM["id"], "88", "mitra")
    _assert_nothing_stored(stores)


def test_feedback_mixed_bot_file_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    mixed = _csv(_session("a" * 32, "1001", bot="90") + _session("b" * 32, "1002", bot="88"))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(mixed))
    assert resp.status_code == 400
    (error,) = resp.json()["errors"]
    assert "more than one bot_id" in error and "'88'" in error and "'90'" in error
    stores.config.assert_not_called()
    _assert_nothing_stored(stores)


def test_feedback_missing_bot_id_column_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    header = HEADER.replace("bot_id,", "", 1)
    rows = [r.split(",", 1)[1] for r in _session("a" * 32, "1001")]
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_csv(rows, header)))
    assert resp.status_code == 400
    assert resp.json()["errors"] == ["Missing columns: ['bot_id']"]
    _assert_nothing_stored(stores)


def test_feedback_blank_bot_id_on_every_row_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    rows = [r.replace("90,Youth", ",Youth", 1) for r in _session("a" * 32, "1001")]
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_csv(rows)))
    assert resp.status_code == 400
    assert "blank on every row" in resp.json()["errors"][0]
    _assert_nothing_stored(stores)


def test_feedback_missing_required_column_rejected_nothing_stored(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    cols = HEADER.split(",")
    idx = cols.index("session_status")
    header = ",".join(c for c in cols if c != "session_status")
    rows = [",".join(v for i, v in enumerate(r.split(",")) if i != idx) for r in _session("a" * 32, "1001")]
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_csv(rows, header)))
    assert resp.status_code == 400
    assert any("session_status" in e for e in resp.json()["errors"])
    _assert_nothing_stored(stores)


def test_feedback_unparseable_file_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(HEADER.encode() + b"\n"))
    assert resp.status_code == 400
    assert resp.json()["errors"][0].startswith("Failed to parse CSV")
    _assert_nothing_stored(stores)


def test_feedback_does_not_use_story_column_validation(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    spy = MagicMock(return_value=(False, ["story schema"]))
    monkeypatch.setattr(uploads_service_module, "validate_columns", spy)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 200
    spy.assert_not_called()


def test_feedback_duplicate_file_still_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=True))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 400
    assert "ALREADY EXISTS" in resp.json()["detail"]
    stores.lookup.assert_not_called()
    _assert_nothing_stored(stores)


# `source` and size cap
def test_feedback_invalid_source_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="whatsapp"), files=_files())
    assert resp.status_code == 400
    assert "whatsapp" in resp.json()["detail"] and "mitra" in resp.json()["detail"]
    stores.lookup.assert_not_called()
    _assert_nothing_stored(stores)


def test_feedback_source_defaults_to_mitra_and_is_case_insensitive(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="MITRA"), files=_files())
    assert resp.status_code == 200
    assert stores.insert.await_args.kwargs["meta_data"]["source"] == "mitra"


def test_feedback_google_forms_source_not_supported_yet(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="google_forms"), files=_files())
    assert resp.status_code == 400
    assert "not supported yet" in resp.json()["detail"]
    _assert_nothing_stored(stores)


def test_source_field_ignored_for_story(client, monkeypatch):
    """story/discussion do not look at `source` (an unknown value must not change their behaviour)."""
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=True))
    resp = client.post(
        "/v1/upload/", headers=_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "source": "whatsapp"},
        files={"file": ("s.csv", b"id,Title\n1,x\n", "text/csv")},
    )
    assert resp.status_code == 400
    assert "ALREADY EXISTS" in resp.json()["detail"]  # got past request validation to the service


def test_feedback_uses_its_own_size_cap_story_keeps_its_own(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    settings_override(monkeypatch, settings, MAX_CSV_UPLOAD_BYTES=10, FEEDBACK_MAX_CSV_UPLOAD_BYTES=len(GOOD_FILE) + 1)
    # feedback file is far above the 10-byte story cap but under its own cap -> accepted
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 200, resp.text

    # story with the same payload size is rejected by the story cap
    resp = client.post(
        "/v1/upload/", headers=_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L"},
        files={"file": ("s.csv", GOOD_FILE, "text/csv")},
    )
    assert resp.status_code == 413

    # and a feedback file over the feedback cap is rejected with 413, nothing stored
    stores.insert.reset_mock()
    stores.upload.reset_mock()
    settings_override(monkeypatch, settings, FEEDBACK_MAX_CSV_UPLOAD_BYTES=len(GOOD_FILE) - 1)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 413
    _assert_nothing_stored(stores)


def test_validators_report_type_and_caps(monkeypatch):
    from app.api.exceptions import FileTooLarge
    from app.api.validators.uploads import validate_file_bytes, validate_report_type

    assert validate_report_type(" Feedback ") == "feedback"
    settings_override(monkeypatch, settings, MAX_CSV_UPLOAD_BYTES=5, FEEDBACK_MAX_CSV_UPLOAD_BYTES=50)
    validate_file_bytes(b"x" * 20, "feedback")
    with pytest.raises(FileTooLarge):
        validate_file_bytes(b"x" * 20, "story")
    with pytest.raises(FileTooLarge):
        validate_file_bytes(b"x" * 20)  # legacy call signature keeps the story cap
    with pytest.raises(FileTooLarge):
        validate_file_bytes(b"x" * 51, "feedback")


# SESSION LEVEL (process_csv_inline)
def _record(meta: Optional[dict] = None) -> dict:
    return {
        "id": 7, "report_type": "feedback", "cloud_storage_path": "path/to/youth.csv",
        "leader_category": "Youth", "program_name": "1M Voice",
        "meta_data": meta if meta is not None else {"tenant_code": "mitra", "source": "mitra"},
    }


class BgHarness:
    def __init__(self, monkeypatch, record: Optional[dict] = None):
        self.record = record or _record()
        monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value=self.record))
        self.update_status = AsyncMock()
        monkeypatch.setattr(operations_module, "update_status", self.update_status)
        self.lookup = AsyncMock(return_value=RESOLVED)
        self.config = AsyncMock(return_value=ACTIVE_CONFIG)
        monkeypatch.setattr(feedback_ops, "get_program_and_leader", self.lookup)
        monkeypatch.setattr(feedback_ops, "get_active_feedback_config", self.config)
        self.push = MagicMock()
        monkeypatch.setattr(uploads_service_module, "_push_rows_sync", self.push)
        self.dlq = MagicMock()
        monkeypatch.setattr(uploads_service_module, "_push_dlq_sync", self.dlq)
        self.fetch = MagicMock(return_value=GOOD_FILE)
        monkeypatch.setattr(uploads_service_module, "fetch_csv", self.fetch)

    def run(self, **kwargs):
        asyncio.run(uploads_service_module.process_csv_inline(7, **kwargs))

    @property
    def final(self):
        call = self.update_status.await_args_list[-1]
        return call.args[1], (call.args[2] if len(call.args) > 2 else None)


def test_background_all_sessions_published_status_success(monkeypatch):
    h = BgHarness(monkeypatch)
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 2
    assert meta["sessions_skipped"] == 0
    assert meta["config_version_id"] == ACTIVE_CONFIG["config_version_id"]
    assert "processed_at" in meta and "schema_validation_errors" not in meta and "truncated" not in meta
    h.dlq.assert_not_called()

    payloads = h.push.call_args.args[0]
    assert [key for _, key in payloads] == ["7-0", "7-1"]
    event = json.loads(payloads[0][0])
    assert event["submissionType"] == "feedback" and event["submissionId"] == "1001"
    assert event["tags"]["programId"] == PROGRAM["id"] and event["tags"]["leaderCategoryId"] == LEADER["id"]
    # status was moved to in_progress first
    assert h.update_status.await_args_list[0].args[:2] == (7, "in_progress")


def test_background_skipped_sessions_go_to_dlq_listed_in_meta_status_still_success(monkeypatch):
    h = BgHarness(monkeypatch)
    bad = _session("c" * 32, "1003", status="DONE")  # session_status not COMPLETED / IN_PROGRESS
    empty_msg = _session("d" * 32, "1004")
    empty_msg[2] = _row("90", "d" * 32, "1004", 3, "", "", "LANG_Q", 120)  # empty answer
    file_bytes = _csv(_session("a" * 32, "1001") + bad + empty_msg)
    h.run(file_bytes=file_bytes)

    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 1
    assert meta["sessions_skipped"] == 2
    assert meta["sessions_dead_lettered"] == 2
    assert "dlq_publish_error" not in meta
    skipped = {e["submissionId"]: e for e in meta["schema_validation_errors"]}
    assert set(skipped) == {"1003", "1004"}
    assert skipped["1003"]["sessionId"] == "c" * 32 and skipped["1003"]["problems"]

    (messages,) = h.dlq.call_args.args
    assert len(messages) == 2
    by_id = {headers["submissionId"]: (json.loads(value), headers) for value, headers in messages}
    value, headers = by_id["1003"]
    assert value["submissionType"] == "feedback" and value["sessionId"] == "c" * 32
    assert value["tenantCode"] == "mitra" and value["csvUploadId"] == 7 and value["problems"]
    assert headers["sessionId"] == "c" * 32 and headers["tenantCode"] == "mitra"
    assert headers["reason"].startswith("Skipped feedback session:")
    assert "answer" not in json.dumps(value).lower().replace("problems", "")  # no conversation text
    # only the good session reached the ingestion topic
    assert len(h.push.call_args.args[0]) == 1


def test_background_schema_failed_event_is_skipped_and_dead_lettered(monkeypatch):
    h = BgHarness(monkeypatch)
    real = uploads_service_module.validate_ingestion_schema

    def fail_second(event, report_type, event_type):
        assert (report_type, event_type) == ("feedback", "create")
        return ["'data.messages' is empty"] if event["submissionId"] == "1002" else real(event, report_type, event_type)

    monkeypatch.setattr(uploads_service_module, "validate_ingestion_schema", fail_second)
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 1
    assert meta["schema_validation_errors"] == [
        {"submissionId": "1002", "sessionId": "b" * 32, "problems": ["'data.messages' is empty"]}
    ]
    (messages,) = h.dlq.call_args.args
    assert [headers["submissionId"] for _, headers in messages] == ["1002"]


def test_background_dlq_failure_is_recorded_but_upload_still_success(monkeypatch):
    h = BgHarness(monkeypatch)
    h.dlq.side_effect = RuntimeError("dlq broker down")
    file_bytes = _csv(_session("a" * 32, "1001") + _session("c" * 32, "1003", status="DONE"))
    h.run(file_bytes=file_bytes)
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 1
    assert "dlq broker down" in meta["dlq_publish_error"]
    assert "sessions_dead_lettered" not in meta
    assert len(meta["schema_validation_errors"]) == 1


def test_background_error_list_capped_at_200_with_truncated_marker(monkeypatch):
    h = BgHarness(monkeypatch)
    rows = _session("a" * 32, "1")
    for i in range(2, 252):  # 250 bad sessions
        rows += _session(f"{i:032d}", str(i), status="DONE")
    h.run(file_bytes=_csv(rows))
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 250
    assert len(meta["schema_validation_errors"]) == 200
    assert meta["truncated"] is True
    (messages,) = h.dlq.call_args.args
    assert len(messages) == 250  # the DLQ gets every skipped session, only the meta list is capped
    assert meta["sessions_dead_lettered"] == 250


def test_background_all_sessions_skipped_is_still_success(monkeypatch):
    h = BgHarness(monkeypatch)
    h.run(file_bytes=_csv(_session("c" * 32, "1003", status="DONE")))
    status, meta = h.final
    assert status == "success" and meta["rows_pushed"] == 0 and meta["sessions_skipped"] == 1
    h.push.assert_not_called()
    h.dlq.assert_called_once()


def test_background_kafka_failure_marks_on_hold_and_skips_dlq(monkeypatch):
    h = BgHarness(monkeypatch)
    h.push.side_effect = RuntimeError("broker down")
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "on_hold"
    assert meta["stage"] == "Kafka Publishing" and "broker down" in meta["exception"]
    h.dlq.assert_not_called()


def test_background_fetch_failure_marks_on_hold(monkeypatch):
    h = BgHarness(monkeypatch)
    h.fetch.side_effect = RuntimeError("bucket unreachable")

    async def no_sleep(_):
        return None

    monkeypatch.setattr(uploads_service_module.asyncio, "sleep", no_sleep)
    h.run()
    status, meta = h.final
    assert status == "on_hold" and meta["stage"] == "CSV Fetching"
    assert h.fetch.call_count == 3
    h.push.assert_not_called()


def test_background_loads_from_storage_with_string_parse_and_rechecks_file(monkeypatch):
    h = BgHarness(monkeypatch)
    h.fetch.return_value = _csv(_session("a" * 32, "1001", answer="None"))
    h.run()
    status, meta = h.final
    assert status == "success" and meta["rows_pushed"] == 1
    event = json.loads(h.push.call_args.args[0][0][0])
    assert "None" in [m["message"] for m in event["data"]["messages"]]  # not turned into NaN

    # a stored file that no longer passes the config's file-level check is put on hold
    h2 = BgHarness(monkeypatch)
    h2.fetch.return_value = _csv(_session("a" * 32, "1001"), HEADER.replace("session_status", "status"))
    h2.run()
    status, meta = h2.final
    assert status == "on_hold" and meta["stage"] == "Feedback Config Resolution"
    assert meta["validation_errors"]


def test_background_unknown_program_never_fabricated_on_hold(monkeypatch):
    h = BgHarness(monkeypatch)
    h.lookup.return_value = None
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "on_hold"
    assert meta["stage"] == "Feedback Config Resolution" and "does not exist" in meta["error"]
    h.push.assert_not_called()
    h.dlq.assert_not_called()


def test_background_config_missing_on_hold(monkeypatch):
    h = BgHarness(monkeypatch)
    h.config.return_value = None
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "on_hold"
    assert meta["validation_errors"] == ["bot_id 90 is not configured for Youth"]


def test_background_db_error_on_hold(monkeypatch):
    h = BgHarness(monkeypatch)
    h.lookup.side_effect = RuntimeError("db down")
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "on_hold" and "db down" in meta["error"]


def test_background_missing_tenant_in_record_on_hold(monkeypatch):
    h = BgHarness(monkeypatch, record=_record(meta={}))
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "on_hold" and meta["stage"] == "Feedback Config Resolution"
    h.lookup.assert_not_called()


def test_background_meta_data_as_json_string(monkeypatch):
    h = BgHarness(monkeypatch, record={**_record(), "meta_data": json.dumps({"tenant_code": "mitra", "source": "mitra"})})
    h.run(file_bytes=GOOD_FILE)
    assert h.final[0] == "success"


def test_push_dlq_sync_produces_to_dlq_topic_with_headers(monkeypatch):
    from tests.conftest import make_fake_kafka_producer
    producer = make_fake_kafka_producer()
    monkeypatch.setattr(uploads_service_module, "_get_producer", lambda: producer)
    settings_override(monkeypatch, settings, KAFKA_TOPIC_INGESTION_DLQ="test.dlq")
    uploads_service_module._push_dlq_sync([('{"a": 1}', {"submissionId": "1", "sessionId": None, "reason": "r"})])
    args = producer.produce.call_args
    assert args.args[0] == "test.dlq"
    assert args.kwargs["value"] == b'{"a": 1}'
    assert dict(args.kwargs["headers"]) == {"submissionId": b"1", "reason": b"r"}  # None identifiers omitted


def test_push_dlq_sync_raises_on_delivery_error(monkeypatch):
    from confluent_kafka import KafkaException
    from tests.conftest import make_fake_kafka_producer
    producer = make_fake_kafka_producer(delivery_error="boom")
    monkeypatch.setattr(uploads_service_module, "_get_producer", lambda: producer)
    with pytest.raises(KafkaException):
        uploads_service_module._push_dlq_sync([("{}", {"reason": "r"})])


# STORY / DISCUSSION unchanged: the story branch of process_csv_inline is not entered for feedback
# and the feedback branch is not entered for story.
def test_story_record_never_enters_feedback_branch(monkeypatch):
    conn = install_fake_db(monkeypatch, uploads_service_module)
    conn.fetchrow.return_value = None
    record = {"id": 1, "report_type": "story", "cloud_storage_path": "p.csv", "leader_category": "L",
              "program_name": "P", "meta_data": {"tenant_code": "mitra"}}
    monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value=record))
    monkeypatch.setattr(operations_module, "update_status", AsyncMock())
    feedback_branch = AsyncMock()
    monkeypatch.setattr(uploads_service_module, "_process_feedback_inline", feedback_branch)
    import pandas as pd
    monkeypatch.setattr(uploads_service_module, "fetch_csv", MagicMock(return_value=b"raw"))
    monkeypatch.setattr(uploads_service_module, "load_csv", MagicMock(return_value=pd.DataFrame([{"id": "1"}])))
    monkeypatch.setattr(uploads_service_module, "validate_columns", MagicMock(return_value=(False, ["x"])))
    asyncio.run(uploads_service_module.process_csv_inline(1))
    feedback_branch.assert_not_called()


# REAL POSTGRES (opt-in): get_program_and_leader / get_active_feedback_config
DB_URL = os.environ.get("FEEDBACK_TEST_DATABASE_URL")


@pytest.mark.skipif(DB_URL is None, reason="FEEDBACK_TEST_DATABASE_URL not set (scratch DB with schema.sql applied)")
def test_real_db_lookups_by_name_and_tenant(monkeypatch):
    import asyncpg
    from app.database.db import db
    from app.database.seed_feedback import seed_feedback

    async def run():
        pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=2)
        monkeypatch.setattr(db, "pool", pool, raising=False)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO tenant(name, code) VALUES ('Mitra', 'mitra'), ('Other', 'other') ON CONFLICT DO NOTHING")
                await seed_feedback(conn)
                # same names under ANOTHER tenant must never be matched for tenant 'mitra'
                other_leader = await conn.fetchval(
                    "INSERT INTO leader_category (name, tenant_code) VALUES ('Youth', 'other') RETURNING id")
                await conn.execute(
                    "INSERT INTO programs (leaders_id, name, tenant_code) VALUES ($1, '1M Voice', 'other')", other_leader)

            for persona, p in PERSONAS.items():
                got = await feedback_ops.get_program_and_leader("1M Voice", persona, "mitra")
                assert got == {
                    "program": {"id": p["program_id"], "name": "1M Voice"},
                    "leader": {"id": p["leader_category_id"], "name": persona},
                    "tenant_code": "mitra",
                }
            other = await feedback_ops.get_program_and_leader("1M Voice", "Youth", "other")
            assert other["leader"]["id"] == str(other_leader)
            assert await feedback_ops.get_program_and_leader("1M Voice", "Nobody", "mitra") is None
            assert await feedback_ops.get_program_and_leader("No Program", "Youth", "mitra") is None
            assert await feedback_ops.get_program_and_leader("1M Voice", "Youth", "missing-tenant") is None

            for persona, p in PERSONAS.items():
                active = await feedback_ops.get_active_feedback_config(p["program_id"], p["bot_id"])
                assert active["config_id"] == p["config_id"] and active["version"] == 1
                assert active["config"] == mitra_config_v1(persona)  # JSONB came back as a dict
            # a bot configured for a different persona's program is not found
            assert await feedback_ops.get_active_feedback_config(PERSONAS["Youth"]["program_id"], "88") is None
            assert await feedback_ops.get_active_feedback_config(PERSONAS["Youth"]["program_id"], "90", "google_forms") is None

            # only the ACTIVE version is returned; v2 supersedes v1
            async with pool.acquire() as conn:
                await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", YOUTH["config_id"])
                await conn.execute(
                    "INSERT INTO config_version (config_id, version, is_active, config) VALUES ($1::uuid, 2, TRUE, $2::jsonb)",
                    YOUTH["config_id"], json.dumps({"marker": "v2"}))
            active = await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "90")
            assert active["version"] == 2 and active["config"] == {"marker": "v2"}
            async with pool.acquire() as conn:
                await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", YOUTH["config_id"])
            assert await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "90") is None
        finally:
            # these lookups need committed rows (feedback_ops uses its own connections), so undo what the test added
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM config_version WHERE config_id = $1::uuid AND version = 2", YOUTH["config_id"])
                await conn.execute("UPDATE config_version SET is_active = TRUE WHERE config_id = $1::uuid AND version = 1", YOUTH["config_id"])
                await conn.execute("DELETE FROM programs WHERE tenant_code = 'other'")
                await conn.execute("DELETE FROM leader_category WHERE tenant_code = 'other'")
                await conn.execute("DELETE FROM tenant WHERE code = 'other'")
            await pool.close()

    asyncio.run(run())
