"""
Pure tests (fake connection, no database) of the ONE config-lookup rule of the feedback upload: the config of the
session's own program wins, else the tenant's program-less default for the same source and persona, never another
program's config, and more than one candidate at the winning level is an AmbiguousFeedbackConfigError. The scenario tests
on a real Postgres are in tests/test_feedback_config_fallback_pg.py; here the Python side of the rule is pinned
(which rows of the one query are accepted, what is raised, what the upload says when nothing is found).
"""
import asyncio
import copy

import pytest

import app.api.services.uploads as uploads_service
import app.database.feedback_operations as feedback_ops
from app.api.exceptions import AmbiguousFeedbackTarget, InvalidFeedbackFile
from app.database.feedback_operations import AmbiguousFeedbackConfigError, get_active_feedback_config
from tests.conftest import FakeConn, FakePool
from app.database.db import db
from seed_support import gforms_config_v1, mitra_config_v1

PROGRAM = "11111111-1111-4111-8111-111111111111"


def _row(session_type, program_id, config=None):
    return {"config_id": f"cfg-{session_type}-{program_id}", "config_version_id": f"ver-{session_type}-{program_id}",
            "version": 3, "session_type": session_type, "program_id": program_id,
            "config": config if config is not None else {"session_type": session_type}}


def _lookup(monkeypatch, rows, **kwargs):
    conn = FakeConn()
    conn.fetch.return_value = rows

    async def call():
        db.pool = FakePool(conn)          # the pool is kept per event loop: install it inside the running one
        try:
            return await get_active_feedback_config(PROGRAM, "mitra", **kwargs)
        finally:
            db.pool = None
    return asyncio.run(call()), conn


def test_the_own_config_is_returned_even_when_a_default_is_found_with_it(monkeypatch):
    result, _ = _lookup(monkeypatch, [_row("Parent_Voice", PROGRAM), _row("Parent_Voice", None)])
    assert result["program_id"] == PROGRAM and result["config_id"] == f"cfg-Parent_Voice-{PROGRAM}"


def test_a_default_is_returned_when_it_is_the_only_row_and_it_says_it_is_program_less(monkeypatch):
    result, _ = _lookup(monkeypatch, [_row("Parent_Voice", None)])
    assert result["program_id"] is None and result["session_type"] == "Parent_Voice" and result["version"] == 3


def test_no_row_is_no_config(monkeypatch):
    result, _ = _lookup(monkeypatch, [])
    assert result is None


@pytest.mark.parametrize("program_id", [PROGRAM, None])
def test_two_rows_of_the_winning_level_are_ambiguous(monkeypatch, program_id):
    rows = [_row("Parent_Voice", program_id), _row("Youth_Voice", program_id)]
    with pytest.raises(AmbiguousFeedbackConfigError, match="Parent_Voice, Youth_Voice"):
        _lookup(monkeypatch, rows)


def test_the_query_asks_for_the_program_or_the_program_less_rows_of_the_programs_tenant_own_first(monkeypatch):
    _, conn = _lookup(monkeypatch, [], tenant_code="mitra", session_type="Parent_Voice")
    sql, *params = conn.fetch.call_args.args
    assert "c.program_id = $3::uuid" in sql and "c.program_id IS NULL" in sql
    assert "SELECT p.tenant_code FROM programs p WHERE p.id = $3::uuid" in sql        # a default of the program's tenant only
    assert "ORDER BY (c.program_id IS NOT NULL) DESC" in sql and "LIMIT 2" in sql      # own rows first
    assert params == ["mitra", "feedback_upload", PROGRAM, "mitra", "Parent_Voice"]


def _target_with(monkeypatch, config_lookup):
    monkeypatch.setattr(feedback_ops, "get_program_and_leader", _async_value(
        {"program": {"id": PROGRAM, "name": "Beta"}, "leader": {"id": "l", "name": "Parent"}, "tenant_code": "mitra"}))
    monkeypatch.setattr(feedback_ops, "get_active_feedback_config", config_lookup)


def _async_value(value):
    async def call(*args, **kwargs):
        return value
    return call


def test_the_upload_target_says_there_is_no_config_for_the_program_and_no_default(monkeypatch):
    _target_with(monkeypatch, _async_value(None))
    with pytest.raises(InvalidFeedbackFile) as raised:
        asyncio.run(uploads_service.load_feedback_target("Beta", "Parent", "mitra", "mitra"))
    assert any("no config for this program and no program-less default" in message for message in raised.value.errors)


def test_the_upload_target_is_a_409_on_ambiguity(monkeypatch):
    # several configs that the file can not tell apart (only conversation_rows files carry a session_type column)
    async def ambiguous(*args, **kwargs):
        raise AmbiguousFeedbackConfigError("2 configs")
    _target_with(monkeypatch, ambiguous)
    rows = copy.deepcopy(mitra_config_v1("Parent"))
    form = copy.deepcopy(gforms_config_v1("Parent"))
    form["session_type"] = "Other_Voice"
    monkeypatch.setattr(feedback_ops, "get_active_feedback_config_candidates", _async_value([
        {"config_id": "a", "config_version_id": "va", "version": 1, "session_type": "Parent_Voice",
         "program_id": PROGRAM, "config": rows},
        {"config_id": "b", "config_version_id": "vb", "version": 1, "session_type": "Other_Voice",
         "program_id": PROGRAM, "config": form},
    ]))
    with pytest.raises(AmbiguousFeedbackTarget):
        asyncio.run(uploads_service.load_feedback_target("Beta", "Parent", "mitra", "mitra"))


def test_the_upload_target_accepts_the_program_less_default_it_is_given(monkeypatch):
    config = mitra_config_v1("Parent")
    default = {"config_id": "c", "config_version_id": "v", "version": 1, "session_type": config["session_type"],
               "program_id": None, "config": config}
    _target_with(monkeypatch, _async_value(default))
    target = asyncio.run(uploads_service.load_feedback_target("Beta", "Parent", "mitra", "mitra"))
    assert target["active"] is default and target["resolved"]["program"]["id"] == PROGRAM
    assert target["max_csv_upload_bytes"] == config["max_csv_upload_bytes"]


# ---- get_active_feedback_config_candidates: every config of the WINNING level (one program, several personas) ----

def _candidates(rows, **kwargs):
    conn = FakeConn()
    conn.fetch.return_value = rows

    async def call():
        db.pool = FakePool(conn)
        try:
            return await feedback_ops.get_active_feedback_config_candidates(PROGRAM, "mitra", **kwargs)
        finally:
            db.pool = None
    return asyncio.run(call()), conn


def test_the_candidates_are_the_programs_own_configs_and_never_its_defaults():
    rows = [_row("Youth_Voice", PROGRAM), _row("Youth_Voice_Cupshup", PROGRAM), _row("Parent_Voice", None)]
    result, conn = _candidates(rows, tenant_code="mitra")
    assert [c["session_type"] for c in result] == ["Youth_Voice", "Youth_Voice_Cupshup"]
    assert all(c["program_id"] == PROGRAM for c in result)
    sql, *params = conn.fetch.call_args.args
    assert "LIMIT" not in sql and "ORDER BY (c.program_id IS NOT NULL) DESC" in sql
    assert params == ["mitra", "feedback_upload", PROGRAM, "mitra"]


def test_the_candidates_are_the_defaults_when_the_program_has_none_and_empty_when_nothing_matches():
    result, _ = _candidates([_row("Parent_Voice", None), _row("Youth_Voice", None)])
    assert [(c["session_type"], c["program_id"]) for c in result] == [("Parent_Voice", None), ("Youth_Voice", None)]
    assert _candidates([])[0] == []
