"""
Schema + seed tests for the `feedback` report type (docs/feedback-upload-plan.md, slice 1a).

Pure in-memory: the seed runs against conftest.py's FakeConn and schema.sql is only read as text.
The real-Postgres counterpart is tests/test_feedback_pg_integration.py.
"""
import asyncio
import json
import re

from app.database.db import SCHEMA_FILE
from app.database.seed_feedback import seed_feedback
from app.services.feedback_constants import (
    CONFIG_TYPE_FEEDBACK_UPLOAD,
    FEEDBACK_PROGRAM_NAME,
    FEEDBACK_TENANT_CODE,
    PERSONAS,
    mitra_config_v1,
)
from conftest import FakeConn


def _statements(conn):
    return [(c.args[0], c.args[1:]) for c in conn.execute.call_args_list]


def _seed(conn):
    asyncio.run(seed_feedback(conn))


def _tenant_present_conn():
    conn = FakeConn()
    conn.fetchval.return_value = 1
    return conn


def test_seed_issues_four_statements_per_persona():
    conn = _tenant_present_conn()
    _seed(conn)
    stmts = _statements(conn)
    assert len(stmts) == 4 * len(PERSONAS)
    tables = [re.search(r"INSERT INTO (\w+)", sql).group(1) for sql, _ in stmts]
    assert tables == ["leader_category", "programs", "config", "config_version"] * len(PERSONAS)
    conn.fetchval.assert_awaited_once()
    assert conn.fetchval.call_args.args[1] == FEEDBACK_TENANT_CODE


def test_seed_never_overwrites():
    conn = _tenant_present_conn()
    _seed(conn)
    for sql, _ in _statements(conn):
        assert "ON CONFLICT" in sql and "DO NOTHING" in sql
        assert "DO UPDATE" not in sql


def test_seed_parameters_use_fixed_ids_and_constants():
    conn = _tenant_present_conn()
    _seed(conn)
    by_table = {}
    for sql, params in _statements(conn):
        by_table.setdefault(re.search(r"INSERT INTO (\w+)", sql).group(1), []).append(params)

    for i, (persona, p) in enumerate(PERSONAS.items()):
        assert by_table["leader_category"][i] == (p["leader_category_id"], persona, "mitra")
        assert by_table["programs"][i] == (
            p["program_id"], p["leader_category_id"], FEEDBACK_PROGRAM_NAME, "mitra")
        assert by_table["config"][i] == (
            p["config_id"], "mitra", p["config_name"], CONFIG_TYPE_FEEDBACK_UPLOAD,
            p["program_id"], "mitra", p["bot_id"], p["bot_name"])
        cv = by_table["config_version"][i]
        assert cv[0] == p["config_id"]
        # jsonb parameter is a JSON string (asyncpg's default jsonb codec takes text), not a dict
        assert isinstance(cv[1], str)
        assert json.loads(cv[1]) == mitra_config_v1(persona)
        assert cv[2:] == ("seed", "Initial format")

    assert [p[1] for p in by_table["leader_category"]] == ["Youth", "Parent", "General Public"]


def test_seed_config_row_has_null_leader_and_version_is_active_v1():
    conn = _tenant_present_conn()
    _seed(conn)
    sqls = {re.search(r"INSERT INTO (\w+)", s).group(1): s for s, _ in _statements(conn)}
    assert "NULL" in sqls["config"] and "leader_id" in sqls["config"]
    assert re.search(r"SELECT \$1::uuid, 1, TRUE, \$2::jsonb", sqls["config_version"])


def test_seed_called_twice_issues_identical_statements():
    conn = _tenant_present_conn()
    _seed(conn)
    first = _statements(conn)
    conn.execute.reset_mock()
    _seed(conn)
    assert _statements(conn) == first


def test_seed_skips_with_warning_when_tenant_missing(caplog):
    conn = FakeConn()  # fetchval -> None: no tenant row
    with caplog.at_level("WARNING"):
        _seed(conn)
    conn.execute.assert_not_awaited()
    assert any("tenant" in r.message and "mitra" in r.message and r.levelname == "WARNING"
               for r in caplog.records)


def test_seed_never_inserts_a_tenant():
    conn = FakeConn()
    _seed(conn)
    conn2 = _tenant_present_conn()
    _seed(conn2)
    for c in (conn, conn2):
        assert not any("INSERT INTO tenant" in sql for sql, _ in _statements(c))


# schema.sql text checks
SCHEMA_TEXT = SCHEMA_FILE.read_text(encoding="utf-8")


def test_schema_has_the_four_feedback_tables():
    for table in ("feedbacks", "feedback_submissions", "config", "config_version"):
        assert re.search(rf"^CREATE TABLE {table} \(", SCHEMA_TEXT, re.M), table


def test_schema_does_not_create_phase3_table():
    assert "feedback_extracted_data" not in SCHEMA_TEXT


def test_schema_feedback_tables_come_after_their_fk_targets():
    pos = {t: SCHEMA_TEXT.index(f"CREATE TABLE {t} (") for t in
           ("programs", "submissions", "feedback_submissions", "feedbacks", "config", "config_version")}
    assert pos["feedback_submissions"] > pos["submissions"]
    assert pos["feedbacks"] > pos["submissions"]
    assert pos["config"] > pos["programs"]
    assert pos["config_version"] > pos["config"]


def test_schema_unique_indexes_use_if_not_exists():
    # db.py injects IF NOT EXISTS only for plain CREATE TABLE / CREATE INDEX, never for UNIQUE ones.
    unique = re.findall(r"^CREATE UNIQUE INDEX\b.*$", SCHEMA_TEXT, re.M)
    assert len(unique) >= 2
    for line in unique:
        assert line.startswith("CREATE UNIQUE INDEX IF NOT EXISTS "), line


def test_schema_feedback_indexes_present():
    for name in ("idx_feedbacks_program_question", "idx_feedbacks_parent", "idx_feedbacks_source",
                 "uq_config_source_type_program_bot", "uq_config_version_active"):
        assert re.search(rf"CREATE (UNIQUE )?INDEX IF NOT EXISTS {name}\b", SCHEMA_TEXT), name
    assert "COALESCE(program_id, '00000000-0000-0000-0000-000000000000')" in SCHEMA_TEXT
    assert "COALESCE(bot_id, '')" in SCHEMA_TEXT
    assert re.search(r"ON config_version \(config_id\) WHERE is_active", SCHEMA_TEXT)
