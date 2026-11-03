"""
Real-Postgres integration tests for the feedback schema and seed (slice 1a).

Skipped unless FEEDBACK_TEST_DATABASE_URL is set to a DSN of an EMPTY database, e.g.
  FEEDBACK_TEST_DATABASE_URL=postgresql://postgres@localhost:54329/fb_test
The tests apply schema.sql themselves, exactly the way app/database/db.py does (IF NOT EXISTS
injection), and never touch anything outside that database. Test data is synthetic.
"""
import asyncio
import json
import os

import asyncpg
import pytest

from app.database.db import SCHEMA_FILE
from app.database.seed_feedback import seed_feedback
from app.services.feedback_constants import PERSONAS, gforms_config_v1, mitra_config_v1

DSN = os.environ.get("FEEDBACK_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

YOUTH = PERSONAS["Youth"]
PARENT = PERSONAS["Parent"]


def _schema_sql() -> str:
    """schema.sql with IF NOT EXISTS injected the way Database.initialize_schema does it."""
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    sql = sql.replace("CREATE TABLE IF NOT EXISTS ", "__TABLE_ALREADY__")
    sql = sql.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
    sql = sql.replace("__TABLE_ALREADY__", "CREATE TABLE IF NOT EXISTS ")
    sql = sql.replace("CREATE INDEX IF NOT EXISTS ", "__INDEX_ALREADY__")
    sql = sql.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ")
    sql = sql.replace("__INDEX_ALREADY__", "CREATE INDEX IF NOT EXISTS ")
    return sql


def run_db(body):
    """
    Runs `async def body(conn)` against the test database inside one transaction that is always
    rolled back, after the schema (twice) and the seed have been applied. DDL is transactional in
    Postgres, so the target database is left untouched even if it was empty.
    """
    async def runner():
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            sql = _schema_sql()
            await conn.execute(sql)
            await conn.execute(sql)
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT DO NOTHING")
            await seed_feedback(conn)
            return await body(conn)
        finally:
            await tx.rollback()
            await conn.close()
    return asyncio.run(runner())


async def _expect_violation(conn, exc, sql, *args):
    """Runs `sql` in a savepoint and asserts it raises `exc`; the outer transaction stays usable."""
    sp = conn.transaction()
    await sp.start()
    try:
        await conn.execute(sql, *args)
    except exc:
        await sp.rollback()
        return
    await sp.rollback()
    raise AssertionError(f"expected {exc.__name__} from: {sql}")


async def _add_submission(conn, sid="S1", session="TOKEN1"):
    await conn.execute(
        "INSERT INTO submissions (submission_id, session_id, tenant_code, submission_type) "
        "VALUES ($1, $2, 'mitra', 'feedback')", sid, session)


async def _add_feedback(conn, sid="S1", seq=1, parent=None, session="TOKEN1"):
    return await conn.fetchval(
        "INSERT INTO feedbacks (submission_id, tenant_code, session_id, source, sequence, message, parent_id) "
        "VALUES ($1, 'mitra', $2, 'mitra', $3, 'm', $4) RETURNING id", sid, session, seq, parent)


async def _add_feedback_submission(conn, sid="S1", status="COMPLETED"):
    await conn.execute(
        "INSERT INTO feedback_submissions (submission_id, tenant_code, source, completion_status) "
        "VALUES ($1, 'mitra', 'mitra', $2)", sid, status)


SEED_COUNTS = {"leader_category": 3, "programs": 3, "config": 6, "config_version": 6}


def test_schema_applies_twice_and_seed_counts():
    async def body(conn):
        for table, expected in SEED_COUNTS.items():
            assert await conn.fetchval(f"SELECT count(*) FROM {table}") == expected, table
    run_db(body)


def test_seed_twice_leaves_the_same_rows():
    async def body(conn):
        await seed_feedback(conn)
        await seed_feedback(conn)
        for table, expected in SEED_COUNTS.items():
            assert await conn.fetchval(f"SELECT count(*) FROM {table}") == expected, table
        assert await conn.fetchval("SELECT count(*) FROM tenant") == 1
    run_db(body)


def test_seeded_rows_match_the_contract():
    async def body(conn):
        for persona, p in PERSONAS.items():
            lc = await conn.fetchrow("SELECT name, tenant_code FROM leader_category WHERE id = $1::uuid",
                                     p["leader_category_id"])
            assert (lc["name"], lc["tenant_code"]) == (persona, "mitra")
            prog = await conn.fetchrow("SELECT name, leaders_id::text AS leaders_id FROM programs WHERE id = $1::uuid",
                                       p["program_id"])
            assert prog["name"] == "1M Voice" and prog["leaders_id"] == p["leader_category_id"]
            cfg = await conn.fetchrow(
                "SELECT config_source, config_type, program_id::text AS program_id, leader_id, bot_id, bot_name, "
                "tenant_code, name FROM config WHERE id = $1::uuid", p["config_id"])
            assert dict(cfg) == {
                "config_source": "mitra", "config_type": "feedback_upload", "program_id": p["program_id"],
                "leader_id": None, "bot_id": p["bot_id"], "bot_name": p["bot_name"], "tenant_code": "mitra",
                "name": p["config_name"]}
            ver = await conn.fetchrow(
                "SELECT version, is_active, config::text AS config, created_by, change_note "
                "FROM config_version WHERE config_id = $1::uuid", p["config_id"])
            assert (ver["version"], ver["is_active"], ver["created_by"], ver["change_note"]) == (
                1, True, "seed", "Initial format")
            assert json.loads(ver["config"]) == mitra_config_v1(persona)

            gf = await conn.fetchrow(
                "SELECT config_source, config_type, program_id::text AS program_id, leader_id, bot_id, bot_name, "
                "tenant_code, name FROM config WHERE id = $1::uuid", p["gforms_config_id"])
            assert dict(gf) == {
                "config_source": "google_forms", "config_type": "feedback_upload", "program_id": p["program_id"],
                "leader_id": None, "bot_id": None, "bot_name": None, "tenant_code": "mitra",
                "name": f"{persona} form CSV validator"}
            gver = await conn.fetchrow(
                "SELECT version, is_active, config::text AS config, created_by, change_note "
                "FROM config_version WHERE config_id = $1::uuid", p["gforms_config_id"])
            assert (gver["version"], gver["is_active"], gver["created_by"], gver["change_note"]) == (
                1, True, "seed", "Initial format")
            assert json.loads(gver["config"]) == gforms_config_v1(persona)
    run_db(body)


def test_google_forms_and_mitra_configs_of_one_program_coexist():
    async def body(conn):
        for p in PERSONAS.values():
            rows = await conn.fetch(
                "SELECT config_source, bot_id FROM config WHERE program_id = $1::uuid ORDER BY config_source",
                p["program_id"])
            assert [(r["config_source"], r["bot_id"]) for r in rows] == [
                ("google_forms", None), ("mitra", p["bot_id"])]
        # the Google Forms row is the one a lookup by (source, type, program) finds, with its active version
        row = await conn.fetchrow(
            "SELECT c.id::text AS id, v.version FROM config c JOIN config_version v ON v.config_id = c.id AND v.is_active "
            "WHERE c.config_source = 'google_forms' AND c.config_type = 'feedback_upload' AND c.program_id = $1::uuid "
            "AND c.bot_id IS NULL", YOUTH["program_id"])
        assert (row["id"], row["version"]) == (YOUTH["gforms_config_id"], 1)
    run_db(body)


def test_seed_does_not_overwrite_edits_or_resurrect_v1():
    async def body(conn):
        await conn.execute("UPDATE config SET name = 'edited' WHERE id = $1::uuid", YOUTH["config_id"])
        # operator superseded v1: v1 inactive, v2 active
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid",
                           YOUTH["config_id"])
        await conn.execute("INSERT INTO config_version (config_id, version, config) VALUES ($1::uuid, 2, '{}'::jsonb)",
                           YOUTH["config_id"])
        # operator deleted Parent's v1 and kept a v2
        await conn.execute("DELETE FROM config_version WHERE config_id = $1::uuid", PARENT["config_id"])
        await conn.execute("INSERT INTO config_version (config_id, version, config) VALUES ($1::uuid, 2, '{}'::jsonb)",
                           PARENT["config_id"])
        await seed_feedback(conn)  # must not raise (one-active-version index) nor change anything
        assert await conn.fetchval("SELECT name FROM config WHERE id = $1::uuid", YOUTH["config_id"]) == "edited"
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1::uuid",
                                   YOUTH["config_id"]) == 2
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1::uuid",
                                   PARENT["config_id"]) == 1
    run_db(body)


def test_seed_does_not_overwrite_google_forms_edits():
    async def body(conn):
        # operator edited the Google Forms config row and superseded its v1 with an active v2
        await conn.execute("UPDATE config SET name = 'edited gf' WHERE id = $1::uuid", YOUTH["gforms_config_id"])
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid",
                           YOUTH["gforms_config_id"])
        await conn.execute("INSERT INTO config_version (config_id, version, config) VALUES ($1::uuid, 2, '{}'::jsonb)",
                           YOUTH["gforms_config_id"])
        await seed_feedback(conn)
        assert await conn.fetchval("SELECT name FROM config WHERE id = $1::uuid", YOUTH["gforms_config_id"]) == "edited gf"
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1::uuid",
                                   YOUTH["gforms_config_id"]) == 2
        assert await conn.fetchval("SELECT count(*) FROM config") == 6
    run_db(body)


def test_seed_skips_when_tenant_missing():
    async def runner():
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            await conn.execute(_schema_sql())
            await seed_feedback(conn)  # no tenant row: logs a warning, inserts nothing
            for table in ("tenant", "leader_category", "programs", "config", "config_version"):
                assert await conn.fetchval(f"SELECT count(*) FROM {table}") == 0, table
        finally:
            await tx.rollback()
            await conn.close()
    asyncio.run(runner())


def test_feedback_submissions_unique_submission_tenant():
    async def body(conn):
        await _add_submission(conn)
        await _add_feedback_submission(conn)
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO feedback_submissions (submission_id, tenant_code, source, completion_status) "
            "VALUES ('S1', 'mitra', 'mitra', 'COMPLETED')")
    run_db(body)


def test_feedback_submissions_source_ref_unique_but_nulls_allowed():
    async def body(conn):
        for i in (1, 2):
            await _add_submission(conn, f"S{i}", f"TOKEN{i}")
            await _add_feedback_submission(conn, f"S{i}")  # two NULL source_refs are fine
        await conn.execute("UPDATE feedback_submissions SET source_ref = 'r1' WHERE submission_id = 'S1'")
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "UPDATE feedback_submissions SET source_ref = 'r1' WHERE submission_id = 'S2'")
    run_db(body)


def test_completion_status_check_rejects_other_values():
    async def body(conn):
        await _add_submission(conn)
        await _expect_violation(
            conn, asyncpg.CheckViolationError,
            "INSERT INTO feedback_submissions (submission_id, tenant_code, source, completion_status) "
            "VALUES ('S1', 'mitra', 'mitra', 'DONE')")
        await _add_feedback_submission(conn, status="IN_PROGRESS")
    run_db(body)


def test_feedbacks_unique_submission_tenant_sequence():
    async def body(conn):
        await _add_submission(conn)
        await _add_feedback(conn, seq=1)
        await _add_feedback(conn, seq=2)
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO feedbacks (submission_id, tenant_code, session_id, source, sequence, message) "
            "VALUES ('S1', 'mitra', 'TOKEN1', 'mitra', 2, 'dup')")
    run_db(body)


def test_feedbacks_requires_existing_submission():
    async def body(conn):
        await _expect_violation(
            conn, asyncpg.ForeignKeyViolationError,
            "INSERT INTO feedbacks (submission_id, tenant_code, session_id, source, sequence, message) "
            "VALUES ('NOPE', 'mitra', 'T', 'mitra', 1, 'm')")
    run_db(body)


def test_duplicate_config_rejected_including_null_bot_id_and_null_program():
    async def body(conn):
        # same (source, type, program, bot) as the seeded Youth config, different id and name
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, bot_id) "
            "VALUES ('mitra', 'dup', 'feedback_upload', $1::uuid, 'mitra', $2)", YOUTH["program_id"], YOUTH["bot_id"])
        # NULL bot_id twice for the same program: a plain UNIQUE would let this through
        # (the Google Forms row of the Youth program is seeded, so this is the second one)
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, bot_id) "
            "VALUES ('google_forms', 'gf2', 'feedback_upload', $1::uuid, 'mitra', NULL)", YOUTH["program_id"])
        # NULL program (source default) twice
        await conn.execute(
            "INSERT INTO config (config_source, name, config_type, tenant_code) "
            "VALUES ('google_forms', 'default', 'feedback_upload', 'mitra')")
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config (config_source, name, config_type, tenant_code) "
            "VALUES ('google_forms', 'default2', 'feedback_upload', 'mitra')")
        # a different bot under the same program is a different config
        await conn.execute(
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, bot_id) "
            "VALUES ('mitra', 'other bot', 'feedback_upload', $1::uuid, 'mitra', '999')", YOUTH["program_id"])
    run_db(body)


def test_config_source_check_rejects_other_values():
    async def body(conn):
        await _expect_violation(
            conn, asyncpg.CheckViolationError,
            "INSERT INTO config (config_source, name, config_type, tenant_code) "
            "VALUES ('csv', 'x', 'feedback_upload', 'mitra')")
    run_db(body)


def test_two_active_versions_of_one_config_rejected():
    async def body(conn):
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config_version (config_id, version, is_active, config) "
            "VALUES ($1::uuid, 2, TRUE, '{}'::jsonb)", YOUTH["config_id"])
        # an inactive second version is fine, and so is the same version number twice -> rejected
        await conn.execute(
            "INSERT INTO config_version (config_id, version, is_active, config) "
            "VALUES ($1::uuid, 2, FALSE, '{}'::jsonb)", YOUTH["config_id"])
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config_version (config_id, version, is_active, config) "
            "VALUES ($1::uuid, 2, FALSE, '{}'::jsonb)", YOUTH["config_id"])
    run_db(body)


def test_deleting_submission_cascades_to_feedback_tables():
    async def body(conn):
        await _add_submission(conn)
        await _add_feedback_submission(conn)
        q = await _add_feedback(conn, seq=1)
        await _add_feedback(conn, seq=2, parent=q)
        assert await conn.fetchval("SELECT count(*) FROM feedbacks") == 2
        await conn.execute("DELETE FROM submissions WHERE submission_id = 'S1' AND tenant_code = 'mitra'")
        assert await conn.fetchval("SELECT count(*) FROM feedback_submissions") == 0
        assert await conn.fetchval("SELECT count(*) FROM feedbacks") == 0
    run_db(body)


def test_deleting_parent_question_cascades_to_its_answers():
    async def body(conn):
        await _add_submission(conn)
        q = await _add_feedback(conn, seq=1)
        a = await _add_feedback(conn, seq=2, parent=q)
        await _add_feedback(conn, seq=3, parent=a)  # chained child
        other = await _add_feedback(conn, seq=4)
        await conn.execute("DELETE FROM feedbacks WHERE id = $1", q)
        remaining = [r["id"] for r in await conn.fetch("SELECT id FROM feedbacks")]
        assert remaining == [other]
    run_db(body)


def test_deleting_program_nulls_feedbacks_program_and_cascades_config():
    async def body(conn):
        await _add_submission(conn)
        fid = await _add_feedback(conn)
        await conn.execute("UPDATE feedbacks SET program_id = $1::uuid WHERE id = $2", YOUTH["program_id"], fid)
        await conn.execute("DELETE FROM programs WHERE id = $1::uuid", YOUTH["program_id"])
        assert await conn.fetchval("SELECT program_id FROM feedbacks WHERE id = $1", fid) is None
        assert await conn.fetchval("SELECT count(*) FROM config WHERE id = $1::uuid", YOUTH["config_id"]) == 0
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1::uuid",
                                   YOUTH["config_id"]) == 0
    run_db(body)


def test_feedback_indexes_exist():
    async def body(conn):
        names = {r["indexname"] for r in await conn.fetch(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")}
        assert {"idx_feedbacks_program_question", "idx_feedbacks_parent", "idx_feedbacks_source",
                "uq_config_source_type_program_bot", "uq_config_version_active"} <= names
    run_db(body)
