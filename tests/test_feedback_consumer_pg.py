"""
Real-Postgres tests for the feedback branch of insert_or_update_submission / delete_submission.

Skipped unless FEEDBACK_TEST_DATABASE_URL is set to a DSN of a database that already has schema.sql
applied (e.g. postgresql://postgres@localhost:54329/fb_d_x). The tests seed tenant 'mitra' plus a leader
category / program themselves and use random submission ids, and clean up the rows they create.
Synthetic data only.
"""
import asyncio
import os
import uuid

import asyncpg
import pytest

from app.database.operations import (
    FeedbackUpdateNotSupportedError,
    delete_submission,
    insert_or_update_submission,
)

DSN = os.environ.get("FEEDBACK_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(DSN is None, reason="FEEDBACK_TEST_DATABASE_URL not set")

LEADER_ID = "0a0a0a0a-0000-4000-8000-00000000d001"
PROGRAM_ID = "0a0a0a0a-0000-4000-8000-00000000d002"


def _run(body):
    """Runs `body(conn)` on a fresh connection with tenant/leader/program seeded; cleans up afterwards."""
    async def main():
        conn = await asyncpg.connect(DSN)
        created = []
        try:
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await conn.execute(
                "INSERT INTO leader_category (id, name, tenant_code) VALUES ($1, 'Youth', 'mitra') ON CONFLICT (id) DO NOTHING",
                LEADER_ID)
            await conn.execute(
                "INSERT INTO programs (id, leaders_id, name, tenant_code) VALUES ($1, $2, '1M Voice', 'mitra') "
                "ON CONFLICT (id) DO NOTHING", PROGRAM_ID, LEADER_ID)
            return await body(conn, created)
        finally:
            for sid in created:
                await conn.execute("DELETE FROM submissions WHERE submission_id = $1 AND tenant_code = 'mitra'", sid)
            await conn.close()
    return asyncio.run(main())


def _msg(sequence, message, parent=None, key=None, follow_up=False, translated=None):
    return {
        "sequence": sequence, "questionKey": key, "isFollowUp": follow_up, "message": message,
        "translatedMessage": translated, "messageAt": f"2026-10-15T08:{sequence:02d}:00.000Z",
        "parentSequence": parent,
    }


# Q1, A (parent 1), follow-up Q3, two-message answer (4, 5 -> parent 3), closing 6.
SIX = [
    _msg(1, "How old are you?", key="AGE_IDENTIFICATION", translated="Q"),
    _msg(2, "17", parent=1),
    _msg(3, "What would you change?", key="Q_CHANGE", follow_up=True),
    _msg(4, "More buses", parent=3),
    _msg(5, "and a library", parent=3),
    _msg(6, "Thank you!", key="THANK_YOU"),
]


def _event(submission_id, session_id, messages, completion="COMPLETED", **tags):
    base_tags = {
        "programId": PROGRAM_ID, "programName": "1M Voice",
        "leaderCategoryId": LEADER_ID, "leaderCategoryName": "Youth",
        "state": "Karnataka", "district": None, "source": "mitra",
    }
    base_tags.update(tags)
    return {
        "submissionId": submission_id, "submissionType": "feedback", "sessionId": session_id,
        "tenantCode": "mitra", "eventType": "create", "eventPublishedAt": "2026-10-15T09:00:00.000Z",
        "tags": base_tags,
        "data": {"source": "mitra", "language": "en", "completionStatus": completion,
                 "submissionDate": "2026-10-15T08:01:00.000Z", "messages": messages},
    }


def _ids():
    sid = f"t{uuid.uuid4().hex[:12]}"
    return sid, f"sess-{uuid.uuid4().hex}"


async def _snapshot(conn, sid):
    sub = await conn.fetchrow(
        "SELECT id, status, state, district, submission_type, program_id, leader_id, session_id, updated_at "
        "FROM submissions WHERE submission_id = $1 AND tenant_code = 'mitra'", sid)
    fs = await conn.fetchrow(
        "SELECT id, source, language, completion_status, source_ref, updated_at FROM feedback_submissions "
        "WHERE submission_id = $1 AND tenant_code = 'mitra'", sid)
    fbs = await conn.fetch(
        "SELECT id, sequence, parent_id, message, updated_at, created_at FROM feedbacks "
        "WHERE submission_id = $1 AND tenant_code = 'mitra' ORDER BY sequence", sid)
    return dict(sub) if sub else None, dict(fs) if fs else None, [dict(r) for r in fbs]


def test_create_session_resolves_parents():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        res = await insert_or_update_submission(conn, _event(sid, sess, SIX))
        assert res["changed"] is True and res["status"] == "pending"
        assert res["messages_inserted"] == 6 and res["parents_set"] == 3
        sub, fs, fbs = await _snapshot(conn, sid)
        assert sub["submission_type"] == "feedback" and sub["state"] == "Karnataka" and sub["district"] is None
        assert str(sub["program_id"]) == PROGRAM_ID and str(sub["leader_id"]) == LEADER_ID
        assert sub["session_id"] == sess and sub["status"] == "pending"
        assert fs["source"] == "mitra" and fs["completion_status"] == "COMPLETED" and fs["source_ref"] is None
        by_seq = {r["sequence"]: r for r in fbs}
        assert sorted(by_seq) == [1, 2, 3, 4, 5, 6]
        assert by_seq[1]["parent_id"] is None and by_seq[3]["parent_id"] is None and by_seq[6]["parent_id"] is None
        assert by_seq[2]["parent_id"] == by_seq[1]["id"]
        assert by_seq[4]["parent_id"] == by_seq[3]["id"] and by_seq[5]["parent_id"] == by_seq[3]["id"]
        row = await conn.fetchrow(
            "SELECT is_follow_up, question_key, translated_message, language, message_at, program_id, session_id "
            "FROM feedbacks WHERE submission_id = $1 AND sequence = 3", sid)
        assert row["is_follow_up"] is True and row["question_key"] == "Q_CHANGE"
        assert row["language"] == "en" and row["message_at"] is not None
        assert str(row["program_id"]) == PROGRAM_ID and row["session_id"] == sess
    _run(body)


def test_identical_event_changes_nothing():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1", sid)
        before = await _snapshot(conn, sid)
        res = await insert_or_update_submission(conn, _event(sid, sess, SIX))
        after = await _snapshot(conn, sid)
        assert res["changed"] is False and res["status"] == "success"
        assert res["messages_inserted"] == 0 and res["messages_updated"] == 0 and res["parents_set"] == 0
        assert after == before  # ids, parent ids and every updated_at unchanged
    _run(body)


def test_grown_event_adds_only_new_rows_and_completes():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        first = SIX[:5]  # no closing row yet
        await insert_or_update_submission(conn, _event(sid, sess, first, completion="IN_PROGRESS"))
        await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1", sid)
        before = await _snapshot(conn, sid)
        assert before[1]["completion_status"] == "IN_PROGRESS" and len(before[2]) == 5

        # re-upload of the whole grown session: old rows 1..5 re-sent unchanged plus 6 and 7 (answer to 6)
        grown = SIX + [_msg(7, "Bye", parent=6)]
        res = await insert_or_update_submission(conn, _event(sid, sess, grown, completion="COMPLETED"))
        after = await _snapshot(conn, sid)
        assert res["changed"] is True and res["messages_inserted"] == 2 and res["messages_updated"] == 0
        assert res["status"] == "pending" and after[0]["status"] == "pending"  # sent back to processing
        assert after[1]["completion_status"] == "COMPLETED" and after[1]["id"] == before[1]["id"]
        assert after[1]["updated_at"] >= before[1]["updated_at"]
        assert [r["sequence"] for r in after[2]] == [1, 2, 3, 4, 5, 6, 7]
        for old, new in zip(before[2], after[2][:5]):
            assert new == old  # same id, parent, text and updated_at
        assert after[2][6]["parent_id"] == after[2][5]["id"]

        # delta-only event: only the new tail, parent resolved from the stored row
        tail = [_msg(8, "Really bye", parent=6)]
        res = await insert_or_update_submission(conn, _event(sid, sess, tail, completion="COMPLETED"))
        assert res["messages_inserted"] == 1 and res["parents_set"] == 1
        final = await _snapshot(conn, sid)
        assert final[2][7]["parent_id"] == after[2][5]["id"]
        assert final[2][:7] == after[2]
    _run(body)


def test_changed_text_updates_in_place_keeping_id():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        before = await _snapshot(conn, sid)
        edited = [dict(m) for m in SIX]
        edited[3]["message"] = "Many more buses"
        res = await insert_or_update_submission(conn, _event(sid, sess, edited))
        after = await _snapshot(conn, sid)
        assert res["messages_updated"] == 1 and res["messages_inserted"] == 0
        assert after[2][3]["id"] == before[2][3]["id"] and after[2][3]["message"] == "Many more buses"
        assert after[2][3]["updated_at"] >= before[2][3]["updated_at"]
        for i in (0, 1, 2, 4, 5):
            assert after[2][i] == before[2][i]
    _run(body)


def test_stale_in_progress_does_not_downgrade_completed():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX, completion="COMPLETED"))
        before = await _snapshot(conn, sid)
        res = await insert_or_update_submission(conn, _event(sid, sess, SIX[:4], completion="IN_PROGRESS"))
        after = await _snapshot(conn, sid)
        assert after[1]["completion_status"] == "COMPLETED"
        assert res["changed"] is False
        assert after == before
    _run(body)


def test_delete_cascades():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        assert await delete_submission(conn, sid, "mitra") is True
        for table in ("submissions", "feedback_submissions", "feedbacks"):
            n = await conn.fetchval(f"SELECT count(*) FROM {table} WHERE submission_id = $1 AND tenant_code = 'mitra'", sid)
            assert n == 0, table
    _run(body)


def test_update_event_is_rejected_and_writes_nothing():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        event = _event(sid, sess, SIX)
        event["eventType"] = "update"
        with pytest.raises(FeedbackUpdateNotSupportedError) as exc:
            await insert_or_update_submission(conn, event)
        assert sid in str(exc.value) and "mitra" in str(exc.value)
        assert await _snapshot(conn, sid) == (None, None, [])
    _run(body)


def test_unresolvable_parent_rolls_back_whole_event():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        bad = [_msg(1, "Q", key="K"), _msg(3, "A", parent=2)]  # sequence 2 never exists
        with pytest.raises(ValueError, match="parentSequence"):
            await insert_or_update_submission(conn, _event(sid, sess, bad))
        assert await _snapshot(conn, sid) == (None, None, [])
    _run(body)


def test_session_id_reuse_by_another_submission_is_rejected():
    async def body(conn, created):
        sid, sess = _ids()
        sid2, _ = _ids()
        created.extend([sid, sid2])
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        with pytest.raises(ValueError, match="session"):
            await insert_or_update_submission(conn, _event(sid2, sess, SIX))
        # same submission id arriving with a different session token is also rejected
        with pytest.raises(ValueError, match="already belongs to session"):
            await insert_or_update_submission(conn, _event(sid, f"sess-{uuid.uuid4().hex}", SIX))
        assert (await _snapshot(conn, sid2)) == (None, None, [])
    _run(body)
