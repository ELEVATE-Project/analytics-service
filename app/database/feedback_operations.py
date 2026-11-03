"""
Read-only lookups for the `feedback` upload (program/leader resolution and the active upload config).

Unlike the story/discussion lookup in app/api/services/uploads.py, both the leader category and the
program are matched by NAME AND TENANT, and a miss is reported as None (never fabricated).
"""
import json
import logging
from typing import Any, Dict, List, Optional

from app.database.db import db
from app.services.feedback_constants import CONFIG_TYPE_FEEDBACK_UPLOAD, SOURCE_MITRA

logger = logging.getLogger("analytics_service.database.feedback_operations")

# Ids per ANY($n::text[]) query; keeps a very large upload's lookup to a few round trips.
_LOOKUP_CHUNK_SIZE = 5000


def _as_dict(value: Any) -> Dict[str, Any]:
    """asyncpg returns JSONB as str unless a codec is registered; accept both."""
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError(f"config_version.config must be a JSON object, got {type(value).__name__}")
    return value


async def get_program_and_leader(
    program_name: str,
    leader_category: str,
    tenant_code: str,
) -> Optional[Dict[str, Any]]:
    """
    Resolve a feedback upload's program + persona (leader category), by name and tenant.

    Returns {"program": {"id", "name"}, "leader": {"id", "name"}, "tenant_code"} or None when either
    row is missing for the tenant.
    """
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        leader_row = await conn.fetchrow(
            "SELECT id, name FROM leader_category WHERE name = $1 AND tenant_code = $2 LIMIT 1",
            leader_category,
            tenant_code,
        )
        if leader_row is None:
            return None
        program_row = await conn.fetchrow(
            "SELECT id, name FROM programs WHERE name = $1 AND leaders_id = $2 AND tenant_code = $3 LIMIT 1",
            program_name,
            leader_row["id"],
            tenant_code,
        )
        if program_row is None:
            return None

    return {
        "program": {"id": str(program_row["id"]), "name": program_row["name"]},
        "leader": {"id": str(leader_row["id"]), "name": leader_row["name"]},
        "tenant_code": tenant_code,
    }


async def get_active_feedback_config(
    program_id: str,
    bot_id: Optional[str],
    config_source: str = SOURCE_MITRA,
) -> Optional[Dict[str, Any]]:
    """
    Load the ACTIVE config_version of the `config` row for (config_source, 'feedback_upload',
    program_id, bot_id). A None bot_id matches a config row whose bot_id is NULL (Google Forms
    configs), a bot_id string matches exactly that bot (Mitra).

    Returns {"config_id", "config_version_id", "version", "config": {...}} (ids as str) or None
    when no such config / no active version exists.
    """
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT c.id AS config_id, v.id AS config_version_id, v.version, v.config
            FROM config c
            JOIN config_version v ON v.config_id = c.id AND v.is_active
            WHERE c.config_source = $1
              AND c.config_type = $2
              AND c.program_id = $3::uuid
              AND c.bot_id IS NOT DISTINCT FROM $4::text
            ORDER BY v.version DESC
            LIMIT 1
            """,
            config_source,
            CONFIG_TYPE_FEEDBACK_UPLOAD,
            program_id,
            bot_id,
        )
    if row is None:
        return None

    return {
        "config_id": str(row["config_id"]),
        "config_version_id": str(row["config_version_id"]),
        "version": row["version"],
        "config": _as_dict(row["config"]),
    }


async def get_known_source_refs(tenant_code: str, refs: List[str]) -> set:
    """
    Which of `refs` (Google Forms natural keys) are already stored for the tenant: the subset of
    feedback_submissions.source_ref values. One query per chunk of _LOOKUP_CHUNK_SIZE refs.
    """
    unique_refs = sorted(set(refs))
    if not unique_refs:
        return set()
    if not db.pool:
        await db.connect()

    known: set = set()
    async with db.pool.acquire() as conn:
        for start in range(0, len(unique_refs), _LOOKUP_CHUNK_SIZE):
            rows = await conn.fetch(
                """
                SELECT source_ref
                FROM feedback_submissions
                WHERE tenant_code = $1 AND source_ref = ANY($2::text[])
                """,
                tenant_code,
                unique_refs[start:start + _LOOKUP_CHUNK_SIZE],
            )
            known.update(row["source_ref"] for row in rows)
    return known


async def get_mitra_session_state(tenant_code: str, submission_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Stored state of the Mitra sessions with these submission ids, for the delta check:
    {submission_id: {"session_id", "max_sequence", "message_count", "completion_status"}}.
    Sessions that are not stored are absent. A stored session with no feedbacks rows reports
    message_count 0 and max_sequence 0.
    """
    unique_ids = sorted(set(submission_ids))
    if not unique_ids:
        return {}
    if not db.pool:
        await db.connect()

    state: Dict[str, Dict[str, Any]] = {}
    async with db.pool.acquire() as conn:
        for start in range(0, len(unique_ids), _LOOKUP_CHUNK_SIZE):
            rows = await conn.fetch(
                """
                SELECT fs.submission_id,
                       s.session_id,
                       fs.completion_status,
                       COUNT(f.id) AS message_count,
                       COALESCE(MAX(f.sequence), 0) AS max_sequence
                FROM feedback_submissions fs
                JOIN submissions s
                  ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
                LEFT JOIN feedbacks f
                  ON f.submission_id = fs.submission_id AND f.tenant_code = fs.tenant_code
                WHERE fs.tenant_code = $1 AND fs.submission_id = ANY($2::text[])
                GROUP BY fs.submission_id, s.session_id, fs.completion_status
                """,
                tenant_code,
                unique_ids[start:start + _LOOKUP_CHUNK_SIZE],
            )
            for row in rows:
                state[row["submission_id"]] = {
                    "session_id": row["session_id"],
                    "max_sequence": int(row["max_sequence"]),
                    "message_count": int(row["message_count"]),
                    "completion_status": row["completion_status"],
                }
    return state
