"""
Read-only lookups for the `feedback` upload (program/leader resolution and the active upload config).

Unlike the story/discussion lookup in app/api/services/uploads.py, both the leader category and the
program are matched by NAME AND TENANT, and a miss is reported as None (never fabricated).
"""
import json
import logging
from typing import Any, Dict, Optional

from app.database.db import db
from app.services.feedback_constants import CONFIG_TYPE_FEEDBACK_UPLOAD, SOURCE_MITRA

logger = logging.getLogger("analytics_service.database.feedback_operations")


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
    bot_id: str,
    config_source: str = SOURCE_MITRA,
) -> Optional[Dict[str, Any]]:
    """
    Load the ACTIVE config_version of the `config` row for (config_source, 'feedback_upload',
    program_id, bot_id).

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
              AND c.bot_id = $4
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
