import json
import logging

from app.services.feedback_constants import (
    CONFIG_TYPE_FEEDBACK_UPLOAD,
    FEEDBACK_PROGRAM_NAME,
    FEEDBACK_TENANT_CODE,
    PERSONAS,
    SOURCE_MITRA,
    mitra_config_v1,
)

logger = logging.getLogger("analytics_service.db")

_SEED_CREATED_BY = "seed"
_SEED_CHANGE_NOTE = "Initial format"


async def seed_feedback(conn) -> None:
    """
    Seeds the Mitra `feedback` report type: one leader category, one `1M Voice` program,
    one upload config and its version-1 settings per persona (Youth / Parent / General Public).

    Idempotent and never overwrites: every insert is ON CONFLICT DO NOTHING, so edits made to
    these rows (or new config versions added) without a deploy survive restarts. leader_category,
    programs and config have no unique key on name, so they are keyed by the fixed ids in
    app.services.feedback_constants. Runs inside initialize_schema's advisory lock.
    """
    tenant = await conn.fetchval("SELECT 1 FROM tenant WHERE code = $1", FEEDBACK_TENANT_CODE)
    if tenant is None:
        # The tenant is an environment-level row; never create it here.
        logger.warning(
            f"Feedback seed skipped: tenant {FEEDBACK_TENANT_CODE!r} does not exist in this database."
        )
        return

    for persona, p in PERSONAS.items():
        await conn.execute(
            """
            INSERT INTO leader_category (id, name, tenant_code)
            VALUES ($1::uuid, $2, $3)
            ON CONFLICT (id) DO NOTHING
            """,
            p["leader_category_id"], persona, FEEDBACK_TENANT_CODE,
        )
        await conn.execute(
            """
            INSERT INTO programs (id, leaders_id, name, tenant_code)
            VALUES ($1::uuid, $2::uuid, $3, $4)
            ON CONFLICT (id) DO NOTHING
            """,
            p["program_id"], p["leader_category_id"], FEEDBACK_PROGRAM_NAME, FEEDBACK_TENANT_CODE,
        )
        # No conflict target: also skips a row an operator already created for the same
        # (source, type, program, bot) under a different id (the unique index would reject it).
        await conn.execute(
            """
            INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code,
                                leader_id, bot_id, bot_name)
            VALUES ($1::uuid, $2, $3, $4, $5::uuid, $6, NULL, $7, $8)
            ON CONFLICT DO NOTHING
            """,
            p["config_id"], SOURCE_MITRA, p["config_name"], CONFIG_TYPE_FEEDBACK_UPLOAD,
            p["program_id"], FEEDBACK_TENANT_CODE, p["bot_id"], p["bot_name"],
        )
        # Version 1 is only seeded while the config has no version at all: if it was edited,
        # superseded (v2 active) or v1 removed, re-inserting an active v1 would clobber that
        # (or trip the one-active-version index). Also a no-op if the config row above was skipped.
        await conn.execute(
            """
            INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
            SELECT $1::uuid, 1, TRUE, $2::jsonb, $3, $4
            WHERE EXISTS (SELECT 1 FROM config WHERE id = $1::uuid)
              AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = $1::uuid)
            ON CONFLICT (config_id, version) DO NOTHING
            """,
            p["config_id"], json.dumps(mitra_config_v1(persona)), _SEED_CREATED_BY, _SEED_CHANGE_NOTE,
        )
