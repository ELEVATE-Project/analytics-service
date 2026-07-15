"""
All SQL for csv_upload lives here.

API routes and the workflow activities both import from this module instead of
writing their own queries, so the table's shape only needs to change in
one place.

Uses the existing asyncpg connection pool from app.database.db.
"""

import json
import logging
from typing import Any, Optional

from app.database.db import db

logger = logging.getLogger("analytics_service.csv_pipeline.csv_upload_repo")


async def check_duplicate_file(
    program_name: str,
    leader_category: str,
    report_type: str,
    file_name: str,
    file_size: int,
) -> bool:
    """Return True if a matching file already exists in the tracker."""
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        exists = await conn.fetchval(
            """
            SELECT 1 FROM csv_uploads
            WHERE program_name = $1
              AND leader_category = $2
              AND report_type = $3
              AND file_name = $4
              AND file_size = $5
            LIMIT 1
            """,
            program_name,
            leader_category,
            report_type,
            file_name,
            file_size,
        )
        return exists is not None


async def insert_upload_record(
    report_type: str,
    program_name: str,
    leader_category: str,
    cloud_storage_path: str,
    file_name: str | None = None,
    file_size: int | None = None,
    meta_data: dict[str, Any] | None = None,
) -> int:
    """Insert a new row with status='pending'. Returns the new row's id."""
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO csv_uploads
                (report_type, program_name, leader_category, cloud_storage_path,
                 file_name, file_size, meta_data, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, 'pending')
            RETURNING id
            """,
            report_type,
            program_name,
            leader_category,
            cloud_storage_path,
            file_name,
            file_size,
            json.dumps(meta_data or {}),
        )
        record_id = row["id"]
        logger.info("Inserted csv_uploads record %s (status=pending)", record_id)
        return record_id


async def claim_pending_records(batch_size: int) -> list[dict]:
    """
    Atomically claim up to `batch_size` pending rows by flipping them to
    'in_progress' and returning them, using FOR UPDATE SKIP LOCKED.
    """
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE csv_uploads
            SET status = 'in_progress'
            WHERE id IN (
                SELECT id FROM csv_uploads
                WHERE status = 'pending'
                ORDER BY created_at
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, report_type, program_name, leader_category,
                      cloud_storage_path, meta_data, status,
                      created_at, updated_at
            """,
            batch_size,
        )
        records = [dict(r) for r in rows]
        logger.info("Claimed %d pending record(s)", len(records))
        return records


async def get_record(record_id: int) -> Optional[dict]:
    """Fetch a single tracker record by id."""
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM csv_uploads WHERE id = $1",
            record_id,
        )
        return dict(row) if row else None


async def update_status(
    record_id: int,
    status: str,
    meta_data: dict[str, Any] | None = None,
) -> None:
    """
    Update status, optionally merging new keys into meta_data.
    """
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        if meta_data is not None:
            await conn.execute(
                """
                UPDATE csv_uploads
                SET status = $1,
                    meta_data = meta_data || $2::jsonb
                WHERE id = $3
                """,
                status,
                json.dumps(meta_data),
                record_id,
            )
        else:
            await conn.execute(
                "UPDATE csv_uploads SET status = $1 WHERE id = $2",
                status,
                record_id,
            )
        logger.info("Updated record %s → status=%s", record_id, status)


async def list_by_status(status: str) -> list[dict]:
    """List all tracker records with a given status."""
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM csv_uploads WHERE status = $1 ORDER BY created_at",
            status,
        )
        return [dict(r) for r in rows]
