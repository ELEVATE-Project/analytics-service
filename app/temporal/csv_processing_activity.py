import json
import logging
import traceback
import uuid
from datetime import datetime
from typing import Dict, Any, List
from temporalio import activity
import pandas as pd

from app.config import settings
from app.database.db import db
from app.database import operations as csv_upload_repo
from app.api.services.uploads import load_csv, rows_to_json, split_csv
from app.api.validators.uploads import validate_columns
from app.storage.gcs import fetch_csv
from kafka import KafkaProducer

logger = logging.getLogger("analytics_service.temporal.csv_processing_activity")

_producer: KafkaProducer | None = None


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: (
                v.encode("utf-8") if isinstance(v, str) else json.dumps(v).encode("utf-8")
            ),
            acks="all",
            retries=3,
        )
    return _producer


def _push_row(payload: str, key: str | None = None) -> None:
    producer = _get_producer()
    future = producer.send(
        settings.KAFKA_TOPIC_INGESTION,
        value=payload,
        key=key.encode("utf-8") if key else None,
    )
    future.get(timeout=10)


def _flush_producer() -> None:
    if _producer is not None:
        _producer.flush()


@activity.defn
async def csv_fetch_and_validate_activity(record_id: int) -> bool:
    """
    Temporal activity to fetch the CSV file from cloud storage and update status.
    """
    record = await csv_upload_repo.get_record(record_id)
    if not record:
        raise ValueError(f"Record {record_id} not found in database.")

    cloud_storage_path = record["cloud_storage_path"]

    try:
        # Fetch from GCS
        csv_file = fetch_csv(cloud_storage_path)
        df = load_csv(csv_file)
    except Exception as exc:
        logger.exception("Failed to fetch/load CSV for record %s", record_id)
        error_meta = {
            "stage": "CSV Fetching",
            "error": "Failed to fetch/load CSV from GCS",
            "exception": str(exc),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        await csv_upload_repo.update_status(record_id, "on_hold", error_meta)
        return False

    is_valid, errors = validate_columns(df, record["report_type"])
    if not is_valid:
        logger.warning("Validation failed for record %s: %s", record_id, errors)
        error_meta = {
            "stage": "CSV Column Validation",
            "error": "Invalid CSV schema",
            "validation_errors": errors,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        await csv_upload_repo.update_status(record_id, "on_hold", error_meta)
        return False

    await csv_upload_repo.update_status(record_id, "in_progress")
    return True


@activity.defn
async def csv_push_to_kafka_activity(record_id: int) -> int:
    """
    Temporal activity to process the CSV file row by row and publish
    individual messages to Kafka. Returns the count of rows pushed.
    """
    record = await csv_upload_repo.get_record(record_id)
    if not record:
        raise ValueError(f"Record {record_id} not found in database.")

    report_type = record["report_type"]
    cloud_storage_path = record["cloud_storage_path"]

    # Load CSV
    csv_file = fetch_csv(cloud_storage_path)
    df = load_csv(csv_file)

    is_valid, errors = validate_columns(df, report_type)
    if not is_valid:
        logger.warning("Kafka push blocked for record %s due to invalid columns: %s", record_id, errors)
        error_meta = {
            "stage": "CSV Column Validation",
            "error": "Invalid CSV schema - aborting Kafka push",
            "validation_errors": errors,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        await csv_upload_repo.update_status(record_id, "on_hold", error_meta)
        raise ValueError("CSV validation failed for Kafka push")

    # Fetch programs / leader categories info from Postgres once for context mapping
    program_info = None
    leader_info = None
    # Use tenant_code from the upload payload (stored in meta_data) as primary source,
    # falling back to DB lookup and then to "mitra" as last resort.
    record_meta = record.get("meta_data") or {}
    if isinstance(record_meta, str):
        import json as _json
        try:
            record_meta = _json.loads(record_meta)
        except _json.JSONDecodeError:
            record_meta = {}

    if not isinstance(record_meta, dict):
        record_meta = {}

    tenant_code = record_meta.get("tenant_code") or "mitra"

    try:
        async with db.pool.acquire() as conn:
            leader_row = await conn.fetchrow(
                "SELECT id, name, description, tenant_code FROM leader_category WHERE name = $1 LIMIT 1",
                record.get("leader_category")
            )
            if leader_row:
                leader_info = {
                    "id": str(leader_row["id"]),
                    "name": leader_row["name"],
                    "description": leader_row["description"],
                }
                tenant_code = leader_row["tenant_code"]

            if leader_row:
                program_row = await conn.fetchrow(
                    "SELECT id, name, description, tenant_code, leaders_id FROM programs WHERE name = $1 AND leaders_id = $2 LIMIT 1",
                    record.get("program_name"), leader_row["id"]
                )
            else:
                program_row = await conn.fetchrow(
                    "SELECT id, name, description, tenant_code, leaders_id FROM programs WHERE name = $1 LIMIT 1",
                    record.get("program_name")
                )

            if program_row:
                program_info = {
                    "id": str(program_row["id"]),
                    "name": program_row["name"],
                    "description": program_row["description"],
                }
                tenant_code = program_row.get("tenant_code", tenant_code)

            if program_row and not leader_info:
                leader_row_from_program = await conn.fetchrow(
                    "SELECT id, name, description, tenant_code FROM leader_category WHERE id = $1 LIMIT 1",
                    program_row["leaders_id"]
                )
                if leader_row_from_program:
                    leader_info = {
                        "id": str(leader_row_from_program["id"]),
                        "name": leader_row_from_program["name"],
                        "description": leader_row_from_program["description"],
                    }
                    tenant_code = leader_row_from_program.get("tenant_code", tenant_code)
    except Exception as db_exc:
        logger.warning("Failed to query program/leader category metadata from DB: %s", db_exc)

    # Fallbacks if DB query returned nothing
    if not leader_info:
        leader_info = {
            "id": str(uuid.uuid4()),
            "name": record.get("leader_category") or "District Leader",
            "description": f"Leader category: {record.get('leader_category') or 'District Leader'}",
        }
    if not program_info:
        program_info = {
            "id": str(uuid.uuid4()),
            "name": record.get("program_name") or "My Program",
            "description": f"Program: {record.get('program_name') or 'My Program'}",
        }

    metadata = {
        "programInfo": program_info,
        "LeaderCategoryInfo": leader_info,
        "tenantCode": tenant_code,
    }

    chunks = split_csv(df)
    pushed = 0
    record_number = 0

    try:
        for chunk in chunks:
            for payload in rows_to_json(chunk, report_type, metadata=metadata):
                record_number += 1
                _push_row(payload, key=f"{record_id}-{pushed}")
                pushed += 1
        _flush_producer()
    except Exception as exc:
        logger.exception("Kafka push failed for record %s at record number %s", record_id, record_number)
        error_meta = {
            "stage": "Kafka Publishing",
            "error": "Failed to publish record",
            "record_number": record_number,
            "exception": str(exc),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        await csv_upload_repo.update_status(record_id, "on_hold", error_meta)
        raise exc

    return pushed


@activity.defn
async def csv_update_status_activity(params: Dict[str, Any]) -> None:
    """
    Temporal activity to update the overall processing status of a csv_upload in PostgreSQL.
    """
    record_id = params["record_id"]
    status = params["status"]
    meta_data = params.get("meta_data")
    await csv_upload_repo.update_status(record_id, status, meta_data)


@activity.defn
async def fetch_pending_csv_uploads_activity() -> List[int]:
    """
    Temporal activity to fetch the IDs of all pending csv_upload records.
    """
    records = await csv_upload_repo.list_by_status("pending")
    return [r["id"] for r in records]
