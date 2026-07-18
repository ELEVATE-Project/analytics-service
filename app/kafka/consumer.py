import asyncio
import hashlib
import json
import logging
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from temporalio.client import Client

from app.config import settings
from app.database.db import db
from app.database.operations import (
    insert_or_update_submission,
    delete_submission,
    update_submission_status
)
from app.temporal.workflows import ConfigDrivenProcessingWorkflow

logger = logging.getLogger("analytics_service.kafka.consumer")


def _get_nested(obj: dict, dotted_path: str):
    """Walks a dotted path (e.g. 'data.pdfUrls.original') through nested dicts.
    Returns (value, found) — found is False if any segment is missing or not a dict."""
    current = obj
    for segment in dotted_path.split("."):
        if not isinstance(current, dict) or segment not in current:
            return None, False
        current = current[segment]
    return current, True


def _is_empty(value) -> bool:
    """True only for None, "", [], {} — explicitly NOT for 0 or False, which are
    falsy-but-valid values (unlike a bare `not value` check)."""
    if value is None:
        return True
    if isinstance(value, (str, list, dict, tuple)) and len(value) == 0:
        return True
    return False


def _emptiness_label(value) -> str:
    """Distinguishes *why* a value tripped _is_empty(), for precise problem messages."""
    return "null" if value is None else "empty"


def _payload_fingerprint(raw_payload: str) -> str:
    """A log-safe stand-in for a raw Kafka payload: a short hash plus byte length,
    enough to correlate a log line with the full message already preserved on the
    DLQ topic, without duplicating submission text / user identifiers into logs."""
    digest = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()[:12]
    return f"sha256:{digest} ({len(raw_payload)} bytes)"


def _extract_identifiers(event: dict) -> dict:
    """Pulls correlation identifiers (submissionId/tenantCode/sessionId — not
    submission content) out of a parsed event, so an invalid message can be found
    directly by these fields instead of by hashing payloads."""
    return {
        "submissionId": event.get("submissionId"),
        "tenantCode": event.get("tenantCode"),
        "sessionId": event.get("sessionId"),
    }


def _format_identifiers(identifiers: dict) -> str:
    present = [f"{k}={v}" for k, v in identifiers.items() if v is not None]
    return ", ".join(present) if present else "no identifiers found"


class IngestionConsumer:
    def __init__(self):
        self.consumer_conf = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            # Manual commit (see start()) — commit only after process_message()
            # has actually completed, so a crash mid-processing leaves the offset
            # uncommitted and the message is redelivered rather than silently lost.
            "enable.auto.commit": False,
        }
        self.dlq_topic = settings.KAFKA_TOPIC_INGESTION_DLQ
        self.consumer = None
        self.dlq_producer = None
        self.temporal_client = None
        self.running = False

    async def initialize(self):
        """
        Connects database pool, caches the Temporal client, and sets up the DLQ producer.
        """
        await db.connect()
        self.dlq_producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
        try:
            logger.info(f"Initializing Temporal client on {settings.TEMPORAL_HOST}...")
            self.temporal_client = await Client.connect(settings.TEMPORAL_HOST)
        except Exception as e:
            logger.error(f"Failed to connect to Temporal: {e}. Worker will not be able to trigger real-time flows.")

    async def _trigger_realtime_workflow(self, submission_id: str, tenant_code: str, submission_type: str):
        """
        Starts a Temporal workflow execution in real-time.
        """
        if not self.temporal_client:
            try:
                logger.info(f"Temporal client not connected. Attempting to reconnect to {settings.TEMPORAL_HOST}...")
                self.temporal_client = await Client.connect(settings.TEMPORAL_HOST)
            except Exception as e:
                logger.error(f"Failed to connect to Temporal: {e}. Leaving submission {submission_id} as 'pending'.")
                return

        process_steps = settings.get_process_config(submission_type)
        if not process_steps:
            logger.warning(f"No process steps defined for submission type '{submission_type}'. Skipping orchestration.")
            return

        workflow_id = f"realtime-{submission_id}-{tenant_code}"
        payload = {
            "submission_id": submission_id,
            "tenant_code": tenant_code,
            "process_steps": process_steps
        }

        try:
            logger.info(f"Starting Temporal workflow {workflow_id} for submission {submission_id}...")
            
            # Start workflow asynchronously without waiting for it to finish
            await self.temporal_client.start_workflow(
                ConfigDrivenProcessingWorkflow.run,
                payload,
                id=workflow_id,
                task_queue=settings.TEMPORAL_QUEUE
            )
            
            # Update status in PostgreSQL to 'processing'
            async with db.pool.acquire() as conn:
                await update_submission_status(conn, submission_id, tenant_code, "processing")
                
            logger.info(f"Workflow {workflow_id} triggered successfully.")
        except Exception as e:
            logger.error(f"Failed to trigger workflow {workflow_id}: {e}. Leaving submission {submission_id} as 'pending'.")
            # Reset client on connection / gRPC issues to trigger reconnect next time
            err_str = str(e).lower()
            if any(term in err_str for term in ["connect", "rpc", "connection", "unavailable"]):
                logger.info("Detected connection issue with Temporal. Resetting client to trigger reconnect on next message.")
                self.temporal_client = None

    async def _send_to_dlq(self, raw_payload: str, reason: str, identifiers: dict = None) -> None:
        """
        Publishes a malformed/invalid message to the dead-letter topic instead of
        silently dropping it, so it can be inspected or replayed later after the
        producer-side bug that caused it is fixed. When available, submissionId/
        tenantCode/sessionId are attached as extra headers (and logged) so the
        message can be found directly, without hashing payloads.

        Raises if the producer isn't initialized, or if delivery can't be confirmed
        within the flush timeout — callers (and start()'s offset-commit logic) must
        treat this message as NOT durably handled unless this returns without
        raising, otherwise a failed/unconfirmed DLQ publish plus a committed offset
        would lose the message with no trace of it anywhere.
        """
        identifiers = identifiers or {}
        id_str = _format_identifiers(identifiers)

        if not self.dlq_producer:
            raise RuntimeError(
                f"DLQ producer not initialized; cannot durably route invalid message. "
                f"Reason: {reason}. Identifiers: {id_str}"
            )

        def _produce():
            delivery_error = {}

            def _on_delivery(err, _msg):
                if err is not None:
                    delivery_error["error"] = err

            headers = [("reason", reason.encode("utf-8"))]
            for key, value in identifiers.items():
                if value is not None:
                    headers.append((key, str(value).encode("utf-8")))
            self.dlq_producer.produce(
                self.dlq_topic,
                value=raw_payload.encode("utf-8"),
                headers=headers,
                callback=_on_delivery,
            )
            # flush() polls until the delivery callback fires (or the timeout
            # elapses) — only this guarantees we know the outcome before returning,
            # unlike poll(0) which just services already-completed callbacks.
            remaining = self.dlq_producer.flush(10)
            if remaining > 0:
                raise TimeoutError(
                    f"Timed out waiting for DLQ delivery confirmation ({remaining} message(s) still in-flight)"
                )
            if "error" in delivery_error:
                raise RuntimeError(f"DLQ delivery failed: {delivery_error['error']}")

        await asyncio.to_thread(_produce)
        logger.warning(f"Sent invalid message to DLQ topic '{self.dlq_topic}'. Reason: {reason}. Identifiers: {id_str}. Payload: {_payload_fingerprint(raw_payload)}")

    def _validate_ingestion_schema(self, event: dict, submission_type: str, event_type: str) -> list:
        """
        Validates a Kafka event against the configured required-fields schema for its
        (submissionType, eventType) combination. Returns a list of problem descriptions;
        an empty list means the event is valid.
        """
        normalized_type = submission_type.lower().strip() if isinstance(submission_type, str) else ""
        if event_type in ("create", "update") and "story" not in normalized_type and "discussion" not in normalized_type:
            return [f"Unrecognized submissionType {submission_type!r}; no ingestion schema to validate against"]

        try:
            schema = settings.get_kafka_ingestion_schema(submission_type)
        except ValueError as e:
            return [str(e)]

        event_schema = schema.get(event_type)
        if event_schema is None:
            return [f"No ingestion schema section defined for eventType {event_type!r}"]

        problems = []
        for path in event_schema.get("required", []):
            value, found = _get_nested(event, path)
            if not found:
                problems.append(f"'{path}' is missing")
            elif _is_empty(value):
                problems.append(f"'{path}' is {_emptiness_label(value)}")

        if event_type == "update" and event_schema.get("newValuesNoEmpty"):
            new_values = event.get("newValues")
            if isinstance(new_values, dict):
                for key, value in new_values.items():
                    if _is_empty(value):
                        problems.append(f"'newValues.{key}' is {_emptiness_label(value)}")

        return problems

    async def process_message(self, raw_payload: str) -> None:
        """
        Processes a single deserialized Kafka message, executing DB changes and routing.
        Invalid messages (bad JSON, wrong shape, missing/null required keys, unsupported
        eventType) are routed to the DLQ topic instead of being silently dropped.
        """
        try:
            event = json.loads(raw_payload)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Kafka message JSON: {_payload_fingerprint(raw_payload)}. Error: {e}")
            await self._send_to_dlq(raw_payload, f"Invalid JSON: {e}")
            return

        if not isinstance(event, dict):
            reason = f"Expected a JSON object, got {type(event).__name__}"
            logger.error(f"Invalid Kafka message shape: expected an object but received {type(event).__name__}: {_payload_fingerprint(raw_payload)}")
            await self._send_to_dlq(raw_payload, reason)
            return

        identifiers = _extract_identifiers(event)
        event_type_raw = event.get("eventType", "create")
        if not isinstance(event_type_raw, str):
            reason = f"Invalid eventType: expected a string, got {type(event_type_raw).__name__} ({event_type_raw!r})"
            logger.error(f"{reason}. Identifiers: {_format_identifiers(identifiers)}")
            await self._send_to_dlq(raw_payload, reason, identifiers)
            return
        event_type = event_type_raw.lower().strip()
        submission_type = event.get("submissionType")

        problems = self._validate_ingestion_schema(event, submission_type, event_type)
        if problems:
            reason = f"Failed ingestion validation for submissionType {submission_type!r}, eventType {event_type!r}: {'; '.join(problems)}"
            logger.error(f"{reason}. Identifiers: {_format_identifiers(identifiers)}")
            await self._send_to_dlq(raw_payload, reason, identifiers)
            return

        # Keep None as None here (rather than str(None) == "None", which is truthy)
        # — the validator above already guarantees submissionId is present/non-empty,
        # this just normalizes it to a string for downstream DB calls.
        submission_id_raw = event.get("submissionId")
        submission_id = str(submission_id_raw) if submission_id_raw is not None else None
        tenant_code = event.get("tenantCode")

        logger.info(f"Processing event '{event_type}' for submission {submission_id} (tenant: {tenant_code})")

        async with db.pool.acquire() as conn:
            if event_type == "create":
                # Check for duplicate entry
                exists = None
                if hasattr(conn, "fetchval"):
                    exists = await conn.fetchval(
                        "SELECT 1 FROM submissions WHERE submission_id = $1 AND tenant_code = $2",
                        submission_id, tenant_code
                    )
                if exists:
                    logger.warning(f"Duplicate entry: Submission {submission_id} under tenant {tenant_code} already exists. Skipping ingestion.")
                    return

                res = await insert_or_update_submission(conn, event)
                
                # Check orchestration mode
                mode = settings.PROCESSING_MODE.lower().strip()
                if mode == "real-time":
                    # Trigger the processing immediately
                    await self._trigger_realtime_workflow(submission_id, tenant_code, submission_type)
                else:
                    logger.info(f"Batch mode enabled. Queued submission {submission_id} in 'pending' status.")

            elif event_type == "update":
                res = await insert_or_update_submission(conn, event)
                
                # Check orchestration mode
                mode = settings.PROCESSING_MODE.lower().strip()
                if mode == "real-time":
                    # Trigger the processing immediately
                    await self._trigger_realtime_workflow(submission_id, tenant_code, submission_type)
                else:
                    logger.info(f"Batch mode enabled. Queued submission {submission_id} in 'pending' status.")

            elif event_type == "delete":
                # Execute deletion
                await delete_submission(conn, submission_id, tenant_code)
            else:
                logger.error(f"Unsupported Kafka eventType: {event_type}. Identifiers: {_format_identifiers(identifiers)}")
                await self._send_to_dlq(raw_payload, f"Unsupported eventType: {event_type}", identifiers)

    async def _ensure_topics_exist(self, topics: list) -> None:
        """Create the given Kafka topics if they do not already exist."""
        admin_client = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

        try:
            futures = await asyncio.to_thread(
                admin_client.create_topics,
                [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics],
            )
            for name, future in futures.items():
                try:
                    future.result(timeout=30)
                    logger.info(f"Kafka topic '{name}' is ready.")
                except Exception as exc:
                    message = str(exc).lower()
                    if "already exists" in message:
                        logger.info(f"Kafka topic '{name}' already exists.")
                    else:
                        logger.warning(f"Could not create Kafka topic '{name}': {exc}")
        except Exception as exc:
            logger.warning(f"Kafka admin client failed while ensuring topics {topics}: {exc}")

    async def start(self) -> None:
        """
        Starts the polling loop using confluent-kafka inside an asyncio executor thread.
        """
        await self.initialize()
        await self._ensure_topics_exist([settings.KAFKA_TOPIC_INGESTION, self.dlq_topic])

        self.consumer = Consumer(self.consumer_conf)
        self.consumer.subscribe([settings.KAFKA_TOPIC_INGESTION])
        self.running = True
        
        logger.info(f"Started Kafka consumer listening to '{settings.KAFKA_TOPIC_INGESTION}'...")
        
        try:
            while self.running:
                # Poll message inside thread to avoid blocking loop
                msg = await asyncio.to_thread(self.consumer.poll, timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    code = msg.error().code()
                    if code == KafkaError._PARTITION_EOF:
                        continue
                    if code in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.UNKNOWN_PARTITION):
                        logger.warning(f"Kafka topic not ready yet: {msg.error()}")
                        await asyncio.sleep(1)
                        continue
                    logger.error(f"Kafka error: {msg.error()}")
                    raise KafkaException(msg.error())
                
                raw_payload = msg.value().decode("utf-8")
                try:
                    await self.process_message(raw_payload)
                    processed_ok = True
                except Exception as e:
                    processed_ok = False
                    logger.error(
                        f"Unhandled error processing Kafka message — leaving offset "
                        f"uncommitted so it is retried. "
                        f"Payload: {_payload_fingerprint(raw_payload)}. Error: {e}",
                        exc_info=True,
                    )

                # Commit only when process_message() actually completed without
                # raising — that covers both a successful DB/workflow write AND a
                # confirmed DLQ delivery (process_message()'s DLQ paths call
                # _send_to_dlq(), which itself raises on unconfirmed/failed delivery).
                # If it raised for any other reason, the offset stays uncommitted and
                # the message is redelivered on the next poll/restart instead of
                # being silently dropped.
                if processed_ok:
                    try:
                        await asyncio.to_thread(self.consumer.commit, msg, asynchronous=False)
                    except Exception as commit_err:
                        logger.error(f"Failed to commit Kafka offset: {commit_err}", exc_info=True)

        except asyncio.CancelledError:
            logger.info("Kafka consumer loop cancelled.")
        finally:
            self.running = False
            if self.consumer:
                self.consumer.close()
            if self.dlq_producer:
                await asyncio.to_thread(self.dlq_producer.flush, 10)
            await db.disconnect()
            logger.info("Kafka consumer stopped.")

    def stop(self) -> None:
        self.running = False
