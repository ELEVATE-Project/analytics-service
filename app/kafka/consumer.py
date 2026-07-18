import asyncio
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

    async def _send_to_dlq(self, raw_payload: str, reason: str) -> None:
        """
        Publishes a malformed/invalid message to the dead-letter topic instead of
        silently dropping it, so it can be inspected or replayed later after the
        producer-side bug that caused it is fixed.
        """
        if not self.dlq_producer:
            logger.error(f"DLQ producer not initialized; dropping invalid message. Reason: {reason}. Payload: {raw_payload!r}")
            return

        def _produce():
            self.dlq_producer.produce(
                self.dlq_topic,
                value=raw_payload.encode("utf-8"),
                headers=[("reason", reason.encode("utf-8"))],
            )
            self.dlq_producer.poll(0)

        try:
            await asyncio.to_thread(_produce)
            logger.warning(f"Sent invalid message to DLQ topic '{self.dlq_topic}'. Reason: {reason}. Payload: {raw_payload!r}")
        except Exception as e:
            logger.error(
                f"Failed to publish message to DLQ topic '{self.dlq_topic}': {e}. Original payload: {raw_payload!r}",
                exc_info=True,
            )

    async def process_message(self, raw_payload: str) -> None:
        """
        Processes a single deserialized Kafka message, executing DB changes and routing.
        Invalid messages (bad JSON, wrong shape, missing/null required keys, unsupported
        eventType) are routed to the DLQ topic instead of being silently dropped.
        """
        try:
            event = json.loads(raw_payload)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Kafka message JSON: {raw_payload!r}. Error: {e}")
            await self._send_to_dlq(raw_payload, f"Invalid JSON: {e}")
            return

        if not isinstance(event, dict):
            reason = f"Expected a JSON object, got {type(event).__name__}"
            logger.error(f"Invalid Kafka message shape: expected an object but received {type(event).__name__}: {raw_payload!r}")
            await self._send_to_dlq(raw_payload, reason)
            return

        event_type = event.get("eventType", "create").lower().strip()
        submission_id_raw = event.get("submissionId")
        # Keep None as None here (rather than str(None) == "None", which is truthy and
        # would silently bypass the null/missing check below) so a null or absent
        # submissionId is correctly caught as invalid.
        submission_id = str(submission_id_raw) if submission_id_raw is not None else None
        tenant_code = event.get("tenantCode")
        submission_type = event.get("submissionType")

        if not submission_id or not tenant_code:
            reason = "Missing or null submissionId/tenantCode"
            logger.error(f"Invalid Kafka message format. Missing submissionId or tenantCode: {event}")
            await self._send_to_dlq(raw_payload, reason)
            return

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
                logger.error(f"Unsupported Kafka eventType: {event_type}")
                await self._send_to_dlq(raw_payload, f"Unsupported eventType: {event_type}")

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
                except Exception as e:
                    logger.error(
                        f"Unhandled error processing Kafka message, skipping it. "
                        f"Payload: {raw_payload!r}. Error: {e}",
                        exc_info=True,
                    )

                # Commit only now that processing has been attempted (succeeded, or
                # failed and was logged above) — never before. If the process crashes
                # inside process_message(), this line never runs, the offset stays
                # uncommitted, and the message is redelivered on restart instead of
                # being silently dropped.
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
