import asyncio
import logging
from temporalio.client import Client
from temporalio.worker import Worker

from app.config import settings
from app.database.db import db
from app.temporal.workflows import (
    ConfigDrivenProcessingWorkflow,
    BatchProcessingWorkflow,
    CsvProcessingWorkflow,
    CsvBatchProcessingWorkflow
)
from app.temporal.activities import (
    update_status_activity,
    fetch_pending_submissions_activity
)
from app.temporal.deface_blur_activity import deface_blur_activity
from app.temporal.pii_and_abusive_activity import pii_and_abusive_language_detection_activity
from app.temporal.thematic_activity import thematic_classification_activity
from app.temporal.csv_processing_activity import (
    csv_fetch_and_validate_activity,
    csv_push_to_kafka_activity,
    csv_update_status_activity,
    fetch_pending_csv_uploads_activity
)
from app.temporal.story_rating_activity import story_rating_activity

logger = logging.getLogger("analytics_service.temporal.worker")


async def start_worker():
    """
    Connects to Temporal server and listens on the configured task queue.
    """
    # Initialize database connection pool
    await db.connect()

    try:
        logger.info(f"Connecting to Temporal Server at {settings.TEMPORAL_HOST}...")
        client = await Client.connect(settings.TEMPORAL_HOST)
    except Exception as e:
        logger.error(f"Failed to connect to Temporal Server on {settings.TEMPORAL_HOST}: {e}")
        await db.disconnect()
        return

    # Define registered activities and workflows
    workflows = [
        ConfigDrivenProcessingWorkflow,
        BatchProcessingWorkflow,
        CsvProcessingWorkflow,
        CsvBatchProcessingWorkflow
    ]
    activities = [
        pii_and_abusive_language_detection_activity,
        thematic_classification_activity,
        deface_blur_activity,
        story_rating_activity,
        update_status_activity,
        fetch_pending_submissions_activity,
        csv_fetch_and_validate_activity,
        csv_push_to_kafka_activity,
        csv_update_status_activity,
        fetch_pending_csv_uploads_activity
    ]

    worker = Worker(
        client,
        task_queue=settings.TEMPORAL_QUEUE,
        workflows=workflows,
        activities=activities
    )

    # Register daily batch schedules if configured for batch mode
    if settings.PROCESSING_MODE.lower().strip() == "batch":
        from temporalio.client import (
            Schedule,
            ScheduleActionStartWorkflow,
            ScheduleSpec,
            ScheduleAlreadyRunningError,
        )

        # 1. Register CSV batch processing schedule
        try:
            logger.info(f"Registering CSV batch schedule '{settings.CSV_SCHEDULE_CRON_TIME}' in Temporal...")
            await client.create_schedule(
                id="csv-batch-processing",
                schedule=Schedule(
                    action=ScheduleActionStartWorkflow(
                        CsvBatchProcessingWorkflow.run,
                        id="csv-batch-processing-run",
                        task_queue=settings.TEMPORAL_QUEUE,
                    ),
                    spec=ScheduleSpec(
                        cron_expressions=[settings.CSV_SCHEDULE_CRON_TIME]
                    ),
                ),
            )
            logger.info("CSV batch schedule successfully registered.")
        except ScheduleAlreadyRunningError:
            logger.info("CSV batch schedule already exists in Temporal. Skipping registration.")
        except Exception as e:
            logger.error(f"Failed to register CSV batch schedule in Temporal: {e}")

        # 2. Register Analysis batch processing schedule
        try:
            logger.info(f"Registering daily analysis batch schedule '{settings.BATCH_SCHEDULE_CRON}' in Temporal...")
            await client.create_schedule(
                id="daily-batch-processing",
                schedule=Schedule(
                    action=ScheduleActionStartWorkflow(
                        BatchProcessingWorkflow.run,
                        id="daily-batch-processing-run",
                        task_queue=settings.TEMPORAL_QUEUE,
                    ),
                    spec=ScheduleSpec(
                        cron_expressions=[settings.BATCH_SCHEDULE_CRON]
                    ),
                ),
            )
            logger.info("Daily analysis batch schedule successfully registered.")
        except ScheduleAlreadyRunningError:
            logger.info("Daily analysis batch schedule already exists in Temporal. Skipping registration.")
        except Exception as e:
            logger.error(f"Failed to register daily analysis batch schedule in Temporal: {e}")

    logger.info(f"🚀 Temporal Worker started. Listening on task queue '{settings.TEMPORAL_QUEUE}'...")
    try:
        await worker.run()
    except asyncio.CancelledError:
        logger.info("Worker execution cancelled.")
    finally:
        await db.disconnect()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(start_worker())
