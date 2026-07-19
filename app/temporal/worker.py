import asyncio
import logging
from temporalio.client import Client
from temporalio.worker import Worker

from app.config import settings
from app.database.db import db
from app.temporal.workflows import ConfigDrivenProcessingWorkflow, BatchProcessingWorkflow
from app.temporal.activities import (
    update_status_activity,
    fetch_pending_submissions_activity
)
from app.temporal.deface_blur_activity import deface_blur_activity
from app.temporal.pii_and_abusive_activity import pii_and_abusive_language_detection_activity
from app.temporal.thematic_activity import thematic_classification_activity
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
    workflows = [ConfigDrivenProcessingWorkflow, BatchProcessingWorkflow]
    activities = [
        pii_and_abusive_language_detection_activity,
        thematic_classification_activity,
        deface_blur_activity,
        story_rating_activity,
        update_status_activity,
        fetch_pending_submissions_activity
    ]

    worker = Worker(
        client,
        task_queue=settings.TEMPORAL_QUEUE,
        workflows=workflows,
        activities=activities
    )

    # Register daily batch schedule if configured for batch mode
    if settings.PROCESSING_MODE.lower().strip() == "batch":
        try:
            from temporalio.client import (
                Schedule,
                ScheduleActionStartWorkflow,
                ScheduleSpec,
                ScheduleAlreadyRunningError,
            )

            logger.info(f"Registering daily batch schedule '{settings.BATCH_SCHEDULE_CRON}' in Temporal...")
            await client.create_schedule(
                id="daily-batch-processing",
                schedule=Schedule(
                    action=ScheduleActionStartWorkflow(
                        BatchProcessingWorkflow.run,
                        settings.BATCH_SIZE,
                        id="daily-batch-processing-run",
                        task_queue=settings.TEMPORAL_QUEUE,
                    ),
                    spec=ScheduleSpec(
                        cron_expressions=[settings.BATCH_SCHEDULE_CRON]
                    ),
                ),
            )
            logger.info("Daily batch schedule successfully registered.")
        except ScheduleAlreadyRunningError:
            logger.info("Daily batch schedule already exists in Temporal. Skipping registration.")
        except Exception as e:
            logger.error(f"Failed to register daily batch schedule in Temporal: {e}")

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
