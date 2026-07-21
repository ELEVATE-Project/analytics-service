import logging
from temporalio.client import Client
from app.config import settings
from app.temporal.workflows import ConfigDrivenProcessingWorkflow

logger = logging.getLogger("analytics_service.api.services.submission_service")


async def trigger_submission_manually(submission_id: str, tenant_code: str, submission_type: str) -> dict:
    """
    Manually triggers real-time processing workflow for a submission.
    """
    logger.info(f"API: Received manual trigger request for submission {submission_id}")

    process_steps = settings.get_process_config(submission_type)
    if not process_steps:
        raise ValueError(f"No process configuration found for submission type: {submission_type}")

    try:
        client = await Client.connect(settings.TEMPORAL_HOST)
        workflow_id = f"manual-{submission_id}-{tenant_code}"
        handle = await client.start_workflow(
            ConfigDrivenProcessingWorkflow.run,
            {
                "submission_id": submission_id,
                "tenant_code": tenant_code,
                "process_steps": process_steps,
            },
            id=workflow_id,
            task_queue=settings.TEMPORAL_QUEUE,
        )
        return {
            "status": "success",
            "message": "Workflow started successfully",
            "workflow_id": handle.id,
            "run_id": handle.first_execution_run_id,
        }
    except Exception as e:
        logger.error(f"API: Manual trigger failed: {e}")
        raise e
