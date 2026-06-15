import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from temporalio.client import Client

from app.config import settings
from app.temporal.workflows import ConfigDrivenProcessingWorkflow

logger = logging.getLogger("analytics_service.api.routes")
router = APIRouter(prefix="/api/submissions", tags=["Submissions"])

class ManualTriggerRequest(BaseModel):
    submission_id: str
    tenant_code: str
    submission_type: str

@router.post("/trigger")
async def trigger_submission_manually(request: ManualTriggerRequest):
    """
    Manually triggers real-time processing workflow for a submission.
    """
    logger.info(f"API: Received manual trigger request for submission {request.submission_id}")
    
    # 1. Resolve steps config based on type
    process_steps = settings.get_process_config(request.submission_type)
    if not process_steps:
        raise HTTPException(
            status_code=400,
            detail=f"No process configuration found for submission type: {request.submission_type}"
        )

    # 2. Connect to Temporal and trigger
    try:
        client = await Client.connect(settings.TEMPORAL_HOST)
        workflow_id = f"manual-{request.submission_id}-{request.tenant_code}"
        
        handle = await client.start_workflow(
            ConfigDrivenProcessingWorkflow.run,
            {
                "submission_id": request.submission_id,
                "tenant_code": request.tenant_code,
                "process_steps": process_steps
            },
            id=workflow_id,
            task_queue=settings.TEMPORAL_QUEUE
        )
        return {
            "status": "success",
            "message": "Workflow started successfully",
            "workflow_id": handle.id,
            "run_id": handle.first_execution_run_id
        }
    except Exception as e:
        logger.error(f"API: Manual trigger failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start workflow: {str(e)}"
        )
