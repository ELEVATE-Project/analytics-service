import logging
from enum import Enum
from typing import Tuple, Optional
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

logger = logging.getLogger("analytics_service.temporal.liveness")

_WORKFLOW_ID_PREFIXES = ("realtime", "batch-child", "step-retry")


class TemporalLivenessStatus(str, Enum):
    RUNNING = "running"
    RETRYABLE = "retryable"


async def check_submission_temporal_state(
    client: Client,
    submission_id: str,
    tenant_code: str
) -> Tuple[TemporalLivenessStatus, Optional[str]]:
    """
    Checks candidate workflow IDs for this submission in Temporal:
      - realtime-{submission_id}-{tenant_code}
      - batch-child-{submission_id}-{tenant_code}
      - step-retry-{submission_id}-{tenant_code}

    Returns:
      (TemporalLivenessStatus.RUNNING, matching_workflow_id) if any candidate workflow is currently RUNNING.
      (TemporalLivenessStatus.RETRYABLE, None) if no candidate workflow is running.

    Fails closed: Any unexpected RPCError or connection failure is re-raised so callers
    do not accidentally retry something that might still be alive.
    """
    for prefix in _WORKFLOW_ID_PREFIXES:
        wf_id = f"{prefix}-{submission_id}-{tenant_code}"
        handle = client.get_workflow_handle(wf_id)
        try:
            desc = await handle.describe()
            if desc.status == WorkflowExecutionStatus.RUNNING:
                logger.info(f"Submission '{submission_id}' (tenant: '{tenant_code}') has active running workflow: '{wf_id}'")
                return TemporalLivenessStatus.RUNNING, wf_id
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND:
                continue
            logger.error(f"Unexpected RPC error while checking workflow '{wf_id}': {e}")
            raise

    return TemporalLivenessStatus.RETRYABLE, None

