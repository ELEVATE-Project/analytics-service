import asyncio
from datetime import timedelta
from typing import Dict, Any, List
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio.common import RetryPolicy
    from app.temporal.activities import (
        pii_detection_activity,
        thematic_analysis_activity,
        deface_blur_activity,
        update_status_activity,
        fetch_pending_submissions_activity
    )

@workflow.defn
class ConfigDrivenProcessingWorkflow:
    @workflow.run
    async def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        submission_id = payload["submission_id"]
        tenant_code = payload["tenant_code"]
        process_steps = payload.get("process_steps", [])

        # Set up a generic, resilient retry policy for LLM/CV activities
        retry_policy = RetryPolicy(
            maximum_attempts=3,
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0
        )

        # 1. Update status to 'processing'
        await workflow.execute_activity(
            update_status_activity,
            {"submission_id": submission_id, "tenant_code": tenant_code, "status": "processing"},
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=retry_policy
        )

        completed_steps = []
        process_metadata = {}

        try:
            for step in process_steps:
                step_name = step.get("name")
                target_columns = step.get("columns", [])

                if step_name == "pii_detection":
                    res = await workflow.execute_activity(
                        pii_detection_activity,
                        {
                            "submission_id": submission_id,
                            "tenant_code": tenant_code,
                            "target_columns": target_columns
                        },
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=retry_policy
                    )
                    completed_steps.append("pii_detection")
                    process_metadata["pii_detection"] = res

                elif step_name == "thematic_analysis":
                    res = await workflow.execute_activity(
                        thematic_analysis_activity,
                        {
                            "submission_id": submission_id,
                            "tenant_code": tenant_code,
                            "target_columns": target_columns
                        },
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=retry_policy
                    )
                    completed_steps.append("thematic_analysis")
                    process_metadata["thematic_analysis"] = res

                elif step_name in ("image_blur", "image_blurring"):
                    res = await workflow.execute_activity(
                        deface_blur_activity,
                        {
                            "submission_id": submission_id,
                            "tenant_code": tenant_code
                        },
                        start_to_close_timeout=timedelta(minutes=15),
                        retry_policy=retry_policy
                    )
                    completed_steps.append("image_blur")
                    process_metadata["image_blur"] = res

            # Update status to 'success' with execution details
            await workflow.execute_activity(
                update_status_activity,
                {
                    "submission_id": submission_id,
                    "tenant_code": tenant_code,
                    "status": "success",
                    "process_status": {
                        "completed_steps": completed_steps,
                        "metadata": process_metadata,
                        "timestamp": workflow.now().isoformat()
                    }
                },
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=retry_policy
            )

        except Exception as e:
            workflow.logger.error(f"Ingestion processing failed for {submission_id}: {e}")
            
            # Update status to 'failed' with error details
            await workflow.execute_activity(
                update_status_activity,
                {
                    "submission_id": submission_id,
                    "tenant_code": tenant_code,
                    "status": "failed",
                    "process_status": {
                        "failed_at_step": completed_steps[-1] if completed_steps else "ingestion",
                        "error_message": str(e),
                        "timestamp": workflow.now().isoformat()
                    }
                },
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=retry_policy
            )
            raise

        return {"submission_id": submission_id, "status": "success"}


@workflow.defn
class BatchProcessingWorkflow:
    @workflow.run
    async def run(self) -> Dict[str, Any]:
        """
        Runs batch execution for all pending submissions.
        Retrieves pending records and executes child workflows in parallel.
        """
        # Fetch all pending submissions and their dynamic configurations
        pending_list: List[Dict[str, Any]] = await workflow.execute_activity(
            fetch_pending_submissions_activity,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=2)
        )

        if not pending_list:
            return {"processed_count": 0, "message": "No pending submissions found."}

        # Fan-out child workflows to process submissions in parallel
        child_tasks = []
        for pending in pending_list:
            child_tasks.append(
                workflow.execute_child_workflow(
                    ConfigDrivenProcessingWorkflow.run,
                    pending,
                    id=f"batch-child-{pending['submission_id']}-{pending['tenant_code']}"
                )
            )

        # Wait for all child workflows to complete
        results = await asyncio.gather(*child_tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if not isinstance(r, Exception))
        failed_count = len(results) - success_count

        return {
            "processed_count": len(pending_list),
            "success_count": success_count,
            "failed_count": failed_count
        }
