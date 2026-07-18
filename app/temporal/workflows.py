import asyncio
from datetime import timedelta
from typing import Dict, Any, List
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio.common import RetryPolicy
    from app.temporal.activities import (
        update_status_activity,
        fetch_pending_submissions_activity
    )
    from app.temporal.deface_blur_activity import deface_blur_activity
    from app.temporal.pii_and_abusive_activity import pii_and_abusive_language_detection_activity
    from app.temporal.thematic_activity import thematic_classification_activity
    from app.temporal.story_rating_activity import story_rating_activity

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
        steps_execution = []
        for step in process_steps:
            steps_execution.append({
                "name": step.get("name"),
                "status": "pending",
                "error": None,
                "completed_timestamp": None
            })

        try:
            for idx, step in enumerate(process_steps):
                step_name = step.get("name")
                target_columns = step.get("columns", [])
                # Optional per-step LLM overrides; activities fall back to global .env settings if omitted
                llm_overrides = {
                    "llm_model": step.get("llm_model"),
                    "max_tokens": step.get("max_tokens"),
                    "llm_timeout_seconds": step.get("llm_timeout_seconds"),
                }

                workflow.logger.info(f"Starting workflow step {idx + 1}/{len(process_steps)}: '{step_name}' for submission_id: {submission_id}")

                if step_name == "pii_and_abusive_language_detection":
                    res = await workflow.execute_activity(
                        pii_and_abusive_language_detection_activity,
                        {
                            "submission_id": submission_id,
                            "tenant_code": tenant_code,
                            "target_columns": target_columns,
                            "analysis_type": step_name,
                            **llm_overrides,
                        },
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=retry_policy
                    )
                    status_val = res.get("status", "success") if isinstance(res, dict) else "success"
                    steps_execution[idx]["status"] = status_val
                    steps_execution[idx]["completed_timestamp"] = workflow.now().isoformat()
                    completed_steps.append(step_name)
                    workflow.logger.info(f"Completed workflow step: '{step_name}' with status: {status_val}")


                elif step_name == "thematic_classification":
                    res = await workflow.execute_activity(
                        thematic_classification_activity,
                        {
                            "submission_id": submission_id,
                            "tenant_code": tenant_code,
                            "target_columns": target_columns,
                            "analysis_type": step_name,
                            **llm_overrides,
                        },
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=retry_policy
                    )
                    status_val = res.get("status", "success") if isinstance(res, dict) else "success"
                    steps_execution[idx]["status"] = status_val
                    steps_execution[idx]["completed_timestamp"] = workflow.now().isoformat()
                    completed_steps.append("thematic_classification")
                    workflow.logger.info(f"Completed workflow step: '{step_name}' with status: {status_val}")

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
                    status_val = res.get("status", "success") if isinstance(res, dict) else "success"
                    steps_execution[idx]["status"] = status_val
                    steps_execution[idx]["completed_timestamp"] = workflow.now().isoformat()
                    completed_steps.append("image_blur")
                    workflow.logger.info(f"Completed workflow step: '{step_name}' with status: {status_val}")

                elif step_name == "story_rating":
                    res = await workflow.execute_activity(
                        story_rating_activity,
                        {
                            "submission_id": submission_id,
                            "tenant_code": tenant_code,
                            **llm_overrides,
                        },
                        start_to_close_timeout=timedelta(minutes=10),
                        retry_policy=retry_policy
                    )
                    status_val = res.get("status", "success") if isinstance(res, dict) else "success"
                    steps_execution[idx]["status"] = status_val
                    steps_execution[idx]["completed_timestamp"] = workflow.now().isoformat()
                    completed_steps.append("story_rating")
                    workflow.logger.info(f"Completed workflow step: '{step_name}' with status: {status_val}")

                else:
                    unsupported_err = f"Unsupported step name: {step_name}"
                    steps_execution[idx]["status"] = "failed"
                    steps_execution[idx]["error"] = unsupported_err
                    steps_execution[idx]["completed_timestamp"] = workflow.now().isoformat()
                    raise ValueError(unsupported_err)

            # Update status to 'success' with execution details
            await workflow.execute_activity(
                update_status_activity,
                {
                    "submission_id": submission_id,
                    "tenant_code": tenant_code,
                    "status": "success",
                    "process_status": {
                        "status": "success",
                        "steps": steps_execution,
                        "timestamp": workflow.now().isoformat()
                    }
                },
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=retry_policy
            )

        except Exception as e:
            workflow.logger.error(f"Ingestion processing failed for {submission_id}: {e}")
            
            # Locate which step failed and mark it, plus mark remaining as skipped
            failed_found = False
            for s in steps_execution:
                if s["status"] == "pending":
                    if not failed_found:
                        s["status"] = "failed"
                        s["error"] = str(e)
                        s["completed_timestamp"] = workflow.now().isoformat()
                        failed_found = True
                    else:
                        s["status"] = "skipped"

            # Update status to 'failed' with error details
            await workflow.execute_activity(
                update_status_activity,
                {
                    "submission_id": submission_id,
                    "tenant_code": tenant_code,
                    "status": "failed",
                    "process_status": {
                        "status": "failed",
                        "failed_at_step": completed_steps[-1] if completed_steps else "ingestion",
                        "error_message": str(e),
                        "steps": steps_execution,
                        "timestamp": workflow.now().isoformat()
                    }
                },
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=retry_policy
            )
            raise

        return {
            "submission_id": submission_id,
            "status": "success",
            "steps": steps_execution
        }


@workflow.defn
class BatchProcessingWorkflow:
    @workflow.run
    async def run(self, batch_size: int) -> Dict[str, Any]:
        """
        Runs batch execution for all pending submissions, fetching and fanning out
        child workflows in bounded chunks (batch_size) rather than loading the entire
        pending queue into memory and launching all child workflows at once — at a few
        thousand pending submissions, doing it unbounded risks OOM on the worker and
        overwhelming the Temporal cluster with concurrent starts.

        batch_size is passed in by the caller (rather than read from settings here)
        so that replaying this workflow's history stays deterministic even if worker
        configuration changes between the original run and a replay.
        """
        # Safety cap on chunks processed in a single run — if this is ever hit, the
        # remainder is picked up by the next scheduled run rather than looping forever.
        max_chunks = 1000

        total_processed = 0
        total_success = 0
        total_failed = 0
        chunk_index = 0

        while True:
            pending_list: List[Dict[str, Any]] = await workflow.execute_activity(
                fetch_pending_submissions_activity,
                {"limit": batch_size},
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(maximum_attempts=2)
            )

            if not pending_list:
                break

            chunk_index += 1

            # Fan-out child workflows for this chunk only, then wait for all of them
            # before fetching the next chunk (so we never hold more than batch_size
            # rows or concurrent child workflows at a time).
            child_tasks = [
                workflow.execute_child_workflow(
                    ConfigDrivenProcessingWorkflow.run,
                    pending,
                    id=f"batch-child-{pending['submission_id']}-{pending['tenant_code']}"
                )
                for pending in pending_list
            ]
            results = await asyncio.gather(*child_tasks, return_exceptions=True)

            success_count = sum(1 for r in results if not isinstance(r, Exception))
            failed_count = len(results) - success_count

            total_processed += len(pending_list)
            total_success += success_count
            total_failed += failed_count

            workflow.logger.info(
                f"BatchProcessingWorkflow chunk {chunk_index}: {len(pending_list)} submissions "
                f"({success_count} succeeded, {failed_count} failed); running total {total_processed}."
            )

            # A short chunk means the pending queue is drained
            if len(pending_list) < batch_size:
                break

            if chunk_index >= max_chunks:
                workflow.logger.warning(
                    f"BatchProcessingWorkflow hit max_chunks={max_chunks} "
                    f"({total_processed} submissions processed this run) — stopping early; "
                    f"remaining pending submissions will be picked up on the next scheduled run."
                )
                break

        if total_processed == 0:
            return {"processed_count": 0, "message": "No pending submissions found."}

        return {
            "processed_count": total_processed,
            "success_count": total_success,
            "failed_count": total_failed,
            "chunks": chunk_index,
        }
