import logging
from typing import Any, Dict

from temporalio import activity

from app.database.db import db
from app.database.feedback_extraction_operations import extract_submission
from app.services.feedback_extraction_constants import ANALYSIS_TYPE

logger = logging.getLogger("analytics_service.temporal.feedback_extraction")


@activity.defn
async def feedback_metadata_extraction_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity of the `feedback_metadata_extraction` workflow step.

    One LLM call per stored feedback session (Mitra or Google Forms alike) receives the complete conversation and
    returns the DOTS metadata fields (state, district, gender, school_type ...) plus a whole-conversation PII
    flag. The field rows go to feedback_extracted_data and the flag to feedback_submissions.pii_detected, in one
    transaction together with the llm log; a re-run overwrites. The core is
    app.database.feedback_extraction_operations.extract_submission, shared with tools/run_feedback_extraction.py.

    params: submission_id, tenant_code and the optional per-step overrides llm_model / max_tokens /
    llm_timeout_seconds (None = use the extraction config's llm settings, then the global .env settings).
    A conversation without a respondent answer makes no LLM call. Like the other steps this does not set the
    submission's terminal status; the workflow does that once every step has completed.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    analysis_type = params.get("analysis_type")
    if analysis_type is not None and analysis_type != ANALYSIS_TYPE:
        raise ValueError(f"feedback_metadata_extraction_activity cannot run analysis_type {analysis_type!r}")
    overrides = {
        "llm_model": params.get("llm_model"),
        "max_tokens": params.get("max_tokens"),
        "llm_timeout_seconds": params.get("llm_timeout_seconds"),
    }
    try:
        outcome = await extract_submission(
            db.pool, submission_id, tenant_code,
            llm_overrides=overrides, write=True, mark_processing=True,
        )
    except Exception as exc:
        # Never the message of an extraction error at INFO or above beyond its type: it is built without
        # respondent text, but the provider's error body is not ours to repeat.
        logger.error("Feedback metadata extraction failed for submission %s: %s", submission_id, type(exc).__name__)
        # The workflow's own exception handler marks the step and the submission failed once this propagates.
        raise

    result = outcome.run.result
    return {
        "status": "success",
        "llm_called": outcome.run.llm_called,
        "pii_detected": result.pii_detected,
        "fields": {f.field_name: f.status for f in result.fields},
    }
