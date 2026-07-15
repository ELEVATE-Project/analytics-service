"""
Validates an uploaded CSV's columns against the expected schema
defined in the environment variables (settings.STORY_CSV_COLUMN / settings.DISCUSSION_CSV_COLUMN).
"""

import json
import logging
import pandas as pd
from app.config import settings

logger = logging.getLogger("analytics_service.csv_pipeline.validators")


def validate_columns(df: pd.DataFrame, report_type: str) -> tuple[bool, list[str]]:
    """
    Returns (is_valid, list_of_error_messages).

    Checks:
      1. All expected columns defined in settings for report_type are present (case-insensitive check).
    """
    errors: list[str] = []

    normalized_type = report_type.lower().strip()
    if normalized_type == "story":
        raw_cols = settings.STORY_CSV_COLUMN
    elif normalized_type == "discussion":
        raw_cols = settings.DISCUSSION_CSV_COLUMN
    else:
        return False, [f"No expected schema configured for report_type='{report_type}'"]

    try:
        expected_cols = json.loads(raw_cols)
        if not isinstance(expected_cols, list):
            raise ValueError("Expected columns must be a JSON array")
    except Exception as exc:
        return False, [f"Failed to parse expected columns from settings for {report_type}: {exc}"]

    # Normalize both sides to lowercase for case-insensitive comparison
    expected_cols_lower = {col.lower(): col for col in expected_cols}
    actual_cols_lower = {col.lower(): col for col in df.columns}

    missing = set(expected_cols_lower.keys()) - set(actual_cols_lower.keys())
    if missing:
        # Report using original expected column names for clarity
        missing_originals = sorted(expected_cols_lower[m] for m in missing)
        errors.append(f"Missing columns: {missing_originals}")
        logger.warning(f"Validation failed for report_type='{report_type}'. Missing: {missing_originals}")

    extra = set(actual_cols_lower.keys()) - set(expected_cols_lower.keys())
    if extra:
        extra_originals = sorted(actual_cols_lower[e] for e in extra)
        logger.info(f"Note: CSV contains extra columns not in the schema: {extra_originals}")

    return (len(errors) == 0), errors
