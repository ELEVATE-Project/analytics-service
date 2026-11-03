import json
import logging
from typing import List, Optional, Tuple
import pandas as pd
from app.api.exceptions import InvalidReportType, InvalidFileType, FileTooLarge, EmptyFile, InvalidFeedbackSource
from app.config import settings
from app.services.feedback_constants import FEEDBACK_REPORT_TYPE, FEEDBACK_SOURCES

logger = logging.getLogger("analytics_service.api.validators.uploads")


def validate_report_type(report_type: str) -> str:
    if not report_type:
        raise InvalidReportType("Only 'story', 'discussion' or 'feedback' report types are accepted.")
    normalized_type = report_type.lower().strip()
    if normalized_type not in ("story", "discussion", FEEDBACK_REPORT_TYPE):
        raise InvalidReportType("Only 'story', 'discussion' or 'feedback' report types are accepted.")
    return normalized_type


def max_upload_bytes(report_type: Optional[str] = None) -> int:
    """Size cap for a report type: feedback has its own, higher cap; story/discussion keep MAX_CSV_UPLOAD_BYTES."""
    if report_type is not None and report_type.lower().strip() == FEEDBACK_REPORT_TYPE:
        return settings.FEEDBACK_MAX_CSV_UPLOAD_BYTES
    return settings.MAX_CSV_UPLOAD_BYTES


def validate_source(source: str) -> str:
    """Validate the optional `source` form field of a feedback upload; returns the normalized value."""
    normalized_source = source.lower().strip() if source is not None else ""
    if normalized_source not in FEEDBACK_SOURCES:
        raise InvalidFeedbackSource(
            f"Invalid source {source!r}: allowed values are {', '.join(repr(s) for s in FEEDBACK_SOURCES)}."
        )
    return normalized_source


def validate_extension(filename: Optional[str]) -> None:
    if not filename or not filename.lower().endswith(".csv"):
        raise InvalidFileType("Only .csv files are accepted")


def validate_file_bytes(file_bytes: bytes, report_type: Optional[str] = None) -> None:
    if len(file_bytes) > max_upload_bytes(report_type):
        raise FileTooLarge("Uploaded file is too large")
    if not file_bytes:
        raise EmptyFile("Uploaded file is empty")


def validate_columns(df: pd.DataFrame, report_type: str) -> Tuple[bool, List[str]]:
    """
    Returns (is_valid, list_of_error_messages).

    Checks:
      1. All expected columns defined in settings for report_type are present (case-insensitive check).
    """
    errors: List[str] = []

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
        errors.append(f"Extra/unexpected columns: {extra_originals}")
        logger.warning(f"Validation failed for report_type='{report_type}'. Extra: {extra_originals}")

    return (len(errors) == 0), errors
