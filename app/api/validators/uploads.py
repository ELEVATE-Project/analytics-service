import json
import logging
import pandas as pd
from app.api.exceptions import InvalidReportType, InvalidFileType, FileTooLarge, EmptyFile
from app.config import settings

logger = logging.getLogger("analytics_service.api.validators.uploads")


def validate_report_type(report_type: str) -> str:
    if not report_type:
        raise InvalidReportType("Only 'story' or 'discussion' report types are accepted.")
    normalized_type = report_type.lower().strip()
    if normalized_type not in ("story", "discussion"):
        raise InvalidReportType("Only 'story' or 'discussion' report types are accepted.")
    return normalized_type


def validate_extension(filename: str | None) -> None:
    if not filename or not filename.lower().endswith(".csv"):
        raise InvalidFileType("Only .csv files are accepted")


def validate_file_bytes(file_bytes: bytes) -> None:
    if len(file_bytes) > settings.MAX_CSV_UPLOAD_BYTES:
        raise FileTooLarge("Uploaded file is too large")
    if not file_bytes:
        raise EmptyFile("Uploaded file is empty")


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
        errors.append(f"Extra/unexpected columns: {extra_originals}")
        logger.warning(f"Validation failed for report_type='{report_type}'. Extra: {extra_originals}")

    return (len(errors) == 0), errors
