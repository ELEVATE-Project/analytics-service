"""
Splits a validated CSV into per-row payloads ready for Kafka.

If you need a config/process split (per the whiteboard sketch: "split the
csv -> config / process rows"), do that classification inside split_csv()
based on whatever column or rule distinguishes the two — currently this
just does a straight row-by-row split since the rule wasn't specified.
"""

import io
import json
import logging

import pandas as pd

logger = logging.getLogger("analytics_service.csv_pipeline.processor")


def load_csv(csv_file: io.BytesIO) -> pd.DataFrame:
    """Parse an in-memory CSV (BytesIO from GCS) into a DataFrame."""
    csv_file.seek(0)
    return pd.read_csv(csv_file)


def split_csv(df: pd.DataFrame) -> list[pd.DataFrame]:
    """
    Split a DataFrame into chunks for processing.

    Placeholder: currently returns the whole DataFrame as a single chunk.
    Replace with real config/process splitting logic once the rule for
    telling those two row types apart is defined (e.g. a "row_type" column).
    """
    return [df]


import secrets
import string
from datetime import datetime

def generate_session_id() -> str:
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(32))


def parse_csv_list(val) -> list[str]:
    if pd.isna(val) or val is None or not str(val).strip():
        return []
    s = str(val).strip()
    if (s.startswith("[") and s.endswith("]")) or \
       (s.startswith("(") and s.endswith(")")) or \
       (s.startswith("{") and s.endswith("}")):
        s = s[1:-1].strip()

    if "|" in s:
        raw_items = s.split("|")
    else:
        raw_items = s.split(",")
        
    cleaned = []
    for x in raw_items:
        x_clean = x.strip().strip("'\"").strip()
        if x_clean:
            cleaned.append(x_clean)
    return cleaned


def get_url_field(val):
    urls = parse_csv_list(val)
    if not urls:
        return None
    if len(urls) == 1:
        return urls[0]
    return urls


def clean_segment(s: str) -> str:
    import re
    s = s.strip()
    # Strip digit numbering (e.g. 1. or 1)) at start
    pattern_num = r'^\s*\d+[\.\)]\s*'
    s = re.sub(pattern_num, '', s).strip()
    # Strip bullet points (e.g. - or * or •) at start
    pattern_bullet = r'^\s*[\-\*\u2022]\s*'
    s = re.sub(pattern_bullet, '', s).strip()
    return s


def parse_segments(val, delimiter="|") -> list[str]:
    import re
    if pd.isna(val) or val is None or not str(val).strip():
        return []
    s = str(val).strip()
    if delimiter in s:
        raw_segments = s.split(delimiter)
    elif "\n" in s:
        raw_segments = s.split("\n")
    else:
        # Detect numbered patterns like "1. text 2. text", "1.text 2.text", or "1) text 2) text"
        # Use (?!\d) after the dot to avoid false positives on decimals like "2.5"
        # Require at least two numbered items to trigger splitting
        numbered_pattern = r'(?:^|\s)\d+(?:\.(?!\d)|\))\s*\S'
        has_numbering = len(re.findall(numbered_pattern, s)) >= 2
        if has_numbering:
            raw_segments = re.split(r'(?:^|\s+)\d+(?:\.(?!\d)|\))\s*', s)
        else:
            raw_segments = [s]
    
    segments = []
    for x in raw_segments:
        x_clean = clean_segment(x)
        if x_clean:
            segments.append(x_clean)
    return segments


def format_datetime(val, with_ms=True) -> str:
    if pd.isna(val) or val is None:
        val = datetime.utcnow()
    if isinstance(val, str):
        try:
            val = pd.to_datetime(val)
        except Exception:
            return val
    if hasattr(val, "to_pydatetime"):
        val = val.to_pydatetime()
    if isinstance(val, datetime):
        if with_ms:
            return val.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        else:
            return val.strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(val)


def row_to_json(
    row: pd.Series,
    report_type: str,
    event_type: str = "create",
    metadata: dict | None = None,
) -> str:
    """
    Convert a single CSV row into the JSON payload pushed to Kafka.

    The payload matches the format expected by the existing
    IngestionConsumer.process_message() so the same downstream
    pipeline (PII detection, thematic analysis) picks it up.
    """
    row_dict = row.where(pd.notna(row), None).to_dict()
    
    try:
        submission_id = int(row_dict.get("id"))
    except Exception:
        submission_id = row_dict.get("id")
        
    session_id = generate_session_id()
    
    # Extract metadata or default
    program_info = None
    leader_info = None
    tenant_code = "mitra"
    
    if metadata:
        program_info = metadata.get("programInfo")
        leader_info = metadata.get("LeaderCategoryInfo")
        tenant_code = metadata.get("tenantCode", "mitra")
        
    # Get designation logic
    designation = None
    if report_type.lower() == "story":
        designation = row_dict.get("Designation")
    if not designation and leader_info and leader_info.get("name"):
        designation = leader_info["name"].split("(")[0].strip()
        
    # Published date
    published_at_raw = row_dict.get("Report Created At")
    event_published_at = format_datetime(published_at_raw, with_ms=True)
    
    # Common user variables
    user_name = row_dict.get("User name")
    organization = row_dict.get("Organization")
    district = row_dict.get("District")
    
    data = {
        "programInfo": program_info,
        "LeaderCategoryInfo": leader_info,
        "title": row_dict.get("Title"),
        "userId": None,
        "userName": user_name,
        "organization": organization,
        "designation": designation,
        "state": None,
        "district": district,
        "submissionDate": None,
    }
    
    if report_type.lower() == "discussion":
        submission_date_raw = row_dict.get("Date of Discussion")
        submission_date = format_datetime(submission_date_raw, with_ms=False)
        
        user_id = str(row_dict.get("Author")) if row_dict.get("Author") is not None else str(submission_id)
        
        participants_data = []
        for role in ["men", "women", "children"]:
            col_name = role.capitalize()
            val = row_dict.get(col_name)
            if val is not None:
                try:
                    count = int(val)
                    if count > 0:
                        participants_data.append({"role": role, "count": count})
                except Exception:
                    pass
                    
        # State
        state = row_dict.get("User Location")
        if state:
            state_str = str(state)
            if "," in state_str:
                state_str = state_str.split(",")[-1].strip()
            state = state_str.title()
            
        data.update({
            "userId": user_id,
            "state": state,
            "submissionDate": submission_date,
            "imageUrls": parse_csv_list(row_dict.get("Image Urls")),
            "pdfUrls": get_url_field(row_dict.get("PDF Urls")),
            "transcriptLink": row_dict.get("Transcript Link") or None,
            "challenges": parse_segments(row_dict.get("Challenges")),
            "solutions": parse_segments(row_dict.get("Solutions")),
            "participantsData": participants_data,
            "author": user_id,
            "language": row_dict.get("Language") or "en",
        })
    else:  # story
        submission_date = format_datetime(published_at_raw, with_ms=False)
        
        user_id = str(row_dict.get("Author")) if "Author" in row_dict and row_dict.get("Author") is not None else (
            str(row_dict.get("userId")) if "userId" in row_dict and row_dict.get("userId") is not None else str(submission_id)
        )
        
        state = row_dict.get("Location")
        if state:
            state_str = str(state)
            if "," in state_str:
                state_str = state_str.split(",")[-1].strip()
            state = state_str.title()
            
        data.update({
            "userId": user_id,
            "state": state,
            "submissionDate": submission_date,
            "imageUrls": parse_csv_list(row_dict.get("Images")),
            "pdfUrls": get_url_field(row_dict.get("Pdf")),
            "transcriptLink": row_dict.get("Transcript Link") or None,
            "objective": row_dict.get("Objective"),
            "challenges": parse_segments(row_dict.get("Challenges")),
            "actionSteps": parse_segments(row_dict.get("Action Steps")),
            "impact": row_dict.get("Impact"),
            "duration": row_dict.get("Duration"),
            "blurb": row_dict.get("Blurb"),
            "content": row_dict.get("Content"),
        })
        
    payload = {
        "submissionId": submission_id,
        "submissionType": report_type,
        "sessionId": session_id,
        "tenantCode": tenant_code,
        "eventType": event_type,
        "eventPublishedAt": event_published_at,
        "data": data,
    }
    
    return json.dumps(payload, default=str)


def rows_to_json(
    df: pd.DataFrame,
    report_type: str,
    event_type: str = "create",
    metadata: dict | None = None,
):
    """Generator yielding one JSON string per row."""
    for _, row in df.iterrows():
        yield row_to_json(row, report_type, event_type, metadata)
