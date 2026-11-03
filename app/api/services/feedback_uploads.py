"""
Pure parsing, validation and event building for `feedback` uploads (Mitra raw conversation CSVs).

No DB, Kafka, network or settings access here: everything the functions need is passed in, so the
module can be tested in isolation. The Kafka event shape and the version-1 config JSON are defined
in app/services/feedback_constants.py; the rules are in docs/feedback-upload-plan.md.

Flow used by the upload service:
    df = parse_feedback_csv(file_bytes)                      # ValueError -> reject the upload
    errors = check_feedback_file(df, config, expected_bot_id)  # file-level; non-empty -> reject whole file
    events, skipped = build_feedback_events(df, config, ...)   # one event per good session; bad ones skipped
"""
import csv
import io
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from app.services.feedback_constants import (
    FEEDBACK_REPORT_TYPE,
    SYNTHESISED_FIRST_QUESTION_SEQUENCE,
)

_INT_RE = re.compile(r"^[0-9]+$")
_ISO_LIKE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
_SAMPLE_SIZE = 3

# Roles the builder reads from the CSV. translated_message is optional (English sessions may omit it).
_ROLE_COLUMNS = (
    "submission_id", "session", "session_status", "bot_id", "language",
    "sequence", "message", "created_at", "stage", "sender_id",
)


# ---------------------------------------------------------------------------------------------
# Parsing and file-level checks
# ---------------------------------------------------------------------------------------------

def parse_feedback_csv(file_bytes: bytes) -> pd.DataFrame:
    """
    Parse an uploaded Mitra CSV with every cell as a string.

    `dtype=str, keep_default_na=False` keeps the answers "NA" / "None" (pandas would turn them into
    NaN) and leaves empty cells as "". A UTF-8 BOM is dropped. Raises ValueError when the file cannot
    be parsed or has no data rows.
    """
    if file_bytes is None or len(file_bytes) == 0:
        raise ValueError("The uploaded CSV file is empty.")
    try:
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        raise ValueError("The uploaded CSV file is empty.")
    except UnicodeDecodeError as exc:
        raise ValueError(f"The uploaded CSV file is not valid UTF-8 text: {exc}")
    except (pd.errors.ParserError, csv.Error) as exc:
        raise ValueError(f"The uploaded CSV file could not be parsed: {exc}")
    df.columns = [str(c).strip() for c in df.columns]
    if len(df) == 0:
        raise ValueError("The uploaded CSV file has a header but no data rows.")
    return df


def _column_lookup(df: pd.DataFrame) -> Dict[str, str]:
    """lower-cased, stripped column name -> actual column name (first one wins)."""
    lookup: Dict[str, str] = {}
    for col in df.columns:
        lookup.setdefault(str(col).strip().lower(), col)
    return lookup


def check_feedback_file(df: pd.DataFrame, config: Dict[str, Any], expected_bot_id: str) -> List[str]:
    """
    File-level checks. Returns error strings; an empty list means the file may be processed.

    - every required column (config["columns"]["required"], case-insensitive) is present;
    - every row's bot_id is present and equals `expected_bot_id` (a file holding another persona's
      bot is rejected whole; the offending ids are reported).
    Blank submission_id / session cells are session-level problems (see build_feedback_events), not
    file-level ones. Extra columns are fine.
    """
    errors: List[str] = []
    lookup = _column_lookup(df)

    required = config["columns"]["required"]
    missing = [col for col in required if col.strip().lower() not in lookup]
    if missing:
        errors.append("Missing required column(s): " + ", ".join(missing))

    bot_col = lookup.get("bot_id")
    if bot_col is None:
        if "bot_id" not in [c.strip().lower() for c in required]:
            errors.append("Missing required column(s): bot_id")
        return errors

    expected = str(expected_bot_id).strip()
    bot_ids = df[bot_col].astype(str).str.strip()
    blank_rows = int((bot_ids == "").sum())
    other_ids = sorted({b for b in bot_ids.unique().tolist() if b != "" and b != expected})
    if other_ids:
        shown = ", ".join(other_ids[:10]) + (" ..." if len(other_ids) > 10 else "")
        errors.append(
            f"File contains rows for bot_id {shown}, but only bot_id {expected} is allowed for this upload"
        )
    if blank_rows > 0:
        errors.append(f"bot_id is blank in {blank_rows} row(s); every row must carry bot_id {expected}")
    return errors


# ---------------------------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------------------------

def _is_blank(value: Optional[str]) -> bool:
    return value is None or value.strip() == ""


def _sample(values: List[Any]) -> str:
    shown = ", ".join(repr(v) for v in values[:_SAMPLE_SIZE])
    return shown + (" ..." if len(values) > _SAMPLE_SIZE else "")


def _distinct(values: List[str]) -> List[str]:
    seen: Dict[str, None] = {}
    for v in values:
        seen.setdefault(v, None)
    return list(seen)


def _format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _resolve_timezone(name: str):
    try:
        return ZoneInfo(name)
    except Exception:
        raise ValueError(f"config.datetime.fallback_timezone {name!r} is not a known timezone")


def _parse_datetime(value: str, fallback_tz) -> Optional[datetime]:
    """Aware datetime, or None when unparsable. Naive values are read in `fallback_tz`."""
    text = value.strip()
    if text == "":
        return None
    dt: Optional[datetime] = None
    try:
        dt = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text)
    except ValueError:
        if _ISO_LIKE_RE.match(text):
            try:
                ts = pd.Timestamp(text)
            except (ValueError, OverflowError):
                return None
            if pd.isna(ts):
                return None
            dt = ts.to_pydatetime()
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=fallback_tz)
    return dt


def _normalise_answer(text: str) -> str:
    return " ".join(text.strip().lower().split())


def _check_config(config: Dict[str, Any]) -> None:
    for key in ("bot_sender_ids", "message_map", "sequence_offset", "completion_values", "questions"):
        if key not in config:
            raise ValueError(f"Feedback config is missing '{key}'")
    for kind in ("bot", "respondent"):
        entry = config["message_map"].get(kind)
        if not isinstance(entry, dict) or "message" not in entry or "translated_message" not in entry:
            raise ValueError(f"Feedback config message_map.{kind} must map 'message' and 'translated_message'")
    if len(config["questions"]) == 0:
        raise ValueError("Feedback config has an empty 'questions' catalog")


# ---------------------------------------------------------------------------------------------
# Event building
# ---------------------------------------------------------------------------------------------

def build_feedback_events(
    df: pd.DataFrame,
    config: Dict[str, Any],
    *,
    program_info: Dict[str, Any],
    leader_info: Dict[str, Any],
    tenant_code: str,
    source: str,
    published_at: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Build one Kafka event per good session.

    program_info / leader_info are {"id": ..., "name": ...}. Returns (events, skipped): events follow
    the contract in app/services/feedback_constants.py; skipped lists every session that failed a
    session-level check as {"submissionId", "sessionId", "problems": [...]} (None for a blank id),
    with ALL of its problems. Other sessions still produce events.

    Rules (see the plan, section 3 and the Sequence numbering paragraph):
    - identity of a session = (submission_id, session); each id must map to exactly one of the other;
    - stored sequence = CSV sequence + config["sequence_offset"]; the unlogged first question is
      synthesised at sequence 1 from config["questions"] (the entry whose key is the stage of the
      session's first CSV row), only when the CSV starts at sequence 1 (tail-only sessions are
      skipped in Phase 1);
    - a sender_id outside config["bot_sender_ids"] is a respondent; parentSequence is null for bot
      rows and, for a respondent row, the stored sequence of the nearest preceding bot row
      (including the synthesised question). Nothing relies on sequence parity;
    - text: stored message = conversation language, translatedMessage = English, via
      config["message_map"] (bot rows swap the CSV columns).
    """
    _check_config(config)
    lookup = _column_lookup(df)
    missing = [role for role in _ROLE_COLUMNS if role not in lookup]
    if missing:
        raise ValueError("Feedback CSV is missing required column(s): " + ", ".join(missing))

    fallback_tz = _resolve_timezone(config.get("datetime", {}).get("fallback_timezone", "UTC"))
    published = published_at if published_at is not None else _format_utc(datetime.now(timezone.utc))

    def column(name: str) -> List[str]:
        actual = lookup.get(name)
        if actual is None:
            return [""] * len(df)
        return df[actual].astype(str).tolist()

    submission_ids = [v.strip() for v in column("submission_id")]
    session_ids = [v.strip() for v in column("session")]
    statuses = [v.strip() for v in column("session_status")]
    bot_ids = [v.strip() for v in column("bot_id")]
    languages = [v.strip() for v in column("language")]
    sequences = [v.strip() for v in column("sequence")]
    stages = [v.strip() for v in column("stage")]
    senders = [v.strip() for v in column("sender_id")]
    created = column("created_at")
    csv_text = {
        "message": column("message"),
        "translated_message": column("translated_message"),
    }
    # message_map names CSV columns; resolve each to the extracted list (any other column is read ad hoc)
    mapped_columns: Dict[str, List[str]] = {}

    def mapped(name: str) -> List[str]:
        if name not in mapped_columns:
            mapped_columns[name] = csv_text[name] if name in csv_text else column(name)
        return mapped_columns[name]

    # Group row positions by session identity, in file order.
    groups: Dict[Tuple[str, str], List[int]] = {}
    for i in range(len(df)):
        groups.setdefault((submission_ids[i], session_ids[i]), []).append(i)

    sessions_of_submission: Dict[str, List[str]] = {}
    submissions_of_session: Dict[str, List[str]] = {}
    for (sub, sess) in groups:
        if sub != "" and sess != "":
            sessions_of_submission.setdefault(sub, []).append(sess)
            submissions_of_session.setdefault(sess, []).append(sub)

    bot_sender_ids = {str(s).strip() for s in config["bot_sender_ids"]}
    completion_values = list(config["completion_values"])
    closing_keys = set(config.get("closing_keys", []))
    offset = int(config["sequence_offset"])
    catalog = {q["key"]: q for q in config["questions"]}
    english_languages = {str(x).lower() for x in config.get("english_languages", ["en"])}
    bot_map = config["message_map"]["bot"]
    respondent_map = config["message_map"]["respondent"]

    events: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for (sub, sess), idx in groups.items():
        problems: List[str] = []
        sub_id = sub if sub != "" else None
        sess_id = sess if sess != "" else None

        # ---- identity ----
        if sub == "":
            problems.append(f"submission_id is blank in {len(idx)} row(s)")
        if sess == "":
            problems.append(f"session is blank in {len(idx)} row(s)")
        if sub != "" and len(sessions_of_submission.get(sub, [])) > 1:
            problems.append(f"submission_id {sub!r} maps to {len(sessions_of_submission[sub])} different sessions")
        if sess != "" and len(submissions_of_session.get(sess, [])) > 1:
            problems.append(f"session {sess!r} maps to {len(submissions_of_session[sess])} different submission_ids")
        if sub == "" or sess == "":
            skipped.append({"submissionId": sub_id, "sessionId": sess_id, "problems": problems})
            continue

        # ---- sequence ----
        seq_by_pos: Dict[int, int] = {}
        blank_seq = 0
        bad_seq: List[str] = []
        non_positive: List[str] = []
        for i in idx:
            raw = sequences[i]
            if raw == "":
                blank_seq += 1
            elif not _INT_RE.match(raw):
                bad_seq.append(raw)
            elif int(raw) < 1:
                non_positive.append(raw)
            else:
                seq_by_pos[i] = int(raw)
        if blank_seq > 0:
            problems.append(f"sequence is missing in {blank_seq} row(s)")
        if len(bad_seq) > 0:
            problems.append(f"sequence is not an integer in {len(bad_seq)} row(s): {_sample(bad_seq)}")
        if len(non_positive) > 0:
            problems.append(f"sequence must be 1 or greater: {_sample(non_positive)}")
        seen_seq: Dict[int, int] = {}
        for seq in seq_by_pos.values():
            seen_seq[seq] = seen_seq.get(seq, 0) + 1
        duplicates = sorted(s for s, n in seen_seq.items() if n > 1)
        if len(duplicates) > 0:
            problems.append(f"duplicate sequence(s) in the session: {_sample(duplicates)}")

        # ---- text ----
        empty_rows = [sequences[i] for i in idx
                      if _is_blank(csv_text["message"][i]) and _is_blank(csv_text["translated_message"][i])]
        if len(empty_rows) > 0:
            problems.append(
                f"message and translated_message are both empty in {len(empty_rows)} row(s) "
                f"(sequence {_sample(empty_rows)})"
            )

        # ---- sender ----
        blank_senders = sum(1 for i in idx if senders[i] == "")
        if blank_senders > 0:
            problems.append(f"sender_id is blank in {blank_senders} row(s)")

        # ---- constant-within-session columns ----
        status_values = _distinct([statuses[i] for i in idx])
        if len(status_values) > 1:
            problems.append("session_status differs within the session: " + _sample(status_values))
        invalid_status = [v for v in status_values if v not in completion_values]
        if len(invalid_status) > 0:
            problems.append(
                f"session_status {_sample(invalid_status)} is not one of: " + ", ".join(completion_values)
            )
        language_values = _distinct([languages[i] for i in idx])
        if "" in language_values:
            problems.append(f"language is blank in {sum(1 for i in idx if languages[i] == '')} row(s)")
        non_blank_languages = [v for v in language_values if v != ""]
        if len(non_blank_languages) > 1:
            problems.append("language differs within the session: " + _sample(non_blank_languages))
        bot_values = _distinct([bot_ids[i] for i in idx])
        if len(bot_values) > 1:
            problems.append("bot_id differs within the session: " + _sample(bot_values))

        # ---- created_at ----
        parsed_at: Dict[int, datetime] = {}
        bad_dates: List[str] = []
        for i in idx:
            dt = _parse_datetime(created[i], fallback_tz)
            if dt is None:
                bad_dates.append(created[i])
            else:
                parsed_at[i] = dt
        if len(bad_dates) > 0:
            problems.append(f"created_at is missing or unparsable in {len(bad_dates)} row(s): {_sample(bad_dates)}")

        if len(problems) > 0:
            skipped.append({"submissionId": sub_id, "sessionId": sess_id, "problems": problems})
            continue

        # ---- ordering, tail-only check, synthesised first question ----
        ordered = sorted(idx, key=lambda i: seq_by_pos[i])
        first = ordered[0]
        first_seq = seq_by_pos[first]
        if first_seq != 1:
            skipped.append({
                "submissionId": sub_id, "sessionId": sess_id,
                "problems": [f"CSV starts at sequence {first_seq}, not 1: sessions exported from the middle "
                             f"(tail-only) are not supported"],
            })
            continue
        first_key = stages[first]
        question = catalog.get(first_key)
        if question is None:
            skipped.append({
                "submissionId": sub_id, "sessionId": sess_id,
                "problems": [f"first row's stage {first_key!r} has no entry in the questions catalog "
                             f"(expected one of: {', '.join(sorted(catalog))})"],
            })
            continue

        language = languages[first]
        is_english = language.lower() in english_languages
        completion_status = statuses[first]
        submission_date = _format_utc(parsed_at[first])

        messages: List[Dict[str, Any]] = []
        synth_text = question["text"]
        synth_seq = SYNTHESISED_FIRST_QUESTION_SEQUENCE
        messages.append({
            "sequence": synth_seq,
            "questionKey": first_key,
            "isFollowUp": False,
            "message": synth_text,
            "translatedMessage": synth_text,
            "messageAt": submission_date,
            "parentSequence": None,
        })

        current_parent = synth_seq
        previous_bot_key: Optional[str] = first_key
        for i in ordered:
            stored_seq = seq_by_pos[i] + offset
            key = stages[i] if stages[i] != "" else None
            is_bot = senders[i] in bot_sender_ids
            text_map = bot_map if is_bot else respondent_map
            conversation_text = mapped(text_map["message"])[i]
            english_text = mapped(text_map["translated_message"])[i]
            if _is_blank(conversation_text):
                conversation_text = english_text
            if _is_blank(english_text):
                english_text = conversation_text if is_english else None

            if is_bot:
                parent: Optional[int] = None
                # A bot row whose stage equals the previous bot row's stage is a probe / follow-up
                # asked on the same question (checked on the real export: 9,303 of 48,013 bot rows,
                # only 93 of them re-send the same text). Closing rows never count.
                follow_up = key is not None and key == previous_bot_key and key not in closing_keys
                previous_bot_key = key
                current_parent = stored_seq
            else:
                parent = current_parent
                follow_up = False

            messages.append({
                "sequence": stored_seq,
                "questionKey": key,
                "isFollowUp": follow_up,
                "message": conversation_text,
                "translatedMessage": english_text,
                "messageAt": _format_utc(parsed_at[i]),
                "parentSequence": parent,
            })

        state, district = _location_tags(messages, config.get("location"))
        events.append({
            "submissionId": sub,
            "submissionType": FEEDBACK_REPORT_TYPE,
            "sessionId": sess,
            "tenantCode": tenant_code,
            "eventType": "create",
            "eventPublishedAt": published,
            "tags": {
                "programId": program_info["id"],
                "programName": program_info["name"],
                "leaderCategoryId": leader_info["id"],
                "leaderCategoryName": leader_info["name"],
                "state": state,
                "district": district,
                "source": source,
            },
            "data": {
                "source": source,
                "language": language,
                "completionStatus": completion_status,
                "submissionDate": submission_date,
                "messages": messages,
            },
        })

    return events, skipped


def _location_tags(messages: List[Dict[str, Any]], location: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """
    state / district from the respondent's answer to the configured stage, via config aliases.

    config["location"] = {"state": {"stage": "STATE_IDENTIFICATION", "aliases": {"<normalised>": "<canonical>"}},
                          "district": {...}}
    The answer's English text (translatedMessage, else message) is normalised (strip, lowercase,
    collapse whitespace) and looked up in the aliases; None when the block or the answer is absent or
    the answer is not recognised.
    """
    if location is None:
        return None, None
    return _location_value(messages, location.get("state")), _location_value(messages, location.get("district"))


def _location_value(messages: List[Dict[str, Any]], rule: Optional[Dict[str, Any]]) -> Optional[str]:
    if rule is None:
        return None
    stage = rule.get("stage")
    aliases = rule.get("aliases", {})
    for msg in messages:
        if msg["parentSequence"] is None or msg["questionKey"] != stage:
            continue
        text = msg["translatedMessage"] if msg["translatedMessage"] is not None else msg["message"]
        return aliases.get(_normalise_answer(text))
    return None
