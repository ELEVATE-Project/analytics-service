"""
Database operations of the `feedback` upload: lookups (program/leader resolution, the active upload config,
the stored state used by the delta check) and the management of questionnaire versions (set / list / relabel /
detect a new version from the questions the stored sessions were asked).

Unlike the story/discussion lookup in app/api/services/uploads.py, both the leader category and the
program are matched by NAME AND TENANT, and a miss is reported as None (never fabricated).

The feedback settings that differ per bot (max_csv_upload_bytes, ingestion_schema, process_steps, optional
expect_stages) live in the persona's active upload config; this module reads them (get_feedback_process_steps,
get_process_steps) and lists / migrates / changes them (list_feedback_config, migrate_feedback_config,
set_feedback_config_value, each change a NEW config_version, the old one kept as history).
"""
import copy
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.config import settings
from app.database.db import db
from app.database.operations import update_submission_status
from app.services.feedback_config_validation import (
    config_setting_problems,
    validate_config_value,
    validate_process_steps,
)
from app.services.feedback_constants import (
    CONFIG_KEY_EXPECT_STAGES,
    CONFIG_KEY_PROCESS_STEPS,
    CONFIG_TYPE_FEEDBACK_UPLOAD,
    FEEDBACK_REPORT_TYPE,
    FEEDBACK_SOURCES,
    REQUIRED_FEEDBACK_CONFIG_KEYS,
    SOURCE_MITRA,
    feedback_config_key_defaults,
)

logger = logging.getLogger("analytics_service.database.feedback_operations")

# Ids per ANY($n::text[]) query; keeps a very large upload's lookup to a few round trips.
_LOOKUP_CHUNK_SIZE = 5000


def _as_dict(value: Any) -> Dict[str, Any]:
    """asyncpg returns JSONB as str unless a codec is registered; accept both."""
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError(f"config_version.config must be a JSON object, got {type(value).__name__}")
    return value


async def get_program_and_leader(
    program_name: str,
    leader_category: str,
    tenant_code: str,
) -> Optional[Dict[str, Any]]:
    """
    Resolve a feedback upload's program + persona (leader category), by name and tenant.

    Returns {"program": {"id", "name"}, "leader": {"id", "name"}, "tenant_code"} or None when either
    row is missing for the tenant.
    """
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        leader_row = await conn.fetchrow(
            "SELECT id, name FROM leader_category WHERE name = $1 AND tenant_code = $2 LIMIT 1",
            leader_category,
            tenant_code,
        )
        if leader_row is None:
            return None
        program_row = await conn.fetchrow(
            "SELECT id, name FROM programs WHERE name = $1 AND leaders_id = $2 AND tenant_code = $3 LIMIT 1",
            program_name,
            leader_row["id"],
            tenant_code,
        )
        if program_row is None:
            return None

    return {
        "program": {"id": str(program_row["id"]), "name": program_row["name"]},
        "leader": {"id": str(leader_row["id"]), "name": leader_row["name"]},
        "tenant_code": tenant_code,
    }


async def get_active_feedback_config(
    program_id: str,
    config_source: str = SOURCE_MITRA,
    *,
    tenant_code: Optional[str] = None,
    session_type: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Load the ACTIVE config_version of the `config` row for (config_source, 'feedback_upload', program_id).
    A program is one persona and a persona has one session_type, so the row is found without any bot id
    (a persona runs several bots over time); the row's session_type is returned for the file check. tenant_code /
    session_type narrow the lookup when given (the Kafka consumer knows both from the event).

    Returns {"config_id", "config_version_id", "version", "session_type", "config": {...}} (ids as str) or
    None when no such config / no active version exists.
    """
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT c.id AS config_id, c.session_type, v.id AS config_version_id, v.version, v.config
            FROM config c
            JOIN config_version v ON v.config_id = c.id AND v.is_active
            WHERE c.config_source = $1
              AND c.config_type = $2
              AND c.program_id = $3::uuid
              AND ($4::text IS NULL OR c.tenant_code = $4)
              AND ($5::text IS NULL OR c.session_type = $5)
            ORDER BY v.version DESC
            LIMIT 1
            """,
            config_source,
            CONFIG_TYPE_FEEDBACK_UPLOAD,
            program_id,
            tenant_code,
            session_type,
        )
    if row is None:
        return None

    return {
        "config_id": str(row["config_id"]),
        "config_version_id": str(row["config_version_id"]),
        "version": row["version"],
        "session_type": row["session_type"],
        "config": _as_dict(row["config"]),
    }


async def _persona_process_steps(conn, source: str, session_type: str, tenant_code: str, program_id) -> List[Dict[str, Any]]:
    """The validated process_steps of the ACTIVE feedback_upload config of one persona (source + session_type +
    tenant; the config of the submission's program is preferred when several exist). Raises ValueError when there
    is no active config or its process_steps is missing / invalid. The message carries no submission content."""
    row = await conn.fetchrow(
        """
        SELECT v.version, v.config
        FROM config c
        JOIN config_version v ON v.config_id = c.id AND v.is_active
        WHERE c.config_type = $1 AND c.config_source = $2 AND c.session_type = $3 AND c.tenant_code = $4
        ORDER BY (c.program_id IS NOT DISTINCT FROM $5::uuid) DESC, v.version DESC
        LIMIT 1
        """,
        CONFIG_TYPE_FEEDBACK_UPLOAD, source, session_type, tenant_code, program_id,
    )
    if row is None:
        raise ValueError(
            f"No active {source} feedback_upload config for session_type {session_type!r} (tenant {tenant_code!r}); "
            "cannot resolve the process steps"
        )
    config = _as_dict(row["config"])
    if CONFIG_KEY_PROCESS_STEPS not in config:
        raise ValueError(
            f"Feedback config of session_type {session_type!r} ({source}) is missing "
            f"'{CONFIG_KEY_PROCESS_STEPS}' (add it with: python tools/feedback_config.py migrate)"
        )
    return validate_process_steps(config[CONFIG_KEY_PROCESS_STEPS])


async def get_feedback_process_steps(
    conn, submission_id: str, tenant_code: str, memo: Optional[Dict[Any, Any]] = None
) -> List[Dict[str, Any]]:
    """
    The process steps of a stored feedback submission: the submission's persona (feedback_submissions.session_type
    + source) selects its feedback_upload config (config_source = the submission's source, same tenant) and the
    ACTIVE config_version's `process_steps` is returned, validated (a list of {"name", optional "columns",
    "llm_model", "max_tokens", "llm_timeout_seconds"}). An empty list is valid: the submission is then marked
    success at ingestion. Raises ValueError when the submission is not a stored feedback submission, its persona
    has no active upload config, or the config lacks / has invalid process_steps.

    `memo` (a dict the caller owns for ONE sweep) remembers the persona lookup keyed by (session_type, source,
    tenant, program), errors included, so a sweep over many submissions of the same persona reads the config once.
    """
    sub = await conn.fetchrow(
        """
        SELECT fs.session_type, fs.source, s.program_id
        FROM feedback_submissions fs
        JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
        WHERE fs.submission_id = $1 AND fs.tenant_code = $2
        """,
        submission_id, tenant_code,
    )
    if sub is None:
        raise ValueError(f"No stored feedback submission {submission_id!r} for tenant {tenant_code!r}")
    key = (sub["session_type"], sub["source"], tenant_code, str(sub["program_id"]) if sub["program_id"] is not None else None)
    if memo is not None and key in memo:
        steps, error = memo[key]
        if error is not None:
            raise ValueError(f"{error} (submission {submission_id!r})")
        return copy.deepcopy(steps)
    try:
        steps = await _persona_process_steps(conn, sub["source"], sub["session_type"], tenant_code, sub["program_id"])
    except ValueError as exc:
        if memo is not None:
            memo[key] = (None, str(exc))
        raise ValueError(f"{exc} (submission {submission_id!r})") from exc
    if memo is not None:
        memo[key] = (steps, None)
    return copy.deepcopy(steps)


async def get_process_steps(
    conn, submission_type: str, submission_id: str, tenant_code: str, memo: Optional[Dict[Any, Any]] = None
) -> List[Dict[str, Any]]:
    """
    The process steps of a submission: feedback reads its persona's upload config (get_feedback_process_steps),
    story / discussion keep reading settings (PROCESS_CONFIG_STORY / PROCESS_CONFIG_DISCUSSION); an unknown type
    has none. `memo` is passed through (see get_feedback_process_steps).
    """
    if isinstance(submission_type, str) and FEEDBACK_REPORT_TYPE in submission_type.lower().strip():
        return await get_feedback_process_steps(conn, submission_id, tenant_code, memo)
    return settings.get_process_config(submission_type)


async def mark_submission_config_failed(conn, submission_id: str, tenant_code: str, reason: str) -> None:
    """
    A feedback submission whose persona config cannot give its process steps is marked 'failed' with the reason in
    its process_status (no retry_attempt is recorded, so the bulk retry picks it up once the config is fixed)
    instead of staying 'pending': a pending row that can never be resolved would keep returning at the head of
    every batch sweep and starve the other submissions. Identifiers and the config problem only, no content.
    """
    await update_submission_status(
        conn, submission_id, tenant_code, "failed",
        {
            "status": "failed",
            "error_message": f"Cannot resolve the process steps of the persona config: {reason}",
            "steps": [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "failed_at_step": None,
        },
    )


async def get_known_source_refs(tenant_code: str, refs: List[str]) -> set:
    """
    Which of `refs` (Google Forms natural keys) are already stored for the tenant: the subset of
    feedback_submissions.source_ref values. One query per chunk of _LOOKUP_CHUNK_SIZE refs.
    """
    unique_refs = sorted(set(refs))
    if not unique_refs:
        return set()
    if not db.pool:
        await db.connect()

    known: set = set()
    async with db.pool.acquire() as conn:
        for start in range(0, len(unique_refs), _LOOKUP_CHUNK_SIZE):
            rows = await conn.fetch(
                """
                SELECT source_ref
                FROM feedback_submissions
                WHERE tenant_code = $1 AND source_ref = ANY($2::text[])
                """,
                tenant_code,
                unique_refs[start:start + _LOOKUP_CHUNK_SIZE],
            )
            known.update(row["source_ref"] for row in rows)
    return known


async def get_mitra_session_state(tenant_code: str, submission_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Stored state of the Mitra sessions with these submission ids, for the delta check:
    {submission_id: {"session_id", "max_sequence", "message_count", "completion_status"}}.
    Sessions that are not stored are absent. A stored session with no feedbacks rows reports
    message_count 0 and max_sequence 0.
    """
    unique_ids = sorted(set(submission_ids))
    if not unique_ids:
        return {}
    if not db.pool:
        await db.connect()

    state: Dict[str, Dict[str, Any]] = {}
    async with db.pool.acquire() as conn:
        for start in range(0, len(unique_ids), _LOOKUP_CHUNK_SIZE):
            rows = await conn.fetch(
                """
                SELECT fs.submission_id,
                       s.session_id,
                       fs.completion_status,
                       COUNT(f.id) AS message_count,
                       COALESCE(MAX(f.sequence), 0) AS max_sequence
                FROM feedback_submissions fs
                JOIN submissions s
                  ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
                LEFT JOIN feedbacks f
                  ON f.submission_id = fs.submission_id AND f.tenant_code = fs.tenant_code
                WHERE fs.tenant_code = $1 AND fs.submission_id = ANY($2::text[])
                GROUP BY fs.submission_id, s.session_id, fs.completion_status
                """,
                tenant_code,
                unique_ids[start:start + _LOOKUP_CHUNK_SIZE],
            )
            for row in rows:
                state[row["submission_id"]] = {
                    "session_id": row["session_id"],
                    "max_sequence": int(row["max_sequence"]),
                    "message_count": int(row["message_count"]),
                    "completion_status": row["completion_status"],
                }
    return state


class _DryRunRollback(Exception):
    """Raised inside a transaction to roll it back after a dry run computed its result."""


async def _restamp_sessions(conn, session_type: str, tenant_code: str, versions) -> Dict[str, Any]:
    """
    Relabel the stored Mitra sessions of `session_type` in `tenant_code` from the given version windows (the list parsed by
    parse_versions) using each session's start (submissions.submission_date, its first real message).
    Writes through `conn` (the caller owns the transaction). Returns {"sessions", "changed", "versions"}.
    """
    from app.services.feedback_versions import stamp_version

    rows = await conn.fetch(
        """
        SELECT fs.submission_id, fs.tenant_code, fs.questionnaire_version, s.submission_date
        FROM feedback_submissions fs
        JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
        WHERE fs.source = $1 AND fs.session_type = $2 AND fs.tenant_code = $3 AND s.submission_date IS NOT NULL
        """,
        SOURCE_MITRA, session_type, tenant_code,
    )
    counts: Dict[str, int] = {}
    changed_ids: List[str] = []
    changed_tenants: List[str] = []
    changed_versions: List[str] = []
    for row in rows:
        key = stamp_version(versions, row["submission_date"])
        counts[key] = counts.get(key, 0) + 1
        if row["questionnaire_version"] != key:
            changed_ids.append(row["submission_id"])
            changed_tenants.append(row["tenant_code"])
            changed_versions.append(key)
    if changed_ids:
        await conn.execute(
            """
            UPDATE feedback_submissions fs
            SET questionnaire_version = t.version, updated_at = now()
            FROM unnest($1::text[], $2::text[], $3::text[]) AS t(submission_id, tenant_code, version)
            WHERE fs.submission_id = t.submission_id AND fs.tenant_code = t.tenant_code
              AND fs.source = $4 AND fs.session_type = $5
            """,
            changed_ids, changed_tenants, changed_versions, SOURCE_MITRA, session_type,
        )
    return {"sessions": len(rows), "changed": len(changed_ids), "versions": counts}


async def restamp_questionnaire_versions(
    session_type: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Relabel the questionnaire_version of the stored Mitra sessions from the ACTIVE upload config of their
    persona (config.questionnaire_versions). Needed after anything that changes the windows outside
    set_questionnaire_version (that one relabels by itself), because re-uploading an unchanged session is a
    no-op and never refreshes the label. Google Forms responses are not touched (their version comes from the form).

    Returns {"session_types": {session_type: {"sessions", "changed", "versions": {key: n}}}, "changed": total,
    "dry_run": bool}. With dry_run nothing is written. One transaction.
    Raises ValueError when an active config has an invalid questionnaire_versions list.
    """
    from app.services.feedback_versions import parse_versions

    if not db.pool:
        await db.connect()

    result: Dict[str, Any] = {"session_types": {}, "changed": 0, "dry_run": dry_run}
    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                configs = await conn.fetch(
                    """
                    SELECT c.session_type, c.tenant_code, v.config
                    FROM config c
                    JOIN config_version v ON v.config_id = c.id AND v.is_active
                    WHERE c.config_source = $1 AND c.config_type = $2
                      AND ($3::text IS NULL OR c.session_type = $3)
                    ORDER BY c.session_type
                    """,
                    SOURCE_MITRA, CONFIG_TYPE_FEEDBACK_UPLOAD, session_type,
                )
                for cfg_row in configs:
                    versions = parse_versions(_as_dict(cfg_row["config"]))
                    summary = await _restamp_sessions(conn, cfg_row["session_type"], cfg_row["tenant_code"], versions)
                    result["session_types"][cfg_row["session_type"]] = summary
                    result["changed"] += summary["changed"]
                if dry_run:
                    raise _DryRunRollback()
    except _DryRunRollback:
        pass
    return result


async def list_questionnaire_versions(session_type: Optional[str] = None) -> Dict[str, Any]:
    """
    The recorded questionnaire versions of each persona's active Mitra upload config, with the number of
    stored sessions per label: {session_type: {"config_version": n, "versions": [{"key", "effective_from"}],
    "sessions": {label: n}}}.
    """
    if not db.pool:
        await db.connect()

    result: Dict[str, Any] = {}
    async with db.pool.acquire() as conn:
        configs = await conn.fetch(
            """
            SELECT c.session_type, v.version, v.config
            FROM config c
            JOIN config_version v ON v.config_id = c.id AND v.is_active
            WHERE c.config_source = $1 AND c.config_type = $2
              AND ($3::text IS NULL OR c.session_type = $3)
            ORDER BY c.session_type
            """,
            SOURCE_MITRA, CONFIG_TYPE_FEEDBACK_UPLOAD, session_type,
        )
        for cfg_row in configs:
            counts = await conn.fetch(
                """
                SELECT questionnaire_version, count(*) AS n
                FROM feedback_submissions
                WHERE source = $1 AND session_type = $2
                GROUP BY 1
                """,
                SOURCE_MITRA, cfg_row["session_type"],
            )
            result[cfg_row["session_type"]] = {
                "config_version": cfg_row["version"],
                "versions": _as_dict(cfg_row["config"]).get("questionnaire_versions"),
                "sessions": {row["questionnaire_version"]: int(row["n"]) for row in counts},
            }
    return result


async def _fetch_session_questions(conn, session_type: str, tenant_code: str, closing_keys: List[str]):
    """The main (not probe, not closing) bot questions of every stored Mitra session of a persona."""
    from app.services.feedback_version_detect import SessionQuestions

    rows = await conn.fetch(
        """
        SELECT fb.submission_id, s.submission_date, fs.questionnaire_version, fb.message, fb.translated_message,
               fs.meta_data ->> 'bot_id' AS bot_id
        FROM feedbacks fb
        JOIN feedback_submissions fs ON fs.submission_id = fb.submission_id AND fs.tenant_code = fb.tenant_code
        JOIN submissions s ON s.submission_id = fb.submission_id AND s.tenant_code = fb.tenant_code
        WHERE fs.source = $1 AND fs.session_type = $2 AND fs.tenant_code = $3
          AND s.submission_date IS NOT NULL
          AND fb.parent_id IS NULL AND NOT fb.is_follow_up
          AND (fb.question_key IS NULL OR NOT (fb.question_key = ANY($4::text[])))
        ORDER BY fb.submission_id, fb.sequence
        """,
        SOURCE_MITRA, session_type, tenant_code, closing_keys,
    )
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        english = row["translated_message"]
        text = row["message"] if english is None or english.strip() == "" else english
        entry = grouped.setdefault(row["submission_id"], {
            "start": row["submission_date"], "stamped": row["questionnaire_version"], "texts": [],
            "bot_id": row["bot_id"]})
        entry["texts"].append(text)
    return [SessionQuestions(sid, e["start"], e["stamped"], tuple(e["texts"]), e["bot_id"]) for sid, e in grouped.items()]


async def detect_questionnaire_versions(
    session_type: Optional[str] = None,
    *,
    min_sessions: int = 20,
    baseline_sessions: int = 100,
    min_coverage: float = 0.3,
    include_wording: bool = False,
) -> Dict[str, Any]:
    """
    Read-only. For each persona's active Mitra upload config, compare the main questions the stored sessions were
    asked with what each recorded version is known to ask (app/services/feedback_version_detect.py) and propose a
    new version and its start when a new question set appeared. Nothing is written: a person records the proposal
    with set_questionnaire_version. Returns {session_type: detect() result + "next_step"}.
    """
    from app.services.feedback_version_detect import detect
    from app.services.feedback_versions import parse_versions

    if not db.pool:
        await db.connect()

    result: Dict[str, Any] = {}
    async with db.pool.acquire() as conn:
        configs = await conn.fetch(
            """
            SELECT c.session_type, c.tenant_code, v.config
            FROM config c
            JOIN config_version v ON v.config_id = c.id AND v.is_active
            WHERE c.config_source = $1 AND c.config_type = $2
              AND ($3::text IS NULL OR c.session_type = $3)
            ORDER BY c.session_type
            """,
            SOURCE_MITRA, CONFIG_TYPE_FEEDBACK_UPLOAD, session_type,
        )
        for cfg_row in configs:
            config = _as_dict(cfg_row["config"])
            keys = [key for key, _ in parse_versions(config)]
            closing = config.get("closing_keys")
            sessions = await _fetch_session_questions(
                conn, cfg_row["session_type"], cfg_row["tenant_code"], closing if isinstance(closing, list) else [])
            found = detect(sessions, keys, min_sessions=min_sessions, baseline_sessions=baseline_sessions,
                           min_coverage=min_coverage, include_wording=include_wording)
            proposal = found["proposal"]
            found["next_step"] = None if proposal is None else (
                f"python tools/questionnaire_versions.py set --session-type {cfg_row['session_type']} "
                f"--key {proposal['key']} --effective-from {proposal['effective_from']} --dry-run")
            result[cfg_row["session_type"]] = found
    return result


async def set_questionnaire_version(
    session_type: str,
    key: str,
    effective_from: str,
    *,
    created_by: str = "cli",
    change_note: Optional[str] = None,
    restamp: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Record that questionnaire version `key` of a persona's bot is in force from `effective_from` (an ISO instant
    with a UTC offset, for example 2026-12-01T10:30:00+05:30). A NEW key is appended after the last one (so V3, V4 ...
    need no code); an EXISTING key (not the first) gets its date moved. The windows live in the persona's Mitra upload
    config: this writes a new config_version (the previous one is deactivated, so it stays as history and can be
    re-activated to roll back) and, unless restamp is False, relabels the stored sessions of the persona in the same
    transaction. With dry_run everything is computed and rolled back.

    Returns {"session_type", "changed", "config_version", "versions", "restamp", "dry_run"}; changed is False (and
    nothing is written) when the list would not change. Raises ValueError for an unknown persona, a missing UTC
    offset, a date that is not after the previous version / before the next one, or the first version.
    """
    from app.services.feedback_versions import parse_versions

    if not isinstance(key, str) or key.strip() == "":
        raise ValueError("the version key must be a non-empty string")
    key = key.strip()
    try:
        instant = datetime.fromisoformat(
            effective_from[:-1] + "+00:00" if effective_from.endswith(("Z", "z")) else effective_from
        )
    except (ValueError, AttributeError):
        raise ValueError(f"effective_from {effective_from!r} is not an ISO instant (for example 2026-12-01T10:30:00+05:30)")
    if instant.tzinfo is None:
        raise ValueError(f"effective_from {effective_from!r} needs a UTC offset (for example +05:30)")
    effective_text = instant.isoformat()

    if not db.pool:
        await db.connect()

    result: Dict[str, Any] = {"session_type": session_type, "changed": False, "dry_run": dry_run,
                              "config_version": None, "versions": None, "restamp": None}
    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                # Lock the config row FIRST and read its active version afterwards in a separate statement: a
                # statement that waits for the lock keeps its snapshot for the join, so a single locking SELECT
                # over config + config_version could hand back a version a concurrent writer just deactivated.
                locked = await conn.fetchrow(
                    """
                    SELECT c.id AS config_id, c.tenant_code
                    FROM config c
                    WHERE c.config_source = $1 AND c.config_type = $2 AND c.session_type = $3
                    FOR UPDATE
                    """,
                    SOURCE_MITRA, CONFIG_TYPE_FEEDBACK_UPLOAD, session_type,
                )
                if locked is None:
                    raise ValueError(f"no active Mitra upload config for session_type {session_type!r}")
                row = await conn.fetchrow(
                    "SELECT id AS version_id, version, config FROM config_version WHERE config_id = $1 AND is_active",
                    locked["config_id"],
                )
                if row is None:
                    raise ValueError(f"no active Mitra upload config for session_type {session_type!r}")
                row = {**dict(row), "config_id": locked["config_id"], "tenant_code": locked["tenant_code"]}
                config = _as_dict(row["config"])
                parse_versions(config)  # the stored list must be valid before we touch it
                entries = [dict(e) for e in config["questionnaire_versions"]]
                position = next((i for i, e in enumerate(entries) if e["key"] == key), None)
                if position == 0:
                    raise ValueError(f"version {key!r} is the first one and starts from the beginning; it has no date")
                if position is None:
                    entries.append({"key": key, "effective_from": effective_text})
                else:
                    # the same instant written with another offset (or Z) is not a change
                    existing = parse_versions(config)[position][1]
                    if existing != instant:
                        entries[position]["effective_from"] = effective_text
                new_config = {**config, "questionnaire_versions": entries}
                versions = parse_versions(new_config)  # unique keys, offsets, strictly increasing

                result["versions"] = entries
                if entries == config["questionnaire_versions"]:
                    result["config_version"] = row["version"]
                    result["restamp"] = await _restamp_sessions(conn, session_type, row["tenant_code"], versions) if restamp else None
                    result["restamp_note"] = "no change to the version list"
                else:
                    latest = await conn.fetchval("SELECT MAX(version) FROM config_version WHERE config_id = $1", row["config_id"])
                    await conn.execute(
                        "UPDATE config_version SET is_active = FALSE, updated_at = now() WHERE id = $1", row["version_id"]
                    )
                    await conn.execute(
                        """
                        INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
                        VALUES ($1, $2, TRUE, $3::jsonb, $4, $5)
                        """,
                        row["config_id"], latest + 1, json.dumps(new_config), created_by,
                        change_note or f"questionnaire version {key} effective from {effective_text}",
                    )
                    result["changed"] = True
                    result["config_version"] = latest + 1
                    result["restamp"] = await _restamp_sessions(conn, session_type, row["tenant_code"], versions) if restamp else None
                if dry_run:
                    raise _DryRunRollback()
    except _DryRunRollback:
        pass
    return result


# ---------------------------------------------------------------------------------------------
# The per-bot settings of an upload config: show / migrate / set (tools/feedback_config.py)
# ---------------------------------------------------------------------------------------------

_CONFIG_SETTING_KEYS = (*REQUIRED_FEEDBACK_CONFIG_KEYS, CONFIG_KEY_EXPECT_STAGES)


async def _lock_and_read_active(conn, config_id) -> Optional[Dict[str, Any]]:
    """
    Lock the config row FIRST and read its active version afterwards in a separate statement (same reasoning as
    set_questionnaire_version: a statement that waits for the lock keeps its snapshot, so one locking SELECT over
    config + config_version could return a version a concurrent writer just deactivated).
    Returns {"version_id", "version", "config"} or None when the config has no active version.
    """
    await conn.fetchrow("SELECT id FROM config WHERE id = $1 FOR UPDATE", config_id)
    row = await conn.fetchrow(
        "SELECT id AS version_id, version, config FROM config_version WHERE config_id = $1 AND is_active", config_id
    )
    if row is None:
        return None
    return {"version_id": row["version_id"], "version": row["version"], "config": _as_dict(row["config"])}


async def _write_new_config_version(conn, config_id, old_version_id, new_config: Dict[str, Any],
                                    created_by: str, change_note: str) -> int:
    """Deactivate the active version (kept as history) and insert the next one. Returns its version number."""
    latest = await conn.fetchval("SELECT MAX(version) FROM config_version WHERE config_id = $1", config_id)
    await conn.execute("UPDATE config_version SET is_active = FALSE, updated_at = now() WHERE id = $1", old_version_id)
    await conn.execute(
        """
        INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
        VALUES ($1, $2, TRUE, $3::jsonb, $4, $5)
        """,
        config_id, latest + 1, json.dumps(new_config), created_by, change_note,
    )
    return latest + 1


_setting_problems = config_setting_problems  # shared with the startup check (app/database/feedback_config_health.py)


async def list_feedback_config(session_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Read-only. The per-bot settings of every ACTIVE feedback_upload config (both sources), optionally of one persona:
    {"configs": [{"session_type", "source", "tenant_code", "config_version", "max_csv_upload_bytes",
    "ingestion_schema", "process_steps", "expect_stages", "problems": {key: why}}]}. A missing key shows as None
    and is listed under problems.
    """
    if not db.pool:
        await db.connect()
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.session_type, c.config_source, c.tenant_code, v.version, v.config
            FROM config c
            JOIN config_version v ON v.config_id = c.id AND v.is_active
            WHERE c.config_type = $1 AND ($2::text IS NULL OR c.session_type = $2)
            ORDER BY c.session_type, c.config_source, c.tenant_code
            """,
            CONFIG_TYPE_FEEDBACK_UPLOAD, session_type,
        )
    configs = []
    for row in rows:
        config = _as_dict(row["config"])
        entry: Dict[str, Any] = {
            "session_type": row["session_type"], "source": row["config_source"],
            "tenant_code": row["tenant_code"], "config_version": row["version"],
        }
        for key in _CONFIG_SETTING_KEYS:
            entry[key] = config.get(key)
        entry["problems"] = _setting_problems(config)
        configs.append(entry)
    return {"configs": configs}


async def migrate_feedback_config(dry_run: bool = False, *, created_by: str = "cli") -> Dict[str, Any]:
    """
    Older databases keep the config version they were seeded with (seeds are ON CONFLICT DO NOTHING), which lacks
    the settings that moved out of .env. For each ACTIVE feedback_upload config (both sources) that lacks any of
    max_csv_upload_bytes / ingestion_schema / process_steps, write a NEW config_version (the old one is deactivated
    and kept as history) with the missing keys added from the seed defaults. Existing keys are never overwritten
    (an invalid existing key is only reported). Idempotent: a second run changes nothing. One transaction; with
    dry_run it is computed and rolled back.

    Returns {"configs": n, "migrated": m, "unchanged": k, "invalid": [..], "details": [{"session_type", "source",
    "added": [keys], "config_version"}], "dry_run": bool}; no config content is returned.
    """
    if not db.pool:
        await db.connect()

    defaults = feedback_config_key_defaults()
    result: Dict[str, Any] = {"configs": 0, "migrated": 0, "unchanged": 0, "invalid": [], "details": [],
                              "dry_run": dry_run}
    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                targets = await conn.fetch(
                    """
                    SELECT c.id AS config_id, c.session_type, c.config_source
                    FROM config c
                    WHERE c.config_type = $1
                    ORDER BY c.session_type, c.config_source, c.tenant_code, c.id
                    """,
                    CONFIG_TYPE_FEEDBACK_UPLOAD,
                )
                for target in targets:
                    active = await _lock_and_read_active(conn, target["config_id"])
                    if active is None:
                        continue  # a config without an active version has nothing to migrate
                    result["configs"] += 1
                    config = active["config"]
                    for key, why in _setting_problems(config).items():
                        if why != "missing":
                            result["invalid"].append(
                                {"session_type": target["session_type"], "source": target["config_source"],
                                 "key": key, "problem": why})
                    added = [key for key in REQUIRED_FEEDBACK_CONFIG_KEYS if key not in config]
                    if len(added) == 0:
                        result["unchanged"] += 1
                        continue
                    new_config = {**config, **{key: defaults[key] for key in added}}
                    version = await _write_new_config_version(
                        conn, target["config_id"], active["version_id"], new_config, created_by,
                        f"added {', '.join(added)} (moved out of .env)",
                    )
                    result["migrated"] += 1
                    result["details"].append({"session_type": target["session_type"],
                                              "source": target["config_source"], "added": added,
                                              "config_version": version})
                if dry_run:
                    raise _DryRunRollback()
    except _DryRunRollback:
        pass
    return result


async def set_feedback_config_value(
    session_type: str,
    source: str,
    key: str,
    value: Any,
    *,
    tenant_code: Optional[str] = None,
    unset: bool = False,
    created_by: str = "cli",
    change_note: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Change one per-bot setting (max_csv_upload_bytes, ingestion_schema, process_steps, expect_stages) of a persona's
    upload config without SQL: the value is validated (app/services/feedback_config_validation.py), then a NEW
    config_version is written (the old one is deactivated and kept as history). `unset` removes the optional
    expect_stages. Nothing is written when the value is already what is stored. With dry_run everything is computed
    and rolled back.

    Returns {"session_type", "source", "key", "changed", "config_version", "missing_keys", "dry_run"}; missing_keys
    lists required settings the config still lacks (run `migrate`). Raises ValueError for an unknown source / key,
    an invalid value, or when no (or more than one) active config matches (pass tenant_code).
    """
    if source not in FEEDBACK_SOURCES:
        raise ValueError(f"source must be one of: {', '.join(FEEDBACK_SOURCES)}")
    if key not in _CONFIG_SETTING_KEYS:
        raise ValueError(f"key must be one of: {', '.join(_CONFIG_SETTING_KEYS)}")
    if unset:
        if key in REQUIRED_FEEDBACK_CONFIG_KEYS:
            raise ValueError(f"'{key}' is required and cannot be unset; only '{CONFIG_KEY_EXPECT_STAGES}' can")
    else:
        value = validate_config_value(key, value)

    if not db.pool:
        await db.connect()

    result: Dict[str, Any] = {"session_type": session_type, "source": source, "key": key, "changed": False,
                              "config_version": None, "missing_keys": None, "dry_run": dry_run}
    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                targets = await conn.fetch(
                    """
                    SELECT c.id AS config_id FROM config c
                    WHERE c.config_source = $1 AND c.config_type = $2 AND c.session_type = $3
                      AND ($4::text IS NULL OR c.tenant_code = $4)
                    """,
                    source, CONFIG_TYPE_FEEDBACK_UPLOAD, session_type, tenant_code,
                )
                if len(targets) == 0:
                    raise ValueError(f"no {source} upload config for session_type {session_type!r}")
                if len(targets) > 1:
                    raise ValueError(
                        f"{len(targets)} {source} upload configs match session_type {session_type!r}; pass tenant_code")
                active = await _lock_and_read_active(conn, targets[0]["config_id"])
                if active is None:
                    raise ValueError(f"no active {source} upload config for session_type {session_type!r}")
                config = active["config"]
                new_config = {k: v for k, v in config.items() if not (unset and k == key)}
                if not unset:
                    new_config[key] = value
                result["missing_keys"] = [k for k in REQUIRED_FEEDBACK_CONFIG_KEYS if k not in new_config]
                if new_config == config:
                    result["config_version"] = active["version"]
                else:
                    result["config_version"] = await _write_new_config_version(
                        conn, targets[0]["config_id"], active["version_id"], new_config, created_by,
                        change_note or (f"removed {key}" if unset else f"set {key}"),
                    )
                    result["changed"] = True
                if dry_run:
                    raise _DryRunRollback()
    except _DryRunRollback:
        pass
    return result
