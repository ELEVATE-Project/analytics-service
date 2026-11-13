"""
Shared constants and the cross-module contract for the `feedback` report type
(Mitra raw conversations now, Google Forms later). See docs/feedback-upload-plan.md.

Everything that more than one module must agree on lives here: the names, sources and config keys of the
feedback report type, the defaults of the required config settings, and the shape of the Kafka event
(documented below, not enforced here). The persona ids and the version-1 content of every config (Mitra upload,
Google Forms upload, extraction) are SEED DATA in seed_config.sql, not code.

Kafka event for one session (eventType "create"; "update"/"delete" reuse the envelope):

    {
      "submissionId": "184884",            # str; Mitra chat-session id (Google Forms: a UUIDv4)
      "submissionType": "feedback",
      "sessionId": "<32-char session token>",
      "tenantCode": "mitra",
      "eventType": "create",
      "eventPublishedAt": "2026-10-15T08:54:43.000Z",
      "tags": {
        "programId": "<uuid>", "programName": "1M Voice",
        "leaderCategoryId": "<uuid>", "leaderCategoryName": "Youth",
        "state": "Karnataka" | null, "district": "..." | null,   # optional
        "source": "mitra"
      },
      "data": {
        "source": "mitra",                  # 'mitra' | 'google_forms'
        "sessionType": "Youth_Voice",       # persona key of the export (config.session_type); required
        "questionnaireVersion": "V2",       # stamped at ingest from the config's version windows; required
        "botId": "101", "botName": "...",   # Mitra only, informational (recorded in meta_data, never matched)
        "language": "hi",
        "completionStatus": "COMPLETED",    # 'COMPLETED' | 'IN_PROGRESS'
        "submissionDate": "2026-10-15T08:54:43.000Z",   # the session's first real message (not the opening row)
        "messages": [                        # ordered by sequence; only NEW rows on a delta upload
          {"sequence": 1,                    # STORED sequence (Mitra: CSV sequence + 1)
           "questionKey": "AGE_IDENTIFICATION",
           "isFollowUp": false,
           "message": "...",                 # conversation language, never null
           "translatedMessage": "..." | null,  # English
           "messageAt": "2026-10-15T08:54:43.000Z" | null,
           "parentSequence": null}           # null = question / closing message; else the question row's sequence
        ]
      }
    }

A row is an answer exactly when parentSequence (stored: feedbacks.parent_id) is set.
"""
import copy
from typing import Any, Dict

FEEDBACK_REPORT_TYPE = "feedback"
FEEDBACK_TENANT_CODE = "mitra"

SOURCE_MITRA = "mitra"
SOURCE_GOOGLE_FORMS = "google_forms"
FEEDBACK_SOURCES = (SOURCE_MITRA, SOURCE_GOOGLE_FORMS)

COMPLETION_COMPLETED = "COMPLETED"
COMPLETION_IN_PROGRESS = "IN_PROGRESS"
COMPLETION_STATUSES = (COMPLETION_COMPLETED, COMPLETION_IN_PROGRESS)

CONFIG_TYPE_FEEDBACK_UPLOAD = "feedback_upload"

# Settings that differ per feedback bot live in the persona's upload config (config_version.config), never in
# .env: every bot can have its own input structure. The keys below are REQUIRED in every feedback_upload config
# (pure validators: app/services/feedback_config_validation.py); the defaults are what `python tools/feedback_config.py
# migrate` adds to configs that predate them (seed_config.sql writes the same values into a new database; a test
# keeps the two equal).
CONFIG_KEY_MAX_CSV_UPLOAD_BYTES = "max_csv_upload_bytes"
CONFIG_KEY_INGESTION_SCHEMA = "ingestion_schema"
CONFIG_KEY_PROCESS_STEPS = "process_steps"
CONFIG_KEY_EXPECT_STAGES = "expect_stages"  # optional: report-only, never rejects a session
REQUIRED_FEEDBACK_CONFIG_KEYS = (CONFIG_KEY_MAX_CSV_UPLOAD_BYTES, CONFIG_KEY_INGESTION_SCHEMA, CONFIG_KEY_PROCESS_STEPS)

# One persona file is uploaded whole and the Parent file alone is ~43MB (the value must stay above it with headroom).
DEFAULT_MAX_CSV_UPLOAD_BYTES = 104857600  # 100MB

# The name of the post-processing step is fixed: the Temporal activity of that name does the metadata extraction.
FEEDBACK_METADATA_EXTRACTION_STEP = "feedback_metadata_extraction"
# Extraction is OPT-IN (every extraction is a paid LLM call per submission): the seed (seed_config.sql) and `migrate` write NO steps,
# so a submission is complete (success) at ingestion. Turn it on per persona with
#   python tools/feedback_config.py set --session-type Youth_Voice --source mitra --key process_steps \
#       --json '[{"name": "feedback_metadata_extraction"}]'
DEFAULT_PROCESS_STEPS: list = []

# One entry per (create/update/delete) event type: "required" is a list of dot-notation paths that must be present
# and non-empty; "optional" is informational only. Array elements of data.messages are validated separately in
# app/services/ingestion_validation.py; "update" is not supported for feedback in this release. tags.state /
# tags.district are optional (respondents skip them).
DEFAULT_INGESTION_SCHEMA = {
    "create": {
        "required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt",
                     "tags.programId", "tags.leaderCategoryId", "data.source", "data.sessionType",
                     "data.questionnaireVersion", "data.language", "data.completionStatus", "data.submissionDate",
                     "data.messages"],
        "optional": ["tags.state", "tags.district", "tags.programName", "tags.leaderCategoryName", "tags.source",
                     "data.botId", "data.botName"],
    },
    "update": {
        "required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt"],
        "newValuesNoEmpty": True,
    },
    "delete": {
        "required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt"],
    },
}


def feedback_config_key_defaults() -> Dict[str, Any]:
    """The three required settings with their defaults (a fresh copy every call)."""
    return {
        CONFIG_KEY_MAX_CSV_UPLOAD_BYTES: DEFAULT_MAX_CSV_UPLOAD_BYTES,
        CONFIG_KEY_INGESTION_SCHEMA: copy.deepcopy(DEFAULT_INGESTION_SCHEMA),
        CONFIG_KEY_PROCESS_STEPS: copy.deepcopy(DEFAULT_PROCESS_STEPS),
    }

# Google Forms rows are numbered 1..n (question row, then its answer row), no offset.
GFORMS_SEQUENCE_OFFSET = 0

# Old-shape Mitra files (no opening row) store sequence as (CSV sequence + 1; the `sequence_offset` of the Mitra
# config in seed_config.sql); the synthesised first question is sequence 1. New-shape files carry the opening row
# at CSV sequence 1 (stored as it is).
SYNTHESISED_FIRST_QUESTION_SEQUENCE = 1
