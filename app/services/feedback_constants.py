"""
Shared constants and the cross-module contract for the `feedback` report type
(Mitra raw conversations now, Google Forms later). See docs/feedback-upload-plan.md.

Everything that more than one module must agree on lives here: the fixed seed ids,
the persona -> program / bot mapping, the version-1 Mitra upload config, and the shape
of the Kafka event (documented below, not enforced here).

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
        "language": "hi",
        "completionStatus": "COMPLETED",    # 'COMPLETED' | 'IN_PROGRESS'
        "submissionDate": "2026-10-15T08:54:43.000Z",
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
from typing import Any, Dict

FEEDBACK_REPORT_TYPE = "feedback"
FEEDBACK_TENANT_CODE = "mitra"
FEEDBACK_PROGRAM_NAME = "1M Voice"

SOURCE_MITRA = "mitra"
SOURCE_GOOGLE_FORMS = "google_forms"
FEEDBACK_SOURCES = (SOURCE_MITRA, SOURCE_GOOGLE_FORMS)

COMPLETION_COMPLETED = "COMPLETED"
COMPLETION_IN_PROGRESS = "IN_PROGRESS"
COMPLETION_STATUSES = (COMPLETION_COMPLETED, COMPLETION_IN_PROGRESS)

CONFIG_TYPE_FEEDBACK_UPLOAD = "feedback_upload"

# Mitra stores sequence as (CSV sequence + 1); the synthesised first question is sequence 1.
MITRA_SEQUENCE_OFFSET = 1
SYNTHESISED_FIRST_QUESTION_SEQUENCE = 1

# Persona == leader category. One program name ("1M Voice") is stored as one `programs` row per
# leader category. All ids are fixed so seeds can use ON CONFLICT (id) DO NOTHING
# (programs / leader_category have no unique key on name).
PERSONAS: Dict[str, Dict[str, str]] = {
    "Youth": {
        "leader_category_id": "5f3d1f0f-f472-47af-8c55-66e10aa48c43",
        "program_id": "dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a",
        "config_id": "864bd1a7-3f34-473e-8578-65c801908657",
        "bot_id": "90",
        "bot_name": "1MV Youth Voice",
        "config_name": "Youth bot CSV validator",
        "first_question_key": "AGE_IDENTIFICATION",
        "first_question_text": "Can you please tell me your age?",
    },
    "Parent": {
        "leader_category_id": "99e4b68a-ea5e-432d-8282-c4d79d3f62e8",
        "program_id": "31dae489-5647-4d0b-91cb-1c5f53dba4ae",
        "config_id": "aa3bf372-219a-405f-b13d-caef9becad68",
        "bot_id": "88",
        "bot_name": "1MV Parent Voice",
        "config_name": "Parent bot CSV validator",
        "first_question_key": "STATE_IDENTIFICATION",
        "first_question_text": "Which state do you stay in?",
    },
    "General Public": {
        "leader_category_id": "f7cf3db1-55bd-4940-be70-556ca14c49b4",
        "program_id": "bc53156c-bcb9-4332-b817-f6c5ef4a0640",
        "config_id": "c19ca45c-7cc5-4fea-a157-c8f508b4c592",
        "bot_id": "89",
        "bot_name": "1MV General voice",
        "config_name": "General bot CSV validator",
        "first_question_key": "AGE_IDENTIFICATION",
        "first_question_text": "Can you please tell me your age?",
    },
}


def mitra_config_v1(persona: str) -> Dict[str, Any]:
    """The version-1 `config_version.config` JSON for a persona's Mitra bot."""
    p = PERSONAS[persona]
    return {
        "columns": {
            "required": ["bot_id", "session_status", "session", "submission_id", "language",
                         "sequence", "message", "created_at", "stage", "sender_id"],
            "optional": ["translated_message", "bot_name", "conversation_row_id", "receiver_id", "updated_at"],
            "ignored": ["audio_file", "file_url", "other_params"],
        },
        "ignore_extra_columns": True,
        # any sender_id NOT in this list is a respondent; it only decides parent links
        "bot_sender_ids": ["1"],
        # Mitra bot rows carry message / translated_message the other way round.
        # stored.message = conversation language, stored.translated_message = English.
        "message_map": {
            "bot": {"message": "translated_message", "translated_message": "message"},
            "respondent": {"message": "message", "translated_message": "translated_message"},
        },
        "sequence_offset": MITRA_SEQUENCE_OFFSET,
        "completion_values": list(COMPLETION_STATUSES),
        "closing_keys": ["THANK_YOU", "CLOSING"],
        "datetime": {"fallback_timezone": "Asia/Kolkata"},
        "questions": [
            {"key": p["first_question_key"], "text": p["first_question_text"], "kind": "metadata"},
        ],
    }
