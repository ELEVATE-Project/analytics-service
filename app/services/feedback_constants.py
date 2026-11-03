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
import copy
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

# Google Forms (Phase 2): one `config` row per persona program with bot_id NULL; ids are fixed like the Mitra ones.
# Google Forms rows are numbered 1..n (question row, then its answer row), no offset.
GFORMS_SEQUENCE_OFFSET = 0

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
        "gforms_config_id": "957b809a-028a-4139-8455-881d24696eb7",
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
        "gforms_config_id": "34bd6a47-1f76-4760-bf45-bea23bea72be",
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
        "gforms_config_id": "6543fdb5-8d6e-4929-ae6e-576d7237f38a",
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


# ---- Google Forms (Phase 2) -----------------------------------------------------------------
# Form definitions for the launch instruments, seeded from 1MV_POC/1MV_questions_by_source.csv
# (question_key = its mitra_step_name, the canonical key shared with Mitra). A question's `headers`
# are the CSV column headers in NORMALISED form (app.api.services.feedback_gforms.normalise_header:
# whitespace collapsed, cut at the first non-ASCII character, trailing "/" and quotes stripped), so the
# bilingual Karnataka headers and the English-only ones resolve to the same question. `kind` is
# 'metadata' for respondent profile questions and 'open' for the rest. Nothing is marked required:
# blank cells are skipped. Pilot and age-banded drafts are not configured.
_GFORMS_METADATA = "metadata"
_GFORMS_OPEN = "open"


def _gforms_question(key: str, kind: str, text: str, *variant_headers: str) -> Dict[str, Any]:
    # The English text is also the normalised header of the export column; variants are reworded headers.
    return {"headers": [text, *variant_headers], "question_key": key, "text": text, "kind": kind}


_GFORMS_FORMS: Dict[str, Dict[str, Any]] = {
    "General Public": {
        "general_public_english_launch": {
            "name": "General Public (English launch)",
            "questions": [
                _gforms_question("AGE_IDENTIFICATION", _GFORMS_METADATA, "How old are you?"),
                _gforms_question("GENDER_IDENTIFICATION", _GFORMS_METADATA, "What is your gender?"),
                _gforms_question("STATE_IDENTIFICATION", _GFORMS_METADATA, "Which state do you stay in?"),
                _gforms_question("DISTRICT_IDENTIFICATION", _GFORMS_METADATA, "Which district do you stay in?"),
                _gforms_question("CURRENT_STATUS", _GFORMS_METADATA,
                                 "What do you currently do - your work or occupation?"),
                _gforms_question("DEFFERENCE", _GFORMS_OPEN,
                                 "How is life different for young people today compared to when you were younger?"),
                _gforms_question("LEARNING_NEEDS_FOR_FUTURE", _GFORMS_OPEN,
                                 "What do you think young people need to learn or experience to thrive in the future?"),
                _gforms_question("LEARNING_NEEDS_FOR_FUTURE", _GFORMS_OPEN,
                                 "Where do you think they can get these skills and experiences?"),
                _gforms_question("HOPE", _GFORMS_OPEN, "What is your one biggest hope for young people around you?"),
                _gforms_question("WORRY", _GFORMS_OPEN, "What is your one biggest worry for young people around you?"),
                _gforms_question("ACHIEVE", _GFORMS_OPEN,
                                 "What do you think you can do to help young people achieve the future they want?"),
            ],
        },
    },
    "Parent": {
        "parents_english_launch": {
            "name": "Parents (English Launch)",
            "questions": [
                _gforms_question("STATE_IDENTIFICATION", _GFORMS_METADATA, "Which State do you stay in?"),
                _gforms_question("DISTRICT_IDENTIFICATION", _GFORMS_METADATA,
                                 "Please tell us the name of the district where you child attends school"),
                _gforms_question("RESPONDENT_RELATION", _GFORMS_METADATA,
                                 "Are you the child's mother, father or guardian?"),
                _gforms_question("SCHOOL_TYPE", _GFORMS_METADATA,
                                 "Does your child study in government school or private school?"),
                _gforms_question("CHILD_CLASS", _GFORMS_METADATA, "In which class/grade does your child study?"),
                _gforms_question("PARENT_EDUCATION", _GFORMS_METADATA,
                                 "Did you get an opportunity to go to school? If yes, up to which class did you study?"),
                _gforms_question("SCHOOL_VISIT_FREQUENCY", _GFORMS_OPEN,
                                 "Over the last 6 months, how many times have you visited your child's school?"),
                _gforms_question("SCHOOL_VISIT_REASONS", _GFORMS_OPEN,
                                 "Why do you usually visit your child's school?"),
                _gforms_question("ACADEMIC_PROGRESS_SATISFACTION", _GFORMS_OPEN,
                                 "How satisfied are you with how your child is doing in studies? "
                                 "Please provide reasons why you think so"),
                _gforms_question("FUTURE_ASPIRATION", _GFORMS_OPEN,
                                 "When your child grows up, what kind of work and life do you wish they will have?"),
                _gforms_question("SCHOOL_IMPROVEMENTS", _GFORMS_OPEN,
                                 "What are 2-3 things you would like to see improved in your child's school/college, "
                                 "to better prepare them for their future?"),
                _gforms_question("SAFE_AND_WELCOMING_SCHOOL", _GFORMS_OPEN,
                                 "What are 2-3 things that will make the school more safe and welcoming for your child?"),
                _gforms_question("HOME_LEARNING_SUPPORT", _GFORMS_OPEN,
                                 "What all do you do at home to support your child's learning?"),
                _gforms_question("SUPPORT_EASE", _GFORMS_OPEN,
                                 "Many parents find it hard to help their child's education. "
                                 "Is it hard or easy for you? Why or why not?"),
                _gforms_question("KEY_SUPPORT_FOR_FUTURE", _GFORMS_OPEN,
                                 "What is one thing that would help your child most to reach the future they want?"),
            ],
        },
    },
    # Youth (English Launch) and Karnataka (Mysore) Youth (V-Launch, bilingual) are one form: the
    # same questions, with one reworded header ("For the things ..." / "Out of the things ...").
    "Youth": {
        "youth_launch": {
            "name": "Youth (English Launch and Karnataka V-Launch)",
            "questions": [
                _gforms_question("AGE_IDENTIFICATION", _GFORMS_METADATA, "How old are you?"),
                _gforms_question("GENDER_IDENTIFICATION", _GFORMS_METADATA, "What is your gender?"),
                _gforms_question("STATE_IDENTIFICATION", _GFORMS_METADATA, "Which state do you stay in?"),
                _gforms_question("DISTRICT_IDENTIFICATION", _GFORMS_METADATA, "Which district do you stay in?"),
                _gforms_question("CURRENT_STATUS", _GFORMS_METADATA,
                                 "Right now, are you studying, working, or doing something else?"),
                _gforms_question("CURRENT_STATUS", _GFORMS_METADATA,
                                 "If you are studying, what class/ course are you studying?"),
                _gforms_question("SCHOOL_TYPE", _GFORMS_METADATA,
                                 "Did you or are you attending a government, government-aided or private "
                                 "education institution?"),
                _gforms_question("HOUSEHOLD_INCOME", _GFORMS_METADATA, "What is your average annual household income?"),
                _gforms_question("DISABILITY_STATUS", _GFORMS_METADATA, "Do you have any disabilities?"),
                _gforms_question("DISABILITY_STATUS", _GFORMS_METADATA,
                                 "If you said yes to the above question and if you are comfortable sharing, "
                                 "what kind of disability it is?"),
                _gforms_question("FUTURE_LIFE_AND_WORK_VISION", _GFORMS_OPEN,
                                 "When you think about your future or when you grow up, what do you want your "
                                 "life and work to look like?"),
                _gforms_question("CONFIDENCE", _GFORMS_OPEN,
                                 "Do you feel confident that you can achieve the work and life you want?"),
                _gforms_question("LEARNING_NEEDS_FOR_FUTURE", _GFORMS_OPEN,
                                 "What do you think you have to learn or do to achieve the future you want?"),
                _gforms_question("CHANCE_TO_LEARN_NOW", _GFORMS_OPEN,
                                 "For the things you just mentioned, are you getting the chance to learn them right now?",
                                 "Out of the things you just mentioned, are you getting the chance to learn them right now?"),
                _gforms_question("CHANCE_TO_LEARN_NOW", _GFORMS_OPEN,
                                 "Can you give examples of what or who helped you learn and build these skills?"),
                _gforms_question("SUPPORT_FROM_ENVIRONMENT", _GFORMS_OPEN,
                                 "What more could your school, college, community, friends, or family do to support you?"),
                _gforms_question("ADVICE_AND_INFORMATION_SOURCES", _GFORMS_OPEN,
                                 "When you have questions about how to prepare for your future, who do you talk to "
                                 "or where do you get this information from?"),
                _gforms_question("ADDITIONAL_COMMENTS", _GFORMS_OPEN,
                                 "Is there something you would like to do to help make learning and education "
                                 "better for young people like you in the future?"),
            ],
        },
    },
}


def gforms_config_v1(persona: str) -> Dict[str, Any]:
    """The version-1 `config_version.config` JSON for a persona's Google Forms upload (wide CSV)."""
    return {
        "layout": "wide",
        "timestamp_column": "Timestamp",
        # Google exports are month-first (9/26/2026 9:58:05) and carry no timezone.
        "datetime": {"formats": ["%m/%d/%Y %H:%M:%S"], "fallback_timezone": "Asia/Kolkata"},
        "id_strategy": "generate_uuid4",
        "default_completion": COMPLETION_COMPLETED,
        # PII is never stored; Google Sheets adds stray blank "Column N" columns.
        "ignored_columns": ["Email Address"],
        "ignored_column_patterns": ["^Column \\d+$"],
        # 'kn' when any answer contains Kannada script, else the default.
        # Bilingual forms print the choice and demographic answers in both languages, so script detection looks at
        # open-question answers only; otherwise every Karnataka response would be labelled kn.
        "language": {"default": "en", "script_detect": {"kn": "[\\u0C80-\\u0CFF]"}, "detect_kinds": ["open"]},
        "forms": copy.deepcopy(_GFORMS_FORMS[persona]),
    }
