"""
Constants of the feedback metadata extraction (one LLM call per stored feedback session; see
docs/feedback-extraction-and-dots-queries.md).

The field lists, bands and prompt names of each persona are CONFIG DATA, not code: the seed of the extraction
config (config_type 'feedback_extraction') is in seed_config.sql, and the live values are the `config_version` rows
in the database, changed with a new config version, never by a deploy (docs/feedback-extraction-bands.md). What
stays here is what the engine (app/services/feedback_extraction.py) needs to validate and apply such a config: the
step / prompt names, the field kinds, the statuses, the limits and the PII defaults.
"""
from typing import Dict, Tuple

# Workflow step name, prompt analysis_type and the llm_logs.analysis_type of the step.
STEP_NAME = "feedback_metadata_extraction"
ANALYSIS_TYPE = STEP_NAME
PROMPT_NAME = "Feedback Metadata Extraction"   # the generic prompt: the fallback of a config without `prompt_name`

CONFIG_TYPE_FEEDBACK_EXTRACTION = "feedback_extraction"
CONFIG_SOURCE_ANY = "any"
EXTRACTION_CONFIG_SCHEMA_VERSION = 1

# feedback_extracted_data.status
STATUS_EXTRACTED = "extracted"
STATUS_NOT_PROVIDED = "not_provided"
STATUS_UNMAPPED = "unmapped"
EXTRACTION_STATUSES = (STATUS_EXTRACTED, STATUS_NOT_PROVIDED, STATUS_UNMAPPED)

KIND_CATEGORY = "category"
KIND_FREE_TEXT = "free_text"
KIND_INTEGER = "integer"   # a whole number inside [min, max] (an age); stored as its canonical decimal string
FIELD_KINDS = (KIND_CATEGORY, KIND_FREE_TEXT, KIND_INTEGER)

# Limits that keep one bad session (or one injection attempt) from blowing up a prompt or a row.
# Respondent messages are NEVER truncated (PII after a cut would be lost). A session with a respondent message longer
# than MAX_RESPONDENT_MESSAGE_CHARS is not sent to the model at all: it is recorded as pii_detected TRUE (type
# 'other'), so it is excluded from the export and a person can look at it. Bot messages are the questionnaire's own
# text; a bot message longer than MAX_BOT_MESSAGE_CHARS is cut (it cannot carry respondent PII).
MAX_RESPONDENT_MESSAGE_CHARS = 20000
MAX_BOT_MESSAGE_CHARS = 4000
MAX_FREE_TEXT_VALUE_CHARS = 120  # a normalised free-text value (a district name) longer than this is not a district
MAX_RAW_VALUE_CHARS = 500
MAX_PII_TYPES = 20

# A run of at least this many digits in a respondent message (spaces, dashes, dots and parentheses between digits
# ignored) is treated as a phone or id number whatever the model says. Ages, years, classes and 4-7 digit amounts pass.
PII_MIN_DIGIT_RUN = 8

# A free-text value (district) that is only one of these is a placeholder, not an answer: status not_provided.
# Compared case-insensitively after trimming spaces and surrounding punctuation.
FREE_TEXT_PLACEHOLDERS = frozenset({
    "n/a", "na", "n.a", "nil", "null", "none", "nan", "undefined", "unknown", "not known", "not provided",
    "not specified", "not applicable", "not available", "not mentioned", "not given", "not answered",
    "no answer", "no district", "nothing", "not sure", "dont know", "don't know", "do not know", "skip", "skipped",
})

# Kinds of personal information the model reports (config `pii.types` overrides this list per persona).
# 'other' must always be present: a type the model names that is not in the list is recorded as 'other'.
PII_TYPES: Tuple[str, ...] = ("name", "phone", "email", "address", "school_name", "id_number", "other")
PII_TYPE_DESCRIPTIONS: Dict[str, str] = {
    "name": "a personal name of the respondent or of anyone they mention (family, teacher, friend)",
    "phone": "a phone number",
    "email": "an email address or a social media handle",
    "address": "a home address, street, house number, or a village/locality small enough to identify a person",
    "school_name": "the name of a specific school, college, institution or workplace",
    "id_number": "an identity or document number (Aadhaar, PAN, roll number, admission number)",
    "other": "any other detail that could identify a person",
}
