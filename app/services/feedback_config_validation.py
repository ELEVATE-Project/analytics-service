"""
Pure validators (no I/O) for the settings that live in a feedback persona's upload config
(config_version.config of a `feedback_upload` config), because every feedback bot can have a different input
structure and .env must not carry things that change:

    "max_csv_upload_bytes": 104857600                       positive int; the size cap of one upload
    "ingestion_schema":     {"create": {...}, "update": {...}, "delete": {...}}   required / optional event paths
    "process_steps":        [{"name": "feedback_metadata_extraction"}]          steps run after ingestion
    "expect_stages":        ["AGE_IDENTIFICATION", ...]          OPTIONAL; report-only (never skips a session)

Each validator returns the checked value and raises ValueError with a message that names the key, so a bad config
is rejected at the point of use with a clear reason (never silently replaced by a default). The seed is seed_config.sql and the
defaults are in app/services/feedback_constants.py; `python tools/feedback_config.py` shows / migrates / changes them.
"""
from typing import Any, Dict, List

from app.services.feedback_constants import (
    CONFIG_KEY_EXPECT_STAGES,
    CONFIG_KEY_INGESTION_SCHEMA,
    CONFIG_KEY_MAX_CSV_UPLOAD_BYTES,
    CONFIG_KEY_PROCESS_STEPS,
    REQUIRED_FEEDBACK_CONFIG_KEYS,
)

# Paths every feedback ingestion schema must require: the consumer cannot find the persona's config, stamp the
# version or store the session without them. A config may require more, never fewer.
MINIMUM_CREATE_REQUIRED = (
    "submissionId", "tenantCode", "tags.programId", "data.source", "data.sessionType",
    "data.questionnaireVersion", "data.messages",
)

# A delete event is matched by these two (a delete carries no persona).
MINIMUM_DELETE_REQUIRED = ("submissionId", "tenantCode")

# Upper bound of max_csv_upload_bytes (1 TiB): far above any real upload, small enough that `file.read(cap + 1)`
# can never overflow into a 500.
MAX_CSV_UPLOAD_BYTES_LIMIT = 2 ** 40

_STEP_KEYS = ("name", "columns", "llm_model", "max_tokens", "llm_timeout_seconds")


def _is_int(value: Any) -> bool:
    """A real int; bool is an int subclass in Python, so it is excluded explicitly."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return (isinstance(value, (int, float))) and not isinstance(value, bool)


def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and len(value.strip()) > 0


def validate_max_csv_upload_bytes(value: Any) -> int:
    if not _is_int(value) or value <= 0:
        raise ValueError(
            f"Feedback config '{CONFIG_KEY_MAX_CSV_UPLOAD_BYTES}' must be a positive integer (bytes), got {value!r}"
        )
    if value > MAX_CSV_UPLOAD_BYTES_LIMIT:
        raise ValueError(
            f"Feedback config '{CONFIG_KEY_MAX_CSV_UPLOAD_BYTES}' must be at most {MAX_CSV_UPLOAD_BYTES_LIMIT} bytes "
            f"(2**40), got {value!r}"
        )
    return value


def validate_ingestion_schema_config(value: Any) -> Dict[str, Any]:
    """The same structure the story/discussion schemas have: create / update / delete sections."""
    key = CONFIG_KEY_INGESTION_SCHEMA
    if not isinstance(value, dict) or not {"create", "update", "delete"} <= value.keys():
        raise ValueError(f"Feedback config '{key}' must be an object with 'create', 'update' and 'delete' sections")
    for section_name in ("create", "update", "delete"):
        section = value[section_name]
        if not isinstance(section, dict):
            raise ValueError(f"Feedback config '{key}.{section_name}' must be an object, got {type(section).__name__}")
        for list_field in ("required", "optional"):
            if list_field in section:
                items = section[list_field]
                if not isinstance(items, list) or not all(_non_empty_str(item) for item in items):
                    raise ValueError(
                        f"Feedback config '{key}.{section_name}.{list_field}' must be a list of non-empty strings"
                    )
        if "newValuesNoEmpty" in section and not isinstance(section["newValuesNoEmpty"], bool):
            raise ValueError(f"Feedback config '{key}.{section_name}.newValuesNoEmpty' must be a boolean")
    if "required" not in value["create"]:
        raise ValueError(f"Feedback config '{key}.create' needs a 'required' list")
    missing = [path for path in MINIMUM_CREATE_REQUIRED if path not in value["create"]["required"]]
    if len(missing) > 0:
        raise ValueError(f"Feedback config '{key}.create.required' must include: {', '.join(missing)}")
    if "required" not in value["delete"]:
        raise ValueError(f"Feedback config '{key}.delete' needs a 'required' list")
    missing = [path for path in MINIMUM_DELETE_REQUIRED if path not in value["delete"]["required"]]
    if len(missing) > 0:
        raise ValueError(f"Feedback config '{key}.delete.required' must include: {', '.join(missing)}")
    return value


def validate_process_steps(value: Any) -> List[Dict[str, Any]]:
    """Same format as PROCESS_CONFIG_STORY: a list of {"name", optional "columns", llm_model, max_tokens,
    llm_timeout_seconds}. An empty list is valid (a submission is then marked success at ingestion)."""
    key = CONFIG_KEY_PROCESS_STEPS
    if not isinstance(value, list):
        raise ValueError(f"Feedback config '{key}' must be a list of steps, got {type(value).__name__}")
    for position, step in enumerate(value):
        label = f"Feedback config '{key}[{position}]'"
        if not isinstance(step, dict):
            raise ValueError(f"{label} must be an object, got {type(step).__name__}")
        if not _non_empty_str(step.get("name")):
            raise ValueError(f"{label} needs a non-empty string 'name'")
        unknown = sorted(k for k in step if k not in _STEP_KEYS)
        if len(unknown) > 0:
            raise ValueError(f"{label} has unknown key(s): {', '.join(unknown)} (allowed: {', '.join(_STEP_KEYS)})")
        if "columns" in step:
            columns = step["columns"]
            if not isinstance(columns, list) or not all(_non_empty_str(c) for c in columns):
                raise ValueError(f"{label}.columns must be a list of non-empty strings")
        if "llm_model" in step and not _non_empty_str(step["llm_model"]):
            raise ValueError(f"{label}.llm_model must be a non-empty string")
        if "max_tokens" in step and (not _is_int(step["max_tokens"]) or step["max_tokens"] <= 0):
            raise ValueError(f"{label}.max_tokens must be a positive integer")
        if "llm_timeout_seconds" in step and (not _is_number(step["llm_timeout_seconds"]) or step["llm_timeout_seconds"] <= 0):
            raise ValueError(f"{label}.llm_timeout_seconds must be a positive number")
    return value


def validate_expect_stages(value: Any) -> List[str]:
    """Optional list of stage names an upload expects (report-only): non-empty, unique strings."""
    key = CONFIG_KEY_EXPECT_STAGES
    if not isinstance(value, list) or not all(_non_empty_str(item) for item in value):
        raise ValueError(f"Feedback config '{key}' must be a list of non-empty strings")
    if len(set(value)) != len(value):
        raise ValueError(f"Feedback config '{key}' must not repeat a stage name")
    return value


_VALIDATORS = {
    CONFIG_KEY_MAX_CSV_UPLOAD_BYTES: validate_max_csv_upload_bytes,
    CONFIG_KEY_INGESTION_SCHEMA: validate_ingestion_schema_config,
    CONFIG_KEY_PROCESS_STEPS: validate_process_steps,
    CONFIG_KEY_EXPECT_STAGES: validate_expect_stages,
}


def validate_config_value(key: str, value: Any) -> Any:
    """Validate one of the four keys by name. Raises ValueError (also for an unknown key)."""
    validator = _VALIDATORS.get(key)
    if validator is None:
        raise ValueError(f"'{key}' is not one of: {', '.join(_VALIDATORS)}")
    return validator(value)


def validate_feedback_config_keys(config: Dict[str, Any]) -> None:
    """
    The three required keys must be present and valid, and the optional expect_stages valid when present.
    Raises ValueError naming the first problem; the hint tells ops how to add a missing key to an older config.
    """
    if not isinstance(config, dict):
        raise ValueError(f"Feedback config must be an object, got {type(config).__name__}")
    for key in REQUIRED_FEEDBACK_CONFIG_KEYS:
        if key not in config:
            raise ValueError(
                f"Feedback config is missing '{key}' (add it with: python tools/feedback_config.py migrate)"
            )
        validate_config_value(key, config[key])
    if CONFIG_KEY_EXPECT_STAGES in config:
        validate_expect_stages(config[CONFIG_KEY_EXPECT_STAGES])


def config_setting_problems(config: Dict[str, Any]) -> Dict[str, str]:
    """{key: message} for every required key that is missing or invalid (and expect_stages when invalid)."""
    problems: Dict[str, str] = {}
    for key in (*REQUIRED_FEEDBACK_CONFIG_KEYS, CONFIG_KEY_EXPECT_STAGES):
        if key not in config:
            if key in REQUIRED_FEEDBACK_CONFIG_KEYS:
                problems[key] = "missing"
            continue
        try:
            validate_config_value(key, config[key])
        except ValueError as exc:
            problems[key] = str(exc)
    return problems
