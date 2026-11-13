"""
Unit tests of the pure validators for the per-bot feedback settings that live in the persona's upload config
(app/services/feedback_config_validation.py) and of the seeded defaults (app/services/feedback_constants.py).
"""
import copy

import pytest

from app.services.feedback_config_validation import (
    MINIMUM_CREATE_REQUIRED,
    validate_config_value,
    validate_expect_stages,
    validate_feedback_config_keys,
    validate_ingestion_schema_config,
    validate_max_csv_upload_bytes,
    validate_process_steps,
)
from app.services.feedback_constants import (
    DEFAULT_INGESTION_SCHEMA,
    DEFAULT_MAX_CSV_UPLOAD_BYTES,
    DEFAULT_PROCESS_STEPS,
    FEEDBACK_METADATA_EXTRACTION_STEP,
    REQUIRED_FEEDBACK_CONFIG_KEYS,
    feedback_config_key_defaults,
)
from seed_support import PERSONAS, gforms_config_v1, mitra_config_v1


# seeds ----------------------------------------------------------------------------------------

@pytest.mark.parametrize("persona", list(PERSONAS))
def test_every_mitra_seed_carries_the_three_settings(persona):
    config = mitra_config_v1(persona)
    for key in REQUIRED_FEEDBACK_CONFIG_KEYS:
        assert key in config
    validate_feedback_config_keys(config)
    assert config["max_csv_upload_bytes"] == 104857600  # the value the .env default had
    assert config["process_steps"] == []  # extraction is opt-in (an LLM call per submission)
    assert "expect_stages" not in config  # optional, never seeded


@pytest.mark.parametrize("persona", ["Youth", "Parent", "General Public"])
def test_every_google_forms_seed_carries_the_three_settings(persona):
    config = gforms_config_v1(persona)
    validate_feedback_config_keys(config)
    assert config["process_steps"] == []
    assert config["ingestion_schema"] == DEFAULT_INGESTION_SCHEMA


def test_defaults_are_fresh_copies():
    a, b = feedback_config_key_defaults(), feedback_config_key_defaults()
    a["process_steps"].append({"name": "x"})
    a["ingestion_schema"]["create"]["required"].clear()
    assert b["process_steps"] == DEFAULT_PROCESS_STEPS and DEFAULT_PROCESS_STEPS == []
    assert len(b["ingestion_schema"]["create"]["required"]) > 0
    assert DEFAULT_MAX_CSV_UPLOAD_BYTES == 104857600


def test_the_default_schema_passes_its_own_validator_and_keeps_the_session_fields_required():
    validate_ingestion_schema_config(DEFAULT_INGESTION_SCHEMA)
    required = DEFAULT_INGESTION_SCHEMA["create"]["required"]
    assert "data.sessionType" in required and "data.questionnaireVersion" in required
    assert all(path in required for path in MINIMUM_CREATE_REQUIRED)


# max_csv_upload_bytes -------------------------------------------------------------------------

@pytest.mark.parametrize("value", [1, 50_000_000, 104857600])
def test_cap_accepts_positive_ints(value):
    assert validate_max_csv_upload_bytes(value) == value


@pytest.mark.parametrize("value", [0, -5, "100", 1.5, 1e8, True, False, None, [1], {"a": 1}])
def test_cap_rejects_everything_else(value):
    with pytest.raises(ValueError, match="max_csv_upload_bytes.*positive integer"):
        validate_max_csv_upload_bytes(value)


# ingestion_schema -----------------------------------------------------------------------------

def _schema():
    return copy.deepcopy(DEFAULT_INGESTION_SCHEMA)


def test_schema_may_require_more_and_may_omit_optional():
    schema = _schema()
    schema["create"]["required"].append("data.botId")
    del schema["create"]["optional"]
    assert validate_ingestion_schema_config(schema) is schema


@pytest.mark.parametrize("bad", [None, [], "x", {}, {"create": {}}, {"create": {}, "update": {}}])
def test_schema_needs_the_three_sections(bad):
    with pytest.raises(ValueError, match="'create', 'update' and 'delete'"):
        validate_ingestion_schema_config(bad)


def test_schema_section_must_be_an_object():
    schema = _schema()
    schema["update"] = "nope"
    with pytest.raises(ValueError, match="ingestion_schema.update' must be an object"):
        validate_ingestion_schema_config(schema)


@pytest.mark.parametrize("section,field,value", [
    ("create", "required", "submissionId"),
    ("create", "required", ["ok", 3]),
    ("create", "required", ["ok", " "]),
    ("create", "optional", [None]),
    ("delete", "required", {"a": 1}),
])
def test_schema_lists_must_hold_non_empty_strings(section, field, value):
    schema = _schema()
    schema[section][field] = value
    with pytest.raises(ValueError, match=f"{section}.{field}' must be a list of non-empty strings"):
        validate_ingestion_schema_config(schema)


def test_schema_new_values_no_empty_must_be_a_bool():
    schema = _schema()
    schema["update"]["newValuesNoEmpty"] = "yes"
    with pytest.raises(ValueError, match="newValuesNoEmpty' must be a boolean"):
        validate_ingestion_schema_config(schema)


def test_schema_create_needs_a_required_list():
    schema = _schema()
    del schema["create"]["required"]
    with pytest.raises(ValueError, match="create' needs a 'required' list"):
        validate_ingestion_schema_config(schema)


@pytest.mark.parametrize("path", MINIMUM_CREATE_REQUIRED)
def test_schema_must_keep_the_minimum_required_paths(path):
    schema = _schema()
    schema["create"]["required"].remove(path)
    with pytest.raises(ValueError, match=f"must include: {path}"):
        validate_ingestion_schema_config(schema)


# process_steps --------------------------------------------------------------------------------

def test_steps_accept_the_story_format():
    steps = [
        {"name": "feedback_metadata_extraction"},
        {"name": "pii_and_abusive_language_detection", "columns": ["message"], "llm_model": "m",
         "max_tokens": 2048, "llm_timeout_seconds": 60},
        {"name": "x", "llm_timeout_seconds": 1.5},
    ]
    assert validate_process_steps(steps) is steps


def test_empty_steps_are_valid():
    assert validate_process_steps([]) == []  # the submission is then marked success at ingestion


@pytest.mark.parametrize("bad", [None, {}, "x", 3, ({"name": "a"},)])
def test_steps_must_be_a_list(bad):
    with pytest.raises(ValueError, match="process_steps' must be a list"):
        validate_process_steps(bad)


@pytest.mark.parametrize("step,needle", [
    ("x", "must be an object"),
    ({}, "non-empty string 'name'"),
    ({"name": ""}, "non-empty string 'name'"),
    ({"name": "  "}, "non-empty string 'name'"),
    ({"name": 3}, "non-empty string 'name'"),
    ({"name": "a", "columns": "x"}, "columns must be a list"),
    ({"name": "a", "columns": [""]}, "columns must be a list"),
    ({"name": "a", "llm_model": ""}, "llm_model"),
    ({"name": "a", "max_tokens": 0}, "max_tokens"),
    ({"name": "a", "max_tokens": True}, "max_tokens"),
    ({"name": "a", "max_tokens": 1.5}, "max_tokens"),
    ({"name": "a", "llm_timeout_seconds": 0}, "llm_timeout_seconds"),
    ({"name": "a", "llm_timeout_seconds": "60"}, "llm_timeout_seconds"),
    ({"name": "a", "colums": []}, "unknown key"),
])
def test_invalid_steps_are_rejected_with_their_position(step, needle):
    with pytest.raises(ValueError, match=r"process_steps\[1\]'.*" + needle):
        validate_process_steps([{"name": "ok"}, step])


# expect_stages --------------------------------------------------------------------------------

def test_expect_stages_accepts_a_list_of_unique_names_and_the_empty_list():
    assert validate_expect_stages(["A", "B"]) == ["A", "B"]
    assert validate_expect_stages([]) == []


@pytest.mark.parametrize("bad", [None, "A", {"A": 1}, ["A", ""], ["A", " "], ["A", 3], [None]])
def test_expect_stages_rejects_non_lists_and_blank_names(bad):
    with pytest.raises(ValueError, match="expect_stages' must be a list of non-empty strings"):
        validate_expect_stages(bad)


def test_expect_stages_rejects_duplicates():
    with pytest.raises(ValueError, match="must not repeat"):
        validate_expect_stages(["A", "B", "A"])


# whole config ---------------------------------------------------------------------------------

@pytest.mark.parametrize("key", REQUIRED_FEEDBACK_CONFIG_KEYS)
def test_missing_required_key_names_the_key_and_the_migrate_tool(key):
    config = mitra_config_v1("Youth")
    del config[key]
    with pytest.raises(ValueError, match=f"missing '{key}'.*tools/feedback_config.py migrate"):
        validate_feedback_config_keys(config)


def test_invalid_expect_stages_is_rejected_but_absence_is_fine():
    config = mitra_config_v1("Youth")
    validate_feedback_config_keys(config)
    config["expect_stages"] = "AGE"
    with pytest.raises(ValueError, match="expect_stages"):
        validate_feedback_config_keys(config)


def test_config_must_be_an_object():
    with pytest.raises(ValueError, match="must be an object"):
        validate_feedback_config_keys([])


def test_validate_config_value_dispatches_by_key_and_rejects_unknown_keys():
    assert validate_config_value("max_csv_upload_bytes", 5) == 5
    with pytest.raises(ValueError, match="positive integer"):
        validate_config_value("max_csv_upload_bytes", "5")
    with pytest.raises(ValueError, match="'columns' is not one of"):
        validate_config_value("columns", {})
