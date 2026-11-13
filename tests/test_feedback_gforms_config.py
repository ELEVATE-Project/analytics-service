"""
Tests for the Google Forms upload config (docs/feedback-upload-plan.md, Phase 2, slice 2a):
`gforms_config_v1(persona)` in app/services/feedback_constants.py.

The structural checks are synthetic. The "real launch form" checks read only the HEADER row of the
exports under 1MV_POC/Google forms data (never an answer) and are skipped when those files are absent.
"""
import csv
import json
import re
from pathlib import Path

import pytest

from seed_support import GFORMS_PERSONAS, PERSONAS, SESSION_TYPE_TO_PERSONA, gforms_config_v1, mitra_config_v1

try:  # the shared rule lives with the adapter; fall back to an identical local copy until it exists
    from app.api.services.feedback_gforms import normalise_header
except ImportError:  # pragma: no cover - only before the adapter module is written
    def normalise_header(header: str) -> str:
        header = re.sub(r"\s+", " ", header).strip()
        for i, ch in enumerate(header):
            if ord(ch) > 127:
                header = header[:i]
                break
        header = header.rstrip(' /"”“')
        return re.sub(r"\s+", " ", header).strip()

REPO = Path(__file__).resolve().parent.parent
GFORMS_DIR = REPO / "1MV_POC" / "Google forms data"

# persona -> the launch-form exports whose headers must all resolve in that persona's config
LAUNCH_FILES = {
    "General Public": ["General public/1 Million Voices_Test Tools - General Public (English launch).csv"],
    "Parent": ["Parents/1 Million Voices_Test Tools - Parents (English Launch).csv"],
    "Youth": [
        "Youth/1 Million Voices_Test Tools - Youth (English Launch).csv",
        "Youth/1 Million Voices_Test Tools - Karnataka(Mysore)- Youth (V-Launch).csv",
    ],
}


def _questions(persona):
    for form in gforms_config_v1(persona)["forms"].values():
        yield from form["questions"]


def _header_map(persona):
    """normalised header -> question_key, across every form of the persona."""
    return {h: q["question_key"] for q in _questions(persona) for h in q["headers"]}


def _resolves(persona, header_row):
    """Mirrors the adapter's column rule: ignored by name / pattern, else the normalised header is configured."""
    cfg = gforms_config_v1(persona)
    known = _header_map(persona)
    unresolved = []
    for col in header_row:
        if col == cfg["timestamp_column"] or col in cfg["ignored_columns"]:
            continue
        if any(re.search(pattern, col) for pattern in cfg["ignored_column_patterns"]):
            continue
        if normalise_header(col) not in known:
            unresolved.append(col)
    return unresolved


def _read_header(relative):
    path = GFORMS_DIR / relative
    if not path.exists():
        pytest.skip(f"{path.name} not available")
    with path.open(encoding="utf-8-sig", newline="") as f:
        return next(csv.reader(f))


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_config_is_json_serialisable_and_round_trips(persona):
    cfg = gforms_config_v1(persona)
    assert json.loads(json.dumps(cfg)) == cfg


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_config_top_level_contract(persona):
    cfg = gforms_config_v1(persona)
    assert cfg["layout"] == "wide"
    assert cfg["timestamp_column"] == "Timestamp"
    assert cfg["id_strategy"] == "generate_uuid4"
    assert cfg["default_completion"] == "COMPLETED"
    assert cfg["datetime"] == {"formats": ["%m/%d/%Y %H:%M:%S"], "fallback_timezone": "Asia/Kolkata"}
    assert cfg["ignored_columns"] == ["Email Address"]
    assert cfg["ignored_column_patterns"] == ["^Column \\d+$"]
    # script detection looks at open-question answers only: bilingual forms print choice answers in both languages
    assert cfg["language"] == {"default": "en", "script_detect": {"kn": "[\\u0C80-\\u0CFF]"}, "detect_kinds": ["open"]}
    assert cfg["forms"], "at least one form"
    for pattern in cfg["ignored_column_patterns"] + list(cfg["language"]["script_detect"].values()):
        re.compile(pattern)
    # month-first, no timezone: the sample from the export notes parses with the configured format
    from datetime import datetime
    assert datetime.strptime("9/26/2026 9:58:05", cfg["datetime"]["formats"][0]) == datetime(2026, 9, 26, 9, 58, 5)


def test_each_call_returns_an_independent_copy():
    first = gforms_config_v1("Youth")
    first["forms"].clear()
    assert gforms_config_v1("Youth")["forms"]


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_every_question_is_complete(persona):
    for form_key, form in gforms_config_v1(persona)["forms"].items():
        assert isinstance(form_key, str) and form_key
        assert isinstance(form["name"], str) and form["name"]
        assert form["questions"]
        for q in form["questions"]:
            assert set(q) == {"headers", "question_key", "text", "kind"}
            assert isinstance(q["headers"], list) and q["headers"]
            assert all(isinstance(h, str) and h for h in q["headers"])
            assert isinstance(q["question_key"], str) and q["question_key"]
            assert re.fullmatch(r"[A-Z][A-Z0-9_]*", q["question_key"]), q["question_key"]
            assert isinstance(q["text"], str) and q["text"]
            assert q["kind"] in ("metadata", "open")


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_headers_are_already_normalised(persona):
    for q in _questions(persona):
        for h in q["headers"]:
            assert normalise_header(h) == h, h
        assert normalise_header(q["text"]) == q["text"]


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_no_header_maps_to_two_question_keys_within_a_form(persona):
    for form_key, form in gforms_config_v1(persona)["forms"].items():
        seen = {}
        for q in form["questions"]:
            for h in q["headers"]:
                assert seen.setdefault(h, q["question_key"]) == q["question_key"], (form_key, h)
        # and no header is listed twice for the same key either: one column, one question
        flat = [h for q in form["questions"] for h in q["headers"]]
        assert len(flat) == len(set(flat)), form_key


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_each_form_has_metadata_and_open_questions(persona):
    for form in gforms_config_v1(persona)["forms"].values():
        kinds = {q["kind"] for q in form["questions"]}
        assert kinds == {"metadata", "open"}


def test_each_persona_has_the_expected_launch_forms():
    assert {p: list(gforms_config_v1(p)["forms"]) for p in GFORMS_PERSONAS} == {
        "Youth": ["youth_launch"],
        "Parent": ["parents_english_launch"],
        "General Public": ["general_public_english_launch"],
    }
    assert {p: len(list(_questions(p))) for p in GFORMS_PERSONAS} == {"Youth": 18, "Parent": 15, "General Public": 11}


def test_demographic_questions_are_metadata():
    for persona in GFORMS_PERSONAS:
        for q in _questions(persona):
            if q["question_key"] in ("AGE_IDENTIFICATION", "GENDER_IDENTIFICATION",
                                     "STATE_IDENTIFICATION", "DISTRICT_IDENTIFICATION"):
                assert q["kind"] == "metadata", (persona, q["question_key"])


def test_persona_first_question_matches_the_mitra_catalog():
    # the Mitra config's synthesised first question must exist in the Google Forms form of the same persona,
    # with the same canonical key (one key = one question across sources)
    for persona, p in ((n, PERSONAS[n]) for n in GFORMS_PERSONAS):
        assert p["first_question_key"] in {q["question_key"] for q in _questions(persona)}


def test_youth_form_has_both_header_wordings():
    (form,) = gforms_config_v1("Youth")["forms"].values()
    (chance,) = [q for q in form["questions"]
                 if q["question_key"] == "CHANCE_TO_LEARN_NOW" and len(q["headers"]) > 1]
    assert chance["headers"] == [
        "For the things you just mentioned, are you getting the chance to learn them right now?",
        "Out of the things you just mentioned, are you getting the chance to learn them right now?",
    ]


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_launch_form_headers_resolve_for_the_matching_persona(persona):
    for relative in LAUNCH_FILES[persona]:
        header = _read_header(relative)
        assert header[0] == "Timestamp"
        assert _resolves(persona, header) == [], relative


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_launch_form_headers_do_not_resolve_for_other_personas(persona):
    others = [p for p in GFORMS_PERSONAS if p != persona]
    for relative in LAUNCH_FILES[persona]:
        header = _read_header(relative)
        for other in others:
            assert _resolves(other, header) != [], (relative, other)


def test_youth_form_resolves_both_english_and_karnataka_headers():
    english, karnataka = (_read_header(r) for r in LAUNCH_FILES["Youth"])
    assert _resolves("Youth", english) == []
    assert _resolves("Youth", karnataka) == []
    # the two exports differ in raw headers (bilingual vs English) but land on the same question keys
    known = _header_map("Youth")
    assert [known[normalise_header(c)] for c in english[1:]] == [known[normalise_header(c)] for c in karnataka[1:]]
    # ... and the only reworded English header is the one the config lists as a variant
    assert {normalise_header(c) for c in english[1:]} ^ {normalise_header(c) for c in karnataka[1:]} == {
        "For the things you just mentioned, are you getting the chance to learn them right now?",
        "Out of the things you just mentioned, are you getting the chance to learn them right now?",
    }


def test_parents_stray_column_and_email_address_are_ignored_not_unmapped():
    header = _read_header(LAUNCH_FILES["Parent"][0])
    assert "Column 16" in header
    assert _resolves("Parent", header) == []
    assert _resolves("Parent", ["Timestamp", "Email Address"]) == []
    # a genuinely unknown column is still reported
    assert _resolves("Parent", ["Timestamp", "Favourite colour"]) == ["Favourite colour"]


# ---------------------------------------------------------------------------------------------
# session_type and questionnaire_version
# ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_config_carries_the_personas_session_type(persona):
    cfg = gforms_config_v1(persona)
    assert cfg["session_type"] == PERSONAS[persona]["session_type"]
    assert SESSION_TYPE_TO_PERSONA[cfg["session_type"]] == persona


def test_session_types_of_the_three_google_form_personas():
    assert {p: gforms_config_v1(p)["session_type"] for p in GFORMS_PERSONAS} == {
        "Youth": "Youth_Voice", "Parent": "Parent_Voice", "General Public": "General_Public_Voice",
    }


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_every_form_has_a_questionnaire_version(persona):
    for form_key, form in gforms_config_v1(persona)["forms"].items():
        assert form["questionnaire_version"] == "V1", form_key
        assert isinstance(form["questionnaire_version"], str) and form["questionnaire_version"].strip()


def test_launch_form_versions_are_known_mitra_version_keys():
    # a form's version label uses the same vocabulary as the persona's Mitra questionnaire_versions
    for persona in GFORMS_PERSONAS:
        mitra_keys = {v["key"] for v in mitra_config_v1(persona)["questionnaire_versions"]}
        for form in gforms_config_v1(persona)["forms"].values():
            assert form["questionnaire_version"] in mitra_keys


def test_educator_has_no_google_form_definition():
    assert "Educator" in PERSONAS and "Educator" not in GFORMS_PERSONAS
    assert PERSONAS["Educator"]["gforms_config_id"] is None
    with pytest.raises(KeyError):
        gforms_config_v1("Educator")


def test_unknown_persona_raises_key_error():
    with pytest.raises(KeyError):
        gforms_config_v1("Nobody")


def test_google_form_personas_have_a_config_id_and_the_others_do_not():
    for name, persona in PERSONAS.items():
        assert (persona["gforms_config_id"] is not None) == (name in GFORMS_PERSONAS), name
