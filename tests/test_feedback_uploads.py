"""
Unit tests for app/api/services/feedback_uploads.py (pure parsing / validation / event building).
All fixtures are synthetic (tests/csv_uploads/feedback_*.csv or inline frames); no real respondent text.
"""
import json
from pathlib import Path

import pandas as pd
import pytest

from app.api.services.feedback_uploads import (
    build_feedback_events,
    check_feedback_file,
    parse_feedback_csv,
)
from app.services.feedback_constants import PERSONAS, mitra_config_v1

FIXTURES = Path(__file__).resolve().parent / "csv_uploads"
PARENT = PERSONAS["Parent"]
PROGRAM = {"id": PARENT["program_id"], "name": "1M Voice"}
LEADER = {"id": PARENT["leader_category_id"], "name": "Parent"}
PUBLISHED = "2026-10-20T00:00:00.000Z"

COLUMNS = ["bot_id", "session_status", "session", "submission_id", "language", "sequence",
           "message", "translated_message", "created_at", "stage", "sender_id"]


@pytest.fixture
def config():
    return mitra_config_v1("Parent")


def read_fixture(name: str) -> pd.DataFrame:
    return parse_feedback_csv((FIXTURES / name).read_bytes())


def build(df, config, **overrides):
    kwargs = dict(program_info=PROGRAM, leader_info=LEADER, tenant_code="mitra", source="mitra",
                  published_at=PUBLISHED)
    kwargs.update(overrides)
    return build_feedback_events(df, config, **kwargs)


def make_df(rows, columns=COLUMNS):
    """rows: dicts overriding a baseline row; missing keys default to a valid respondent row."""
    base = {"bot_id": "88", "session_status": "COMPLETED", "session": "s" * 32, "submission_id": "100",
            "language": "hi", "sequence": "1", "message": "m", "translated_message": "t",
            "created_at": "2026-10-15T08:00:00.000000+05:30", "stage": "STATE_IDENTIFICATION",
            "sender_id": "120"}
    data = [{**base, **r} for r in rows]
    return pd.DataFrame(data, columns=columns).astype(str)


def session_rows(items, **common):
    """items: (kind, stage, message, translated) in CSV order starting at sequence 1."""
    rows = []
    for n, (kind, stage, message, translated) in enumerate(items, 1):
        rows.append({"sequence": str(n), "stage": stage, "message": message, "translated_message": translated,
                     "sender_id": "1" if kind == "B" else "120",
                     "created_at": f"2026-10-15T08:00:{n:02d}.000000+05:30", **common})
    return rows


def by_sequence(event):
    return {m["sequence"]: m for m in event["data"]["messages"]}


# ---------------------------------------------------------------------------------- parsing

def test_parse_keeps_na_and_none_answers():
    raw = b"submission_id,message\n1,NA\n2,None\n3,\n"
    df = parse_feedback_csv(raw)
    assert df["message"].tolist() == ["NA", "None", ""]


def test_parse_handles_utf8_bom():
    df = parse_feedback_csv("﻿submission_id,message\n1,x\n".encode("utf-8"))
    assert df.columns.tolist() == ["submission_id", "message"]


@pytest.mark.parametrize("raw", [b"", b"   \n", b"submission_id,message\n", b"\xff\xfe\x00bad"])
def test_parse_rejects_empty_or_unreadable(raw):
    with pytest.raises(ValueError):
        parse_feedback_csv(raw)


def test_parse_fixture_keeps_na_none():
    df = read_fixture("feedback_valid_mitra.csv")
    assert {"NA", "None"} <= set(df["message"])


# ---------------------------------------------------------------------------------- file checks

def test_check_accepts_valid_file(config):
    assert check_feedback_file(read_fixture("feedback_valid_mitra.csv"), config, "88") == []


def test_check_rejects_file_with_other_persona_bot(config):
    errors = check_feedback_file(read_fixture("feedback_mixed_bots.csv"), config, "88")
    assert len(errors) == 1 and "89" in errors[0] and "88" in errors[0]


def test_check_rejects_blank_bot_id(config):
    df = make_df([{"bot_id": ""}, {"bot_id": "88", "sequence": "2"}])
    errors = check_feedback_file(df, config, "88")
    assert len(errors) == 1 and "blank" in errors[0]


def test_check_reports_missing_required_columns_case_insensitively(config):
    df = make_df([{}])
    df = df.drop(columns=["session_status", "stage"]).rename(columns={"submission_id": "Submission_ID"})
    errors = check_feedback_file(df, config, "88")
    assert len(errors) == 1
    assert "session_status" in errors[0] and "stage" in errors[0] and "submission_id" not in errors[0]


def test_check_allows_extra_columns(config):
    df = make_df([{}])
    df["audio_file"] = ""
    df["whatever"] = "x"
    assert check_feedback_file(df, config, "88") == []


# ---------------------------------------------------------------------------------- events

def test_valid_fixture_builds_one_event_per_session(config):
    df = read_fixture("feedback_valid_mitra.csv")
    events, skipped = build(df, config)
    assert skipped == []
    assert [e["submissionId"] for e in events] == ["900001", "900002", "900003", "900004"]
    e = events[0]
    assert e["submissionType"] == "feedback" and e["eventType"] == "create"
    assert e["tenantCode"] == "mitra" and e["sessionId"] == "a" * 32
    assert e["eventPublishedAt"] == PUBLISHED
    assert e["tags"] == {"programId": PARENT["program_id"], "programName": "1M Voice",
                         "leaderCategoryId": PARENT["leader_category_id"], "leaderCategoryName": "Parent",
                         "state": None, "district": None, "source": "mitra"}
    assert e["data"]["source"] == "mitra" and e["data"]["language"] == "hi"


def test_published_at_defaults_to_now(config):
    events, _ = build_feedback_events(make_df(session_rows([("R", "STATE_IDENTIFICATION", "a", "b")])), config,
                                      program_info=PROGRAM, leader_info=LEADER, tenant_code="mitra", source="mitra")
    stamp = events[0]["eventPublishedAt"]
    assert stamp.endswith("Z") and len(stamp) == len("2026-10-20T00:00:00.000Z")


def test_sequence_shift_and_synthesised_first_question(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "[hi] ans", "english ans"),
        ("B", "DISTRICT_IDENTIFICATION", "District q", "[hi] district q"),
        ("R", "DISTRICT_IDENTIFICATION", "[hi] d", "d"),
    ]))
    events, skipped = build(df, config)
    assert skipped == []
    msgs = events[0]["data"]["messages"]
    assert [m["sequence"] for m in msgs] == [1, 2, 3, 4]
    first = msgs[0]
    assert first["questionKey"] == "STATE_IDENTIFICATION"
    assert first["message"] == first["translatedMessage"] == config["questions"][0]["text"]
    assert first["isFollowUp"] is False and first["parentSequence"] is None
    # no earlier time is known: the synthesised question carries the first CSV row's time
    assert first["messageAt"] == msgs[1]["messageAt"] == events[0]["data"]["submissionDate"]
    assert msgs[1]["parentSequence"] == 1


def test_tail_only_session_is_skipped_with_clear_message(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "b"), ("B", "X", "q", "q")])
    tail = session_rows([("R", "STATE_IDENTIFICATION", "a", "b"), ("B", "X", "q", "q")],
                        session="t" * 32, submission_id="200")
    for r in tail:
        r["sequence"] = str(int(r["sequence"]) + 10)
    events, skipped = build(make_df(rows + tail), config)
    assert [e["submissionId"] for e in events] == ["100"]
    assert len(skipped) == 1 and skipped[0]["submissionId"] == "200"
    assert "starts at sequence 11" in skipped[0]["problems"][0] and "tail-only" in skipped[0]["problems"][0]


def test_first_stage_not_in_catalog_is_skipped(config):
    df = make_df(session_rows([("R", "SOMETHING_ELSE", "a", "b")]))
    events, skipped = build(df, config)
    assert events == [] and "SOMETHING_ELSE" in skipped[0]["problems"][0]


def test_bot_bot_and_respondent_respondent_runs(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "a", "a-en"),
        ("B", "Q2", "q2a", "q2a-l"),
        ("B", "Q2", "q2b", "q2b-l"),      # two bot messages in a row
        ("R", "Q2", "r1", "r1-en"),
        ("R", "Q2", "r2", "r2-en"),       # two respondent messages in a row
        ("R", "Q2", "r3", "r3-en"),
        ("B", "THANK_YOU", "bye", "bye-l"),
    ]))
    events, skipped = build(df, config)
    assert skipped == []
    m = by_sequence(events[0])
    assert m[1]["parentSequence"] is None                       # synthesised question
    assert m[2]["parentSequence"] == 1                          # first answer -> synthesised question
    assert m[3]["parentSequence"] is None and m[4]["parentSequence"] is None   # both bot rows are questions
    assert m[5]["parentSequence"] == 4 and m[6]["parentSequence"] == 4 and m[7]["parentSequence"] == 4
    assert m[8]["parentSequence"] is None                       # closing row
    assert m[8]["questionKey"] == "THANK_YOU"


def test_parent_links_ignore_sequence_parity(config):
    # a respondent row sitting on an odd stored sequence still points at the preceding bot row
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "a", "a"),
        ("R", "STATE_IDENTIFICATION", "a2", "a2"),
        ("B", "Q2", "q", "q"),
        ("R", "Q2", "r", "r"),
    ]))
    events, _ = build(df, config)
    m = by_sequence(events[0])
    assert (m[2]["parentSequence"], m[3]["parentSequence"], m[4]["parentSequence"], m[5]["parentSequence"]) == (1, 1, None, 4)


def test_unsorted_rows_are_ordered_by_sequence(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q"), ("R", "Q2", "r", "r")])
    events, skipped = build(make_df(list(reversed(rows))), config)
    assert skipped == []
    assert [m["sequence"] for m in events[0]["data"]["messages"]] == [1, 2, 3, 4]
    assert events[0]["data"]["submissionDate"] == events[0]["data"]["messages"][1]["messageAt"]


def test_non_default_respondent_sender_is_a_respondent(config):
    df = make_df(session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q"), ("R", "Q2", "r", "r")])
                 )
    df.loc[df["sequence"] == "3", "sender_id"] = "1598"
    events, _ = build(df, config)
    assert by_sequence(events[0])[4]["parentSequence"] == 3


def test_follow_up_is_bot_row_repeating_previous_bot_stage(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "a", "a"),
        ("B", "Q2", "q", "q"), ("R", "Q2", "r", "r"),
        ("B", "Q2", "probe", "probe"), ("R", "Q2", "r2", "r2"),
        ("B", "Q3", "q3", "q3"), ("R", "Q3", "r3", "r3"),
        ("B", "Q2", "again later", "again later"),      # not consecutive: not a follow-up
        ("B", "THANK_YOU", "bye", "bye"), ("B", "THANK_YOU", "bye2", "bye2"),   # closing never a follow-up
    ]))
    events, _ = build(df, config)
    flags = [(m["sequence"], m["isFollowUp"]) for m in events[0]["data"]["messages"]]
    assert [s for s, f in flags if f] == [5]      # CSV row 4 -> stored sequence 5


def test_follow_up_never_set_on_respondent_rows(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    for e in events:
        for m in e["data"]["messages"]:
            if m["parentSequence"] is not None:
                assert m["isFollowUp"] is False


def test_fixture_session_with_multi_message_answer_probe_and_closing(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    m = by_sequence(events[0])
    assert len(m) == 10                                   # 9 CSV rows + synthesised question
    assert m[4]["parentSequence"] == 3 and m[5]["parentSequence"] == 3   # district answer in two messages
    assert m[6]["questionKey"] == "SCHOOL_TYPE" and m[6]["isFollowUp"] is False
    assert m[8]["questionKey"] == "SCHOOL_TYPE" and m[8]["isFollowUp"] is True    # probe
    assert m[10]["questionKey"] == "THANK_YOU" and m[10]["parentSequence"] is None


def test_na_and_none_answers_survive_as_literal_strings(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    m = by_sequence(events[0])
    assert m[7]["message"] == "NA" and m[7]["translatedMessage"] == "NA"
    assert m[9]["message"] == "None" and m[9]["translatedMessage"] == "None"


# ---------------------------------------------------------------------------------- text invariant

def test_bot_rows_swap_columns_and_respondent_rows_copy(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    m = by_sequence(events[0])
    # bot row: CSV message = English, CSV translated_message = language shown
    assert m[3]["message"] == "[hi] district question" and m[3]["translatedMessage"] == "District question text"
    # respondent row: copied as is
    assert m[2]["message"] == "[hi] state answer" and m[2]["translatedMessage"] == "state answer one"


def test_english_session_holds_same_text_on_both_sides(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    en = [e for e in events if e["data"]["language"] == "en"][0]
    for msg in en["data"]["messages"]:
        assert msg["message"] == msg["translatedMessage"] and msg["message"] != ""
    m = by_sequence(en)
    assert m[3]["message"] == "English district question"     # bot row, translated column was empty
    assert m[2]["message"] == "english state answer"          # respondent row, translated column was empty


def test_empty_conversation_side_falls_back_to_english(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "", "english only"),     # respondent: message empty
        ("B", "Q2", "english q", ""),                          # bot: translated_message (= stored message) empty
    ]))
    events, skipped = build(df, config)
    assert skipped == []
    m = by_sequence(events[0])
    assert m[2]["message"] == "english only" and m[2]["translatedMessage"] == "english only"
    assert m[3]["message"] == "english q" and m[3]["translatedMessage"] == "english q"


def test_missing_english_side_stays_null_for_non_english_session(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "[hi] answer", ""),
        ("B", "Q2", "", "[hi] question"),                      # bot row with no English text
    ]))
    events, _ = build(df, config)
    m = by_sequence(events[0])
    assert m[2]["message"] == "[hi] answer" and m[2]["translatedMessage"] is None
    assert m[3]["message"] == "[hi] question" and m[3]["translatedMessage"] is None


def test_translated_message_column_may_be_absent(config):
    df = make_df(session_rows([("R", "STATE_IDENTIFICATION", "english answer", ""),
                               ("B", "Q2", "english q", "")], language="en"))
    df = df.drop(columns=["translated_message"])
    events, skipped = build(df, config)
    assert skipped == []
    assert by_sequence(events[0])[2]["translatedMessage"] == "english answer"


# ---------------------------------------------------------------------------------- completion, dates

def test_completion_status_taken_from_session_status(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    assert [e["data"]["completionStatus"] for e in events] == ["COMPLETED", "IN_PROGRESS", "COMPLETED", "COMPLETED"]


def test_datetimes_are_utc_with_millis_and_z(config):
    df = make_df([{"created_at": "2026-10-15T08:54:34.169642+05:30"}])
    events, _ = build(df, config)
    assert events[0]["data"]["submissionDate"] == "2026-10-15T03:24:34.169Z"
    assert events[0]["data"]["messages"][1]["messageAt"] == "2026-10-15T03:24:34.169Z"


def test_datetime_without_offset_uses_fallback_timezone(config):
    df = make_df([{"created_at": "2026-10-15 08:54:34"}])
    events, _ = build(df, config)
    assert events[0]["data"]["submissionDate"] == "2026-10-15T03:24:34.000Z"      # Asia/Kolkata
    config["datetime"]["fallback_timezone"] = "UTC"
    events, _ = build(df, config)
    assert events[0]["data"]["submissionDate"] == "2026-10-15T08:54:34.000Z"


def test_unknown_fallback_timezone_is_a_config_error(config):
    config["datetime"]["fallback_timezone"] = "Not/AZone"
    with pytest.raises(ValueError):
        build(make_df([{}]), config)


def test_short_fraction_and_z_suffix_parse(config):
    df = make_df([{"created_at": "2026-10-15T08:54:34.17Z"}])
    events, skipped = build(df, config)
    assert skipped == [] and events[0]["data"]["submissionDate"] == "2026-10-15T08:54:34.170Z"


# ---------------------------------------------------------------------------------- bad sessions

def test_bad_sessions_skipped_good_ones_still_build_with_all_problems(config):
    df = read_fixture("feedback_bad_sessions.csv")
    assert check_feedback_file(df, config, "88") == []
    events, skipped = build(df, config)
    assert [e["submissionId"] for e in events] == ["900201", "900203"]
    by_id = {s["submissionId"]: s for s in skipped}
    assert set(by_id) == {"900202", "900204", "900205"}

    bad = by_id["900202"]
    assert bad["sessionId"] == "2" * 32
    joined = " | ".join(bad["problems"])
    assert "duplicate sequence" in joined
    assert "session_status differs" in joined and "WAITING" in joined and "is not one of" in joined
    assert "language differs" in joined
    assert len(bad["problems"]) >= 4                                  # all problems, not just the first

    joined = " | ".join(by_id["900204"]["problems"])
    assert "both empty" in joined and "created_at" in joined

    blank = by_id["900205"]
    assert blank["sessionId"] is None and blank["problems"] == ["session is blank in 1 row(s)"]


def test_blank_ids_are_reported_as_none(config):
    df = make_df([{"submission_id": ""}, {"session": "", "submission_id": "7"}])
    events, skipped = build(df, config)
    assert events == []
    assert {(s["submissionId"], s["sessionId"]) for s in skipped} == {(None, "s" * 32), ("7", None)}


def test_submission_mapped_to_two_sessions_and_vice_versa_are_skipped(config):
    rows = (session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], submission_id="1", session="A" * 32)
            + session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], submission_id="1", session="B" * 32)
            + session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], submission_id="2", session="C" * 32)
            + session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], submission_id="3", session="C" * 32)
            + session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], submission_id="4", session="D" * 32))
    events, skipped = build(make_df(rows), config)
    assert [e["submissionId"] for e in events] == ["4"]
    assert len(skipped) == 4
    assert all("maps to 2" in s["problems"][0] for s in skipped)


@pytest.mark.parametrize("sequence, expected", [
    ("", "sequence is missing"),
    ("abc", "not an integer"),
    ("1.5", "not an integer"),
    ("0", "1 or greater"),
    ("-2", "not an integer"),
])
def test_bad_sequence_values_skip_session(config, sequence, expected):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])
    rows[1]["sequence"] = sequence
    events, skipped = build(make_df(rows), config)
    assert events == [] and expected in " | ".join(skipped[0]["problems"])


def test_blank_sender_and_blank_language_skip_session(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])
    rows[1]["sender_id"] = ""
    rows[0]["language"] = ""
    rows[1]["language"] = ""
    events, skipped = build(make_df(rows), config)
    text = " | ".join(skipped[0]["problems"])
    assert events == [] and "sender_id is blank" in text and "language is blank" in text


def test_bot_id_differing_within_session_skips_it(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])
    rows[1]["bot_id"] = "89"
    events, skipped = build(make_df(rows), config)
    assert events == [] and "bot_id differs" in skipped[0]["problems"][0]


def test_missing_role_column_raises(config):
    with pytest.raises(ValueError):
        build(make_df([{}]).drop(columns=["stage"]), config)


# ---------------------------------------------------------------------------------- location tags

def location_config(config):
    config["location"] = {
        "state": {"stage": "STATE_IDENTIFICATION", "aliases": {"bihar": "Bihar", "in the state of bihar": "Bihar"}},
        "district": {"stage": "DISTRICT_IDENTIFICATION", "aliases": {"sitamarhi": "Sitamarhi"}},
    }
    return config


def test_location_tags_from_english_answers_via_aliases(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "[hi] x", "  In the   State of BIHAR "),
        ("B", "DISTRICT_IDENTIFICATION", "q", "q"),
        ("R", "DISTRICT_IDENTIFICATION", "[hi] y", "Sitamarhi"),
    ]))
    events, _ = build(df, location_config(config))
    assert events[0]["tags"]["state"] == "Bihar" and events[0]["tags"]["district"] == "Sitamarhi"


def test_location_none_when_unrecognised_or_unanswered(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "[hi] x", "Atlantis"),
        ("B", "DISTRICT_IDENTIFICATION", "q", "q"),
    ]))
    events, _ = build(df, location_config(config))
    assert events[0]["tags"]["state"] is None and events[0]["tags"]["district"] is None


def test_location_falls_back_to_message_when_no_english_text(config):
    df = make_df(session_rows([("R", "STATE_IDENTIFICATION", "bihar", "")], language="en"))
    events, _ = build(df, location_config(config))
    assert events[0]["tags"]["state"] == "Bihar"


def test_location_ignores_bot_question_rows_with_the_stage(config):
    # the synthesised question carries the stage key but is a question, not an answer
    df = make_df(session_rows([("R", "STATE_IDENTIFICATION", "[hi] x", "Atlantis")]))
    cfg = location_config(config)
    cfg["location"]["state"]["aliases"][cfg["questions"][0]["text"].lower()] = "WRONG"
    events, _ = build(df, cfg)
    assert events[0]["tags"]["state"] is None


def test_location_absent_block_gives_none(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    assert all(e["tags"]["state"] is None and e["tags"]["district"] is None for e in events)


# ---------------------------------------------------------------------------------- serialisation

def test_every_event_round_trips_through_json(config):
    for name in ("feedback_valid_mitra.csv", "feedback_bad_sessions.csv"):
        events, skipped = build(read_fixture(name), location_config(mitra_config_v1("Parent")))
        for payload in events + skipped:
            assert json.loads(json.dumps(payload, allow_nan=False)) == payload


def test_event_values_are_plain_python_types(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    for e in events:
        for m in e["data"]["messages"]:
            assert type(m["sequence"]) is int and type(m["isFollowUp"]) is bool
            assert type(m["message"]) is str
            assert m["parentSequence"] is None or type(m["parentSequence"]) is int


def test_performance_on_a_large_file(config):
    import time
    items = [("R", "STATE_IDENTIFICATION", "a", "b")]
    for n in range(2, 32):
        items.append(("B", f"Q{n}", "q", "q") if n % 2 == 0 else ("R", f"Q{n - 1}", "r", "r"))
    rows = []
    for s in range(3000):
        rows += session_rows(items, session=f"{s:032d}", submission_id=str(1000 + s))
    df = make_df(rows)                      # 93,000 rows, 3,000 sessions
    start = time.time()
    events, skipped = build(df, config)
    assert len(events) == 3000 and skipped == []
    assert time.time() - start < 10
