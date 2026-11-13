"""
Tests of app/services/feedback_version_detect.py (pure, no database): proposing a new questionnaire version and
the instant it started from the main questions the stored sessions were asked. Synthetic questions only.
"""
from datetime import datetime, timedelta, timezone

import pytest

from app.services.feedback_version_detect import SessionQuestions, detect, next_key, normalise

T0 = datetime(2026, 10, 1, 6, 0, tzinfo=timezone.utc)

V1_QS = ["What is your age?", "Which state do you live in?", "What do you like about school?", "Any wish for the future?"]
V2_QS = ["What is your age?", "How do you feel about school these days?", "Which job do you want?", "Any wish for the future?",
         "Which state do you live in? (asked last now)"]


def make(n, qs, start_index=0, stamped="V1", step=timedelta(minutes=10)):
    return [SessionQuestions(f"s{start_index + i}", T0 + step * (start_index + i), stamped, tuple(qs)) for i in range(n)]


def test_normalise_ignores_case_spacing_and_punctuation_but_not_wording():
    assert normalise("  What is  your AGE?? ") == normalise("what is your age")
    assert normalise("income per year") != normalise("income per month")
    assert normalise("???") == ""


@pytest.mark.parametrize("keys,expected", [(["V1"], "V2"), (["V1", "V2"], "V3"), (["V9"], "V10"), (["base"], "V2"), (["a", "b"], "V3")])
def test_next_key(keys, expected):
    assert next_key(keys) == expected


def test_a_clean_change_is_proposed_with_the_first_session_of_the_new_questions():
    sessions = make(120, V1_QS) + make(80, V2_QS, start_index=120)
    result = detect(sessions, ["V1"])
    proposal = result["proposal"]
    assert proposal["key"] == "V2"
    assert proposal["effective_from"] == sessions[120].start.isoformat()
    assert proposal["sessions_before"] == 120 and proposal["sessions_from_start"] == 80
    assert proposal["new_question_sessions_before"] == 0 and proposal["old_question_sessions_from_start"] == 0
    assert proposal["purity"] == 1.0 and proposal["share_after"] == 1.0
    new = {q["text"] for q in result["new_questions"]}
    assert "How do you feel about school these days?" in new and "Which job do you want?" in new
    assert "What is your age?" not in new
    assert "What do you like about school?" in result["dropped_questions"]


def test_no_change_proposes_nothing():
    result = detect(make(200, V1_QS), ["V1"])
    assert result["proposal"] is None and result["new_questions"] == []
    assert "no new question found" in result["reason"]


def test_a_recorded_version_is_not_proposed_again():
    sessions = make(120, V1_QS) + make(80, V2_QS, start_index=120, stamped="V2")
    result = detect(sessions, ["V1", "V2"])
    assert result["proposal"] is None
    assert result["versions"]["V1"]["sessions"] == 120 and result["versions"]["V2"]["sessions"] == 80
    assert result["versions"]["V2"]["reference_questions"] == len(set(map(normalise, V2_QS)))


def test_the_next_change_after_a_recorded_one_is_v3():
    v3 = ["What is your age?", "Tell us about your village.", "Which job do you want?"]
    sessions = make(120, V1_QS) + make(150, V2_QS, 120, "V2") + make(60, v3, 270, "V2")
    result = detect(sessions, ["V1", "V2"])
    assert result["proposal"]["key"] == "V3"
    assert result["proposal"]["effective_from"] == sessions[270].start.isoformat()


def test_a_stray_early_session_does_not_move_the_start():
    sessions = make(120, V1_QS) + make(80, V2_QS, start_index=120)
    sessions[10] = SessionQuestions("stray", sessions[10].start, "V1", tuple(V2_QS))   # a tester saw the new text early
    result = detect(sessions, ["V1"])
    assert result["proposal"]["effective_from"] == sessions[120].start.isoformat()
    assert result["proposal"]["new_question_sessions_before"] == 1


def test_short_sessions_after_the_change_do_not_hide_it():
    after = make(80, V2_QS, start_index=120)
    after = [SessionQuestions(s.session_id, s.start, s.stamped, s.texts[:1]) if i % 5 == 0 else s for i, s in enumerate(after)]
    result = detect(make(120, V1_QS) + after, ["V1"])
    assert result["proposal"]["effective_from"] == after[1].start.isoformat() or \
        result["proposal"]["effective_from"] == after[0].start.isoformat()
    assert result["proposal"]["old_question_sessions_from_start"] > 0


def test_versions_running_side_by_side_are_reported_not_proposed():
    mixed = make(150, V1_QS)
    for i in range(150, 350):
        mixed += make(1, V1_QS if i % 2 == 0 else V2_QS, start_index=i)
    result = detect(mixed, ["V1"])
    assert result["proposal"] is None
    assert "side by side" in result["reason"] and result["details"]["share_after"] < 1
    assert result["new_questions_total"] > 0


def test_a_rare_conditional_question_is_not_a_new_version():
    sessions = make(300, V1_QS)
    for i in range(0, 300, 25):                 # 12 sessions: fewer than min_sessions
        sessions[i] = SessionQuestions(sessions[i].session_id, sessions[i].start, "V1", tuple(V1_QS) + ("Follow-up for some?",))
    assert detect(sessions, ["V1"])["proposal"] is None


def test_a_scattered_conditional_question_is_not_a_new_version():
    sessions = make(400, V1_QS)
    for i in range(0, 400, 10):                 # 40 sessions but only 10% of the sessions since it first appeared
        sessions[i] = SessionQuestions(sessions[i].session_id, sessions[i].start, "V1", tuple(V1_QS) + ("Branch question?",))
    assert detect(sessions, ["V1"])["proposal"] is None


def test_cosmetic_formatting_is_not_a_change_but_a_reworded_question_is():
    plain = make(120, V1_QS)
    cosmetic = make(80, [q.upper() + "  " for q in V1_QS], start_index=120)
    assert detect(plain + cosmetic, ["V1"])["proposal"] is None
    one_word = make(80, [V1_QS[0], "Which state do you live in these days?"] + V1_QS[2:], start_index=120)
    proposal = detect(plain + one_word, ["V1"])["proposal"]
    assert proposal is not None and proposal["effective_from"] == one_word[0].start.isoformat()


def test_sessions_stamped_with_no_recorded_version_cannot_be_compared():
    sessions = [SessionQuestions(s.session_id, s.start, "X", s.texts) for s in make(100, V1_QS)]
    result = detect(sessions, ["V1"])
    assert result["proposal"] is None and "very first session" in result["reason"]


def test_a_small_deployment_still_works_with_lower_thresholds():
    sessions = make(6, V1_QS) + make(5, V2_QS, 6)
    assert detect(sessions, ["V1"])["proposal"] is None                          # below min_sessions=20
    assert detect(sessions, ["V1"], min_sessions=3, baseline_sessions=6)["proposal"]["effective_from"] == sessions[6].start.isoformat()


def test_input_order_does_not_matter_and_empty_input_is_fine():
    sessions = make(120, V1_QS) + make(80, V2_QS, start_index=120)
    assert detect(list(reversed(sessions)), ["V1"]) == detect(sessions, ["V1"])
    assert detect([], ["V1"])["proposal"] is None


@pytest.mark.parametrize("kwargs", [{"min_sessions": 0}, {"baseline_sessions": 0}, {"min_coverage": 0}, {"min_coverage": 1.5}])
def test_bad_thresholds_are_refused(kwargs):
    with pytest.raises(ValueError):
        detect(make(5, V1_QS), ["V1"], **kwargs)


def test_no_version_key_is_refused():
    with pytest.raises(ValueError):
        detect(make(5, V1_QS), [])


def _with_bots(sessions, bot_for):
    return [SessionQuestions(x.session_id, x.start, x.stamped, x.texts, bot_for(i)) for i, x in enumerate(sessions)]


def test_a_bot_id_that_changes_with_the_questions_corroborates_the_proposal():
    sessions = _with_bots(make(120, V1_QS) + make(80, V2_QS, 120), lambda i: "88" if i < 120 else "101")
    result = detect(sessions, ["V1"])
    proposal = result["proposal"]
    assert proposal["bot_ids_before"] == ["88"] and proposal["bot_ids_from_start"] == ["101"]
    assert proposal["bot_id_changed_at_start"] is True
    assert result["versions"]["V1"]["bot_ids"] == {"88": 120, "101": 80}      # nothing is stamped V2 yet


def test_the_same_bot_edited_in_place_is_still_detected_and_says_so():
    sessions = _with_bots(make(120, V1_QS) + make(80, V2_QS, 120), lambda i: "88")
    proposal = detect(sessions, ["V1"])["proposal"]
    assert proposal is not None and proposal["bot_id_changed_at_start"] is False
    assert proposal["bot_ids_before"] == proposal["bot_ids_from_start"] == ["88"]


def test_sessions_without_a_bot_id_are_fine_and_never_decide_anything():
    proposal = detect(make(120, V1_QS) + make(80, V2_QS, 120), ["V1"])["proposal"]
    assert proposal["bot_ids_before"] == [] and proposal["bot_id_changed_at_start"] is False
    # a changed bot id with unchanged questions proposes nothing
    only_bot = _with_bots(make(200, V1_QS), lambda i: "88" if i < 100 else "101")
    assert detect(only_bot, ["V1"])["proposal"] is None


CONFIDENCE_A = "Thank you for sharing! Would you say you are feeling very confident, somewhat confident, or unsure about your future?"
CONFIDENCE_B = "Thank you for sharing! Would you say you're feeling very confident, somewhat confident, or unsure about your future?"


def test_a_wording_variant_of_one_known_question_is_listed_but_not_proposed():
    base = V2_QS + [CONFIDENCE_A]
    late = [q if q != CONFIDENCE_A else CONFIDENCE_B for q in base]
    sessions = make(150, base, stamped="V2") + make(40, late, 150, "V2")
    result = detect(sessions, ["V1", "V2"])
    assert result["proposal"] is None
    assert "wording variants" in result["reason"]
    variant = result["wording_variants"][0]
    assert variant["similar_to"] == CONFIDENCE_A and variant["nearest_known_similarity"] >= 0.9
    assert result["new_questions_total"] == 1


def test_include_wording_proposes_it_anyway():
    base = V2_QS + [CONFIDENCE_A]
    late = [q if q != CONFIDENCE_A else CONFIDENCE_B for q in base]
    sessions = make(150, base, stamped="V2") + make(40, late, 150, "V2")
    proposal = detect(sessions, ["V1", "V2"], include_wording=True)["proposal"]
    assert proposal is not None and proposal["effective_from"] == sessions[150].start.isoformat()


def test_a_real_new_question_next_to_a_wording_variant_is_still_proposed():
    base = V2_QS + [CONFIDENCE_A]
    late = [q if q != CONFIDENCE_A else CONFIDENCE_B for q in base] + ["Which hobby do you enjoy the most these days?"]
    sessions = make(150, base, stamped="V2") + make(40, late, 150, "V2")
    result = detect(sessions, ["V1", "V2"])
    assert result["proposal"] is not None
    assert any("similar_to" in q for q in result["new_questions"]) and any("similar_to" not in q for q in result["new_questions"])
