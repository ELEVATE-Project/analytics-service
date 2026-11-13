"""Tests for tools/split_mitra_export.py. Synthetic, invented data only."""
import csv
import json
import logging
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import split_mitra_export as sme  # noqa: E402

HEADER = [
    "bot_id", "bot_name", "session_type", "session_status", "session",
    "submission_id", "language", "sequence", "conversation_row_id", "message",
    "translated_message", "created_at", "updated_at", "stage", "receiver_id",
    "sender_id", "audio_file", "file_url", "other_params",
]


def make_row(stype, sess, seq, message="hello", blank=False):
    row = {c: "" for c in HEADER}
    row.update(
        bot_id="b1", bot_name="TestBot", session_type=stype, session_status="done",
        session=sess, submission_id="sub-" + sess, language="en", sequence=str(seq),
        conversation_row_id=f"{sess}-{seq}", message=message,
    )
    if not blank:
        row.update(created_at="2025-01-01 10:00:00", stage="s1", sender_id="u1")
    return [row[c] for c in HEADER]


def write_csv(path, rows, header=HEADER):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    return path


def read_csv(path):
    with open(path, "r", encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh))
    return rows[0], rows[1:]


def basic_rows():
    rows = []
    for stype, sessions in (("Youth_Voice", ["y1", "y2"]), ("Parent_Voice", ["p1"])):
        for s in sessions:
            rows.append(make_row(stype, s, 1, message="", blank=True))
            rows.append(make_row(stype, s, 2, message="first reply"))
            rows.append(make_row(stype, s, 3, message="second reply"))
    return rows


BIG = 10**9


def test_one_file_per_session_type(tmp_path):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    out = tmp_path / "out"
    summary = sme.split_export(src, out, BIG)
    names = sorted(p.name for p in out.iterdir())
    assert names == ["Parent_Voice.csv", "Youth_Voice.csv", "split_summary.json"]
    _, youth = read_csv(out / "Youth_Voice.csv")
    _, parent = read_csv(out / "Parent_Voice.csv")
    assert len(youth) == 6 and len(parent) == 3
    assert {r[2] for r in youth} == {"Youth_Voice"}
    assert summary["input_rows"] == 9 and summary["input_sessions"] == 3


def test_prefix(tmp_path):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    out = tmp_path / "out"
    sme.split_export(src, out, BIG, prefix="mitra_")
    assert (out / "mitra_Youth_Voice.csv").exists()
    assert (out / "mitra_Parent_Voice.csv").exists()


def test_split_into_parts_without_spreading_sessions(tmp_path):
    rows = []
    for i in range(6):
        sid = f"s{i}"
        rows += [make_row("Youth_Voice", sid, n, message=f"m{n}") for n in (1, 2, 3)]
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    one_session = sum(len(sme._encode_row(r)) for r in rows[:3])
    header_bytes = len(sme._encode_row(HEADER))
    limit = header_bytes + 2 * one_session + 5  # two sessions per part
    summary = sme.split_export(src, out, limit)
    files = sorted(p.name for p in out.glob("Youth_Voice_part*.csv"))
    assert files == ["Youth_Voice_part1.csv", "Youth_Voice_part2.csv", "Youth_Voice_part3.csv"]
    assert not (out / "Youth_Voice.csv").exists()
    seen = {}
    for f in files:
        _, data = read_csv(out / f)
        assert (out / f).stat().st_size <= limit
        for r in data:
            seen.setdefault(r[4], set()).add(f)
    assert all(len(v) == 1 for v in seen.values())
    assert len(seen) == 6
    assert [f["sessions"] for f in summary["files"]] == [2, 2, 2]


def test_non_contiguous_sessions_stay_together_in_file_order(tmp_path):
    rows = [
        make_row("Youth_Voice", "A", 1, message="a1"),
        make_row("Youth_Voice", "B", 1, message="b1"),
        make_row("Youth_Voice", "A", 2, message="a2"),
        make_row("Youth_Voice", "C", 1, message="c1"),
        make_row("Youth_Voice", "B", 2, message="b2"),
    ]
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    # Part fits A+B (4 rows) but not C as well.
    sizes = [len(sme._encode_row(r)) for r in rows]
    header_bytes = len(sme._encode_row(HEADER))
    limit = header_bytes + sizes[0] + sizes[2] + sizes[1] + sizes[4] + 1
    sme.split_export(src, out, limit)
    _, p1 = read_csv(out / "Youth_Voice_part1.csv")
    _, p2 = read_csv(out / "Youth_Voice_part2.csv")
    assert [r[9] for r in p1] == ["a1", "b1", "a2", "b2"]
    assert [r[9] for r in p2] == ["c1"]


def test_header_and_cells_byte_identical(tmp_path):
    tricky = 'line one,\nline "two" with, comma\r\nend'
    rows = [
        make_row("Parent_Voice", "p1", 1, message="", blank=True),
        make_row("Parent_Voice", "p1", 2, message=tricky),
        make_row("Parent_Voice", "p1", 3, message="  padded  "),
    ]
    rows[2][10] = "  Unicode नमस्ते  "
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    sme.split_export(src, out, BIG)
    header, data = read_csv(out / "Parent_Voice.csv")
    assert header == HEADER
    assert data == rows
    assert data[0][7] == "1" and data[0][15] == "" and data[0][13] == "" and data[0][11] == ""
    assert data[1][9] == tricky
    # whole file identical to the input when nothing is split
    assert (out / "Parent_Voice.csv").read_bytes() == src.read_bytes()


def test_oversize_session_gets_own_part_and_warns(tmp_path, caplog):
    rows = []
    rows += [make_row("Educator_Voice", "small1", n) for n in (1, 2)]
    rows += [make_row("Educator_Voice", "huge", n, message="x" * 200) for n in range(1, 6)]
    rows += [make_row("Educator_Voice", "small2", n) for n in (1, 2)]
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    small = sum(len(sme._encode_row(r)) for r in rows[:2])
    limit = len(sme._encode_row(HEADER)) + small + 10
    with caplog.at_level(logging.WARNING, logger="split_mitra_export"):
        summary = sme.split_export(src, out, limit)
    assert any("exceed" in r.getMessage() for r in caplog.records)
    assert not any("huge" in r.getMessage() for r in caplog.records)
    by_sessions = [(f["name"], f["sessions"], f["rows"]) for f in summary["files"]]
    assert by_sessions == [
        ("Educator_Voice_part1.csv", 1, 2),
        ("Educator_Voice_part2.csv", 1, 5),
        ("Educator_Voice_part3.csv", 1, 2),
    ]
    _, mid = read_csv(out / "Educator_Voice_part2.csv")
    assert {r[4] for r in mid} == {"huge"}


def _assert_nothing_written(out):
    assert not out.exists() or list(out.iterdir()) == []


def test_error_missing_columns(tmp_path):
    out = tmp_path / "out"
    for drop in ("session_type", "session"):
        header = [c for c in HEADER if c != drop]
        src = write_csv(tmp_path / f"no_{drop}.csv", [], header=header)
        with pytest.raises(sme.SplitError, match=drop):
            sme.split_export(src, out, BIG)
    _assert_nothing_written(out)


@pytest.mark.parametrize("col_idx,label", [(2, "session_type"), (4, "session")])
def test_error_blank_values(tmp_path, col_idx, label):
    rows = basic_rows()
    rows[4][col_idx] = ""
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    with pytest.raises(sme.SplitError, match=f"data row 5.*blank {label}"):
        sme.split_export(src, out, BIG)
    _assert_nothing_written(out)


def test_error_mixed_session_type_in_one_session(tmp_path):
    rows = [
        make_row("Youth_Voice", "m1", 1),
        make_row("Parent_Voice", "m1", 2),
    ]
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    with pytest.raises(sme.SplitError, match="more than one distinct session_type") as ei:
        sme.split_export(src, out, BIG)
    assert "m1" not in str(ei.value)
    _assert_nothing_written(out)


def test_error_wrong_field_count_names_row(tmp_path):
    rows = basic_rows()
    rows[2] = rows[2][:-1]
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    with pytest.raises(sme.SplitError, match=r"data row 3: expected 19 fields, got 18"):
        sme.split_export(src, out, BIG)
    _assert_nothing_written(out)


def test_summary_has_only_aggregates(tmp_path):
    rows = basic_rows()
    rows.append(make_row("Youth_Voice", "y3", 1, message="secret message text"))
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    sme.split_export(src, out, BIG)
    text = (out / "split_summary.json").read_text(encoding="utf-8")
    data = json.loads(text)
    for forbidden in ("secret message text", "y1", "y2", "y3", "p1", "sub-", "first reply"):
        assert forbidden not in text
    assert set(data) == {"input_rows", "input_sessions", "max_bytes", "files", "totals"}
    assert data["input_rows"] == 10 and data["input_sessions"] == 4
    assert data["totals"]["sessions"] == 4 and data["totals"]["rows"] == 10
    for f in data["files"]:
        assert set(f) == {"name", "session_type", "sessions", "rows", "bytes"}
        assert f["bytes"] == (out / f["name"]).stat().st_size
    assert data["totals"]["bytes"] == sum(f["bytes"] for f in data["files"])


def test_refuses_overwrite_then_force(tmp_path):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    out = tmp_path / "out"
    sme.split_export(src, out, BIG)
    sentinel = out / "Youth_Voice.csv"
    before = sentinel.read_bytes()
    sentinel.write_text("changed")
    with pytest.raises(sme.SplitError, match="refusing to overwrite"):
        sme.split_export(src, out, BIG)
    assert sentinel.read_text() == "changed"
    sme.split_export(src, out, BIG, force=True)
    assert sentinel.read_bytes() == before
    assert not list(out.glob("*.tmp"))


def test_main_exit_codes(tmp_path, capsys):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    out = tmp_path / "out"
    assert sme.main([str(src), "--out-dir", str(out), "--max-mb", "80"]) == 0
    table = capsys.readouterr().out
    assert "Youth_Voice.csv" in table and "TOTAL" in table
    assert "first reply" not in table and "y1" not in table
    # second run refuses
    assert sme.main([str(src), "--out-dir", str(out)]) == 1
    assert "refusing to overwrite" in capsys.readouterr().err
    assert sme.main([str(src), "--out-dir", str(out), "--force"]) == 0
    # validation error
    bad = write_csv(tmp_path / "bad.csv", [make_row("", "x", 1)])
    assert sme.main([str(bad), "--out-dir", str(tmp_path / "out2")]) == 1
    assert not (tmp_path / "out2").exists()
    # missing input
    assert sme.main([str(tmp_path / "nope.csv"), "--out-dir", str(tmp_path / "out3")]) == 1


def test_cli_subprocess(tmp_path):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    out = tmp_path / "nested" / "out"
    script = ROOT / "tools" / "split_mitra_export.py"
    ok = subprocess.run(
        [sys.executable, str(script), str(src), "--out-dir", str(out), "--max-mb", "1"],
        capture_output=True, text=True,
    )
    assert ok.returncode == 0, ok.stderr
    assert (out / "Youth_Voice.csv").exists()
    again = subprocess.run(
        [sys.executable, str(script), str(src), "--out-dir", str(out)],
        capture_output=True, text=True,
    )
    assert again.returncode != 0
    assert "--force" in again.stderr
