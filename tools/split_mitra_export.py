#!/usr/bin/env python3
"""Split the Mitra all-persona export CSV into upload-ready per-persona files.

The feedback upload API takes ONE persona (``session_type``) per upload and has
a size cap, so this tool writes one or more CSV files per ``session_type``,
never splitting a session across files and keeping each file <= ``--max-mb``.

Usage:
    python tools/split_mitra_export.py INPUT_CSV --out-dir DIR [--max-mb 80]
                                       [--prefix NAME] [--force]

Streaming, two passes over the input (low memory):
  1. size every session (rows + bytes as they will be written),
  2. route each row to its part file.

The summary (``split_summary.json``, stdout table, logs) contains aggregates
only: never message text, session ids or submission ids.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence

log = logging.getLogger("split_mitra_export")

SESSION_TYPE_COL = "session_type"
SESSION_COL = "session"
SUMMARY_NAME = "split_summary.json"
DEFAULT_MAX_MB = 80


class SplitError(Exception):
    """Validation or usage error; nothing is written when this is raised."""


@dataclass
class _SessionInfo:
    session_type: str
    rows: int = 0
    bytes: int = 0


@dataclass
class _Part:
    session_type: str
    sessions: int = 0
    rows: int = 0
    bytes: int = 0
    name: str = ""


def _encode_row(row: Sequence[str]) -> bytes:
    """Encode one row exactly as it will be written to an output file."""
    buf = io.StringIO(newline="")
    csv.writer(buf, lineterminator="\n").writerow(row)
    return buf.getvalue().encode("utf-8")


def _open_reader(path: Path):
    return open(path, "r", encoding="utf-8", newline="")


def _scan(input_path: Path):
    """Pass 1. Returns (header, sessions dict in first-appearance order, total_rows)."""
    sessions: Dict[str, _SessionInfo] = {}
    total_rows = 0
    with _open_reader(input_path) as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            raise SplitError("input file is empty (no header row)")
        for col in (SESSION_TYPE_COL, SESSION_COL):
            if col not in header:
                raise SplitError(f"required column '{col}' is missing from the header")
        type_idx = header.index(SESSION_TYPE_COL)
        sess_idx = header.index(SESSION_COL)
        ncols = len(header)
        for n, row in enumerate(reader, start=1):
            if len(row) != ncols:
                raise SplitError(
                    f"data row {n}: expected {ncols} fields, got {len(row)}"
                )
            stype = row[type_idx]
            sid = row[sess_idx]
            if not stype.strip():
                raise SplitError(f"data row {n}: blank {SESSION_TYPE_COL}")
            if not sid.strip():
                raise SplitError(f"data row {n}: blank {SESSION_COL}")
            info = sessions.get(sid)
            if info is None:
                info = sessions[sid] = _SessionInfo(session_type=stype)
            elif info.session_type != stype:
                # Deliberately no session id in the message.
                raise SplitError(
                    f"data row {n}: a session has more than one distinct "
                    f"{SESSION_TYPE_COL}"
                )
            info.rows += 1
            info.bytes += len(_encode_row(row))
            total_rows += 1
    return header, sessions, total_rows


def _pack(sessions: Dict[str, _SessionInfo], header_bytes: int, max_bytes: int):
    """Greedy packing per session_type. Returns {type: [_Part]} and the
    session -> (type, part index) routing map."""
    parts: Dict[str, List[_Part]] = {}
    route: Dict[str, tuple] = {}
    oversize = 0
    for sid, info in sessions.items():
        plist = parts.setdefault(info.session_type, [])
        if info.bytes + header_bytes > max_bytes:
            oversize += 1
        cur = plist[-1] if plist else None
        if cur is None or (cur.bytes + info.bytes > max_bytes):
            cur = _Part(session_type=info.session_type, bytes=header_bytes)
            plist.append(cur)
        cur.sessions += 1
        cur.rows += info.rows
        cur.bytes += info.bytes
        route[sid] = (info.session_type, len(plist) - 1)
    return parts, route, oversize


def _file_name(prefix: str, stype: str, idx: int, nparts: int) -> str:
    if nparts == 1:
        return f"{prefix}{stype}.csv"
    return f"{prefix}{stype}_part{idx + 1}.csv"


def split_export(
    input_path,
    out_dir,
    max_bytes: int,
    prefix: str = "",
    force: bool = False,
) -> dict:
    """Split ``input_path`` into per-persona CSVs under ``out_dir``.

    Returns the summary dict (aggregates only). Raises SplitError on validation
    problems or when an output exists and ``force`` is False; in those cases
    nothing is written.
    """
    csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
    input_path = Path(input_path)
    out_dir = Path(out_dir)
    if max_bytes <= 0:
        raise SplitError("max size must be positive")

    header, sessions, total_rows = _scan(input_path)
    header_bytes = len(_encode_row(header))
    parts, route, oversize = _pack(sessions, header_bytes, max_bytes)
    if oversize:
        log.warning(
            "%d session(s) exceed the size limit on their own and are kept whole "
            "in their own part",
            oversize,
        )

    # Name parts and check for collisions before writing anything.
    named: List[_Part] = []
    for stype, plist in parts.items():
        for i, part in enumerate(plist):
            part.name = _file_name(prefix, stype, i, len(plist))
            named.append(part)
    if not force:
        existing = [p.name for p in named if (out_dir / p.name).exists()]
        if (out_dir / SUMMARY_NAME).exists():
            existing.append(SUMMARY_NAME)
        if existing:
            raise SplitError(
                "refusing to overwrite existing file(s) in out dir: "
                + ", ".join(sorted(existing))
                + " (use --force)"
            )

    out_dir.mkdir(parents=True, exist_ok=True)

    # Pass 2: route rows. Write to .tmp files, then rename on success.
    handles = {}
    writers = {}
    tmp_paths: Dict[tuple, Path] = {}
    try:
        for stype, plist in parts.items():
            for i, part in enumerate(plist):
                tmp = out_dir / (part.name + ".tmp")
                fh = open(tmp, "w", encoding="utf-8", newline="")
                handles[(stype, i)] = fh
                tmp_paths[(stype, i)] = tmp
                w = csv.writer(fh, lineterminator="\n")
                w.writerow(header)
                writers[(stype, i)] = w
        sess_idx = header.index(SESSION_COL)
        with _open_reader(input_path) as fh:
            reader = csv.reader(fh)
            next(reader)
            for row in reader:
                writers[route[row[sess_idx]]].writerow(row)
    except BaseException:
        for fh in handles.values():
            fh.close()
        for tmp in tmp_paths.values():
            tmp.unlink(missing_ok=True)
        raise
    for fh in handles.values():
        fh.close()
    for (stype, i), tmp in tmp_paths.items():
        tmp.replace(out_dir / parts[stype][i].name)

    summary = {
        "input_rows": total_rows,
        "input_sessions": len(sessions),
        "max_bytes": max_bytes,
        "files": [
            {
                "name": p.name,
                "session_type": p.session_type,
                "sessions": p.sessions,
                "rows": p.rows,
                "bytes": (out_dir / p.name).stat().st_size,
            }
            for p in named
        ],
    }
    summary["totals"] = {
        "files": len(named),
        "sessions": sum(f["sessions"] for f in summary["files"]),
        "rows": sum(f["rows"] for f in summary["files"]),
        "bytes": sum(f["bytes"] for f in summary["files"]),
    }
    (out_dir / SUMMARY_NAME).write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    return summary


def _print_table(summary: dict) -> None:
    files = summary["files"]
    width = max([len("file")] + [len(f["name"]) for f in files])
    print(f"{'file':<{width}}  {'session_type':<20} {'sessions':>8} {'rows':>8} {'MB':>8}")
    for f in files:
        print(
            f"{f['name']:<{width}}  {f['session_type']:<20} {f['sessions']:>8} "
            f"{f['rows']:>8} {f['bytes'] / 1_000_000:>8.2f}"
        )
    t = summary["totals"]
    print(
        f"{'TOTAL':<{width}}  {'':<20} {t['sessions']:>8} {t['rows']:>8} "
        f"{t['bytes'] / 1_000_000:>8.2f}"
    )
    print(
        f"input: {summary['input_sessions']} sessions, {summary['input_rows']} rows"
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Split the Mitra all-persona export into per-persona upload CSVs."
    )
    ap.add_argument("input_csv")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--max-mb", type=float, default=DEFAULT_MAX_MB,
                    help="max size per output file in MB (1 MB = 1,000,000 bytes; default 80)")
    ap.add_argument("--prefix", default="")
    ap.add_argument("--force", action="store_true",
                    help="overwrite existing output files")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        summary = split_export(
            args.input_csv,
            args.out_dir,
            int(args.max_mb * 1_000_000),
            prefix=args.prefix,
            force=args.force,
        )
    except SplitError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    _print_table(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
