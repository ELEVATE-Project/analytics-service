"""
Show, migrate and change the per-bot feedback settings that live in the persona's upload config (config_version),
not in .env: max_csv_upload_bytes, ingestion_schema, process_steps and the optional expect_stages.

    python tools/feedback_config.py show    [--session-type Youth_Voice]
    python tools/feedback_config.py migrate [--dry-run]
    python tools/feedback_config.py set --session-type Youth_Voice --source mitra \
                                        --key max_csv_upload_bytes --value 50000000 [--dry-run]
    python tools/feedback_config.py set --session-type Youth_Voice --source mitra \
                                        --key process_steps --json '[{"name": "feedback_metadata_extraction"}]'
    python tools/feedback_config.py set --session-type Youth_Voice --source mitra \
                                        --key expect_stages --json '["AGE_IDENTIFICATION", "STATE_IDENTIFICATION"]'
    python tools/feedback_config.py set --session-type Youth_Voice --source mitra --key expect_stages --unset

`show` prints the four settings of every active feedback_upload config and lists what is missing or invalid.
`migrate` adds the settings that are missing (from the seed defaults) to every ACTIVE feedback_upload config of an
older database (seeds never overwrite, so those keep the config version they were created with): it writes a NEW
config version (the old one is deactivated and kept as history), never overwrites an existing key and is idempotent.
`set` changes one setting after validating it, again as a new config version. Scalars (max_csv_upload_bytes) take
--value; lists / objects take --json (a JSON text). Both take --dry-run (compute, print, write nothing). Reads
DATABASE_URL like the application does. Prints JSON (settings, counts); never respondent data.
"""
import argparse
import asyncio
import getpass
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import feedback_operations  # noqa: E402
from app.database.db import db  # noqa: E402
from app.services.feedback_constants import (  # noqa: E402
    CONFIG_KEY_EXPECT_STAGES,
    CONFIG_KEY_INGESTION_SCHEMA,
    CONFIG_KEY_MAX_CSV_UPLOAD_BYTES,
    CONFIG_KEY_PROCESS_STEPS,
    FEEDBACK_SOURCES,
)

KEYS = (CONFIG_KEY_MAX_CSV_UPLOAD_BYTES, CONFIG_KEY_INGESTION_SCHEMA, CONFIG_KEY_PROCESS_STEPS, CONFIG_KEY_EXPECT_STAGES)


def parse_set_value(key: str, value_text, json_text):
    """
    The Python value of `set`'s --value / --json for `key`. Exactly one of the two must be given (the caller has
    checked --unset). --value is a plain integer, only for max_csv_upload_bytes; --json is JSON text for any key
    (for max_csv_upload_bytes it must then be a JSON integer). Raises ValueError with a clear message.
    """
    if value_text is not None and json_text is not None:
        raise ValueError("give either --value or --json, not both")
    if value_text is None and json_text is None:
        raise ValueError("give --value (a number) or --json (a JSON text); --unset only works for expect_stages")
    if value_text is not None:
        if key != CONFIG_KEY_MAX_CSV_UPLOAD_BYTES:
            raise ValueError(f"--value is only for {CONFIG_KEY_MAX_CSV_UPLOAD_BYTES}; use --json for {key}")
        try:
            return int(value_text.strip())
        except ValueError:
            raise ValueError(f"--value {value_text!r} is not an integer")
    try:
        return json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"--json is not valid JSON: {exc}")


async def _run(args):
    await db.connect()
    try:
        if args.command == "show":
            return await feedback_operations.list_feedback_config(args.session_type)
        if args.command == "migrate":
            return await feedback_operations.migrate_feedback_config(dry_run=args.dry_run, created_by=getpass.getuser())
        if args.unset and (args.value is not None or args.json is not None):
            raise ValueError("--unset takes neither --value nor --json")
        value = None if args.unset else parse_set_value(args.key, args.value, args.json)
        return await feedback_operations.set_feedback_config_value(
            args.session_type, args.source, args.key, value, tenant_code=args.tenant_code, unset=args.unset,
            created_by=getpass.getuser(), change_note=args.note, dry_run=args.dry_run,
        )
    finally:
        await db.disconnect()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    show = sub.add_parser("show", help="print the per-bot settings of the active feedback upload configs")
    show.add_argument("--session-type", help="only this persona key, for example Youth_Voice")

    migrate = sub.add_parser("migrate", help="add the missing settings (seed defaults) to the active configs")
    migrate.add_argument("--dry-run", action="store_true", help="report what would change, write nothing")

    setting = sub.add_parser("set", help="change one setting (new config version)")
    setting.add_argument("--session-type", required=True, help="persona key, for example Youth_Voice")
    setting.add_argument("--source", required=True, choices=FEEDBACK_SOURCES, help="mitra or google_forms")
    setting.add_argument("--key", required=True, choices=KEYS, help="the setting to change")
    setting.add_argument("--value", help="a number (max_csv_upload_bytes only), for example 50000000")
    setting.add_argument("--json", help="a JSON text (any setting), for example '[{\"name\": \"x\"}]'")
    setting.add_argument("--unset", action="store_true", help="remove the optional expect_stages")
    setting.add_argument("--tenant-code", help="only needed when several configs match the persona")
    setting.add_argument("--note", help="change note stored on the new config version")
    setting.add_argument("--dry-run", action="store_true", help="validate and print, write nothing")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = asyncio.run(_run(args))
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
