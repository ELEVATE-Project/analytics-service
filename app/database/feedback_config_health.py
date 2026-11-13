"""
Startup health check of the feedback upload configs (run once per process from Database.initialize_schema, so the
web, consumer and worker startup all log it).

A database that predates the per-bot settings (max_csv_upload_bytes, ingestion_schema, process_steps) keeps the
config version it was seeded with, and looks healthy until the first event arrives and is dead-lettered. This check
makes that visible at startup: one ERROR per ACTIVE feedback_upload config that lacks / has an invalid required
setting (same validators as `python tools/feedback_config.py show`), and one WARNING when one of the removed
environment variables is still set (they are ignored now). Only names, session types and sources are logged, never
values or config content. The check never raises: a failure to run it is a warning, not a startup failure.
"""
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from dotenv import dotenv_values

from app.services.feedback_config_validation import config_setting_problems
from app.services.feedback_constants import CONFIG_TYPE_FEEDBACK_UPLOAD

logger = logging.getLogger("analytics_service.db")

REPO_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"

# Settings that moved into each persona's feedback_upload config (see app/services/feedback_constants.py).
REMOVED_ENV_VARS = ("FEEDBACK_MAX_CSV_UPLOAD_BYTES", "FEEDBACK_KAFKA_SCHEMA", "PROCESS_CONFIG_FEEDBACK")


def removed_env_vars_in_use(
    environ: Optional[Mapping[str, str]] = None, env_file: Optional[Path] = None
) -> List[str]:
    """Names (never values) of the removed variables that are set in the process environment or in the .env file."""
    environ = os.environ if environ is None else environ
    env_file = REPO_ENV_FILE if env_file is None else env_file
    found = [name for name in REMOVED_ENV_VARS if name in environ]
    if env_file.is_file():
        in_file = dotenv_values(env_file)
        found.extend(name for name in REMOVED_ENV_VARS if name in in_file and name not in found)
    return found


def warn_removed_env_vars(environ: Optional[Mapping[str, str]] = None, env_file: Optional[Path] = None) -> List[str]:
    """Logs one WARNING naming the removed variables still set; returns the names."""
    names = removed_env_vars_in_use(environ, env_file)
    if len(names) > 0:
        logger.warning(
            "Ignoring removed environment variable(s) %s: these settings now live in each persona's feedback_upload "
            "config (see `python tools/feedback_config.py show`); remove them from the environment / .env.",
            ", ".join(names),
        )
    return names


async def check_feedback_configs(conn) -> List[Dict[str, Any]]:
    """
    Logs one ERROR per ACTIVE feedback_upload config (any source) with a missing / invalid required setting and
    returns [{"session_type", "source", "tenant_code", "problems": {key: why}}] for those configs.
    """
    rows = await conn.fetch(
        """
        SELECT c.session_type, c.config_source, c.tenant_code, v.config
        FROM config c
        JOIN config_version v ON v.config_id = c.id AND v.is_active
        WHERE c.config_type = $1
        ORDER BY c.session_type, c.config_source, c.tenant_code
        """,
        CONFIG_TYPE_FEEDBACK_UPLOAD,
    )
    invalid = []
    for row in rows:
        config = row["config"]
        if isinstance(config, str):
            config = json.loads(config)
        if isinstance(config, dict):
            problems = config_setting_problems(config)
        else:
            problems = {"config": f"must be a JSON object, got {type(config).__name__}"}
        if len(problems) == 0:
            continue
        entry = {
            "session_type": row["session_type"], "source": row["config_source"],
            "tenant_code": row["tenant_code"], "problems": problems,
        }
        invalid.append(entry)
        logger.error(
            "Feedback upload config of session_type %r (%s, tenant %r) is invalid: %s. Feedback events for this "
            "persona will be dead-lettered and its uploads rejected until it is fixed: run "
            "`python tools/feedback_config.py migrate` (adds missing keys) or `show` (details).",
            entry["session_type"], entry["source"], entry["tenant_code"],
            "; ".join(f"{key}: {why}" for key, why in problems.items()),
        )
    return invalid


async def run_startup_checks(conn) -> None:
    """The whole startup check; never raises."""
    try:
        warn_removed_env_vars()
        await check_feedback_configs(conn)
    except Exception as exc:  # a health check must not stop the service from starting
        logger.warning("Feedback config startup check could not run: %s", exc)
