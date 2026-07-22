import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from app.config import settings

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def configure_logging(service_name: str) -> None:
    """
    Configures the root logger with a stdout stream plus two rotating file
    handlers scoped to service_name (one of the --mode values: web, consumer,
    worker, all) - logs/{service_name}.log for everything at LOG_LEVEL and
    logs/{service_name}.error.log for ERROR+ only. Each is retained for
    LOG_RETENTION_DAYS via daily rotation.
    """
    root = logging.getLogger()
    if root.handlers:
        root.handlers.clear()

    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    root.setLevel(level)

    os.makedirs(settings.LOG_DIR, exist_ok=True)
    formatter = logging.Formatter(_LOG_FORMAT)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)

    app_handler = TimedRotatingFileHandler(
        os.path.join(settings.LOG_DIR, f"{service_name}.log"),
        when="midnight",
        backupCount=settings.LOG_RETENTION_DAYS,
        encoding="utf-8",
    )
    app_handler.setLevel(level)
    app_handler.setFormatter(formatter)

    error_handler = TimedRotatingFileHandler(
        os.path.join(settings.LOG_DIR, f"{service_name}.error.log"),
        when="midnight",
        backupCount=settings.LOG_RETENTION_DAYS,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    root.addHandler(stream_handler)
    root.addHandler(app_handler)
    root.addHandler(error_handler)
