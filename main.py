"""Entry point for running the ETL pipeline."""

from __future__ import annotations

import logging

from app.config import get_settings
from app.pipeline import run_pipeline


def configure_logging() -> None:
    """Configure root logger for console output."""

    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


if __name__ == "__main__":
    configure_logging()
    run_pipeline()
