"""ETL pipeline orchestration."""

from __future__ import annotations

import logging

from app.config import get_settings
from app.database import create_db_engine, create_session_factory, get_session, initialize_database
from app.extractor import fetch_weather_for_cities
from app.loader import load_raw_payloads, upsert_hourly_weather
from app.transformer import transform_payloads_to_hourly_df


LOGGER = logging.getLogger(__name__)


def run_pipeline() -> None:
    """Execute full extract-transform-load workflow."""

    settings = get_settings()
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)

    LOGGER.info("Initializing database schema")
    initialize_database(engine)

    payloads = fetch_weather_for_cities(cities=settings.cities, settings=settings)
    if not payloads:
        LOGGER.warning("No payloads fetched. Pipeline will exit without loading transformed data.")
        return

    transformed_df = transform_payloads_to_hourly_df(payloads)

    with get_session(session_factory) as session:
        load_raw_payloads(session, payloads)
        upsert_hourly_weather(session, transformed_df)

    LOGGER.info(
        "Pipeline finished successfully. payload_count=%s transformed_rows=%s",
        len(payloads),
        len(transformed_df),
    )
