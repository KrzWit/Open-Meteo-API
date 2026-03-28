"""Database load logic for raw and transformed weather datasets."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import RawWeatherData, WeatherHourly


LOGGER = logging.getLogger(__name__)


def load_raw_payloads(session: Session, payloads: List[Dict]) -> None:
    """Insert raw API responses into raw_weather_data table."""

    if not payloads:
        LOGGER.warning("No payloads to load into raw_weather_data")
        return

    records = []
    for payload in payloads:
        records.append(
            RawWeatherData(
                city=payload.get("city"),
                latitude=payload.get("requested_latitude", payload.get("latitude")),
                longitude=payload.get("requested_longitude", payload.get("longitude")),
                raw_json=payload,
            )
        )

    session.add_all(records)
    LOGGER.info("Inserted %s raw payload records", len(records))


def upsert_hourly_weather(session: Session, weather_df: pd.DataFrame) -> int:
    """Upsert transformed hourly records into weather_hourly table.

    Returns:
        Number of rows attempted in upsert.
    """

    if weather_df.empty:
        LOGGER.warning("No transformed rows to upsert into weather_hourly")
        return 0

    records = weather_df.to_dict(orient="records")
    load_time = datetime.now(timezone.utc)
    for record in records:
        record["load_timestamp"] = load_time

    stmt = insert(WeatherHourly).values(records)
    update_columns = {
        "latitude": stmt.excluded.latitude,
        "longitude": stmt.excluded.longitude,
        "temperature_2m": stmt.excluded.temperature_2m,
        "relative_humidity_2m": stmt.excluded.relative_humidity_2m,
        "precipitation": stmt.excluded.precipitation,
        "wind_speed_10m": stmt.excluded.wind_speed_10m,
        "load_timestamp": stmt.excluded.load_timestamp,
    }

    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=["city", "timestamp"],
        set_=update_columns,
    )
    session.execute(upsert_stmt)

    LOGGER.info("Upserted %s rows into weather_hourly", len(records))
    return len(records)
