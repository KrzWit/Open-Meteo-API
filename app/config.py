"""Configuration utilities for the ETL application."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class CityConfig:
    """Geographic metadata required to query weather data for a city."""

    name: str
    latitude: float
    longitude: float


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    database_url: str
    open_meteo_base_url: str
    request_timeout_seconds: int
    cities: List[CityConfig]
    log_level: str


def _parse_cities(raw_value: str) -> List[CityConfig]:
    """Parse city definitions from CITY_DEFINITIONS environment variable.

    Expected format:
    "Warsaw:52.2297:21.0122;Berlin:52.5200:13.4050"
    """

    cities: List[CityConfig] = []
    for entry in raw_value.split(";"):
        stripped = entry.strip()
        if not stripped:
            continue

        parts = stripped.split(":")
        if len(parts) != 3:
            raise ValueError(
                "CITY_DEFINITIONS must use format name:latitude:longitude separated by ';'"
            )

        name, latitude, longitude = parts
        cities.append(
            CityConfig(name=name.strip(), latitude=float(latitude), longitude=float(longitude))
        )

    if not cities:
        raise ValueError("CITY_DEFINITIONS cannot be empty")

    return cities


def get_settings() -> Settings:
    """Load and validate application settings from environment variables."""

    city_definitions = os.getenv(
        "CITY_DEFINITIONS", "Warsaw:52.2297:21.0122;Berlin:52.5200:13.4050"
    )
    db_user = os.getenv("POSTGRES_USER", "etl_user")
    db_password = os.getenv("POSTGRES_PASSWORD", "etl_password")
    db_host = os.getenv("POSTGRES_HOST", "db")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "weather_etl")

    database_url = os.getenv(
        "DATABASE_URL",
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
    )

    return Settings(
        database_url=database_url,
        open_meteo_base_url=os.getenv("OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1/forecast"),
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")),
        cities=_parse_cities(city_definitions),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
