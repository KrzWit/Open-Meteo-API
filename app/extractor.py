"""Data extraction logic for Open-Meteo API."""

from __future__ import annotations

import logging
from typing import Dict, List

import requests

from app.config import CityConfig, Settings


LOGGER = logging.getLogger(__name__)


def fetch_city_weather(city: CityConfig, settings: Settings) -> Dict:
    """Fetch hourly weather data from Open-Meteo for a single city."""

    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": "UTC",
    }

    LOGGER.info("Fetching weather for city=%s lat=%s lon=%s", city.name, city.latitude, city.longitude)

    response = requests.get(
        settings.open_meteo_base_url,
        params=params,
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()

    payload = response.json()
    payload["city"] = city.name
    payload["requested_latitude"] = city.latitude
    payload["requested_longitude"] = city.longitude
    return payload


def fetch_weather_for_cities(cities: List[CityConfig], settings: Settings) -> List[Dict]:
    """Fetch weather payloads for all configured cities."""

    payloads: List[Dict] = []
    for city in cities:
        try:
            payloads.append(fetch_city_weather(city=city, settings=settings))
        except requests.RequestException:
            LOGGER.exception("Failed to fetch weather data for city=%s", city.name)
    return payloads
