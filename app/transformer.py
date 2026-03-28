"""Transformation logic for converting API payloads into tabular data."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd


EXPECTED_COLUMNS = [
    "city",
    "latitude",
    "longitude",
    "timestamp",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
]


def transform_payload_to_hourly_df(payload: Dict) -> pd.DataFrame:
    """Convert a single Open-Meteo payload into a normalized DataFrame."""

    hourly = payload.get("hourly", {})
    time_values = hourly.get("time", [])

    if not time_values:
        return pd.DataFrame(columns=EXPECTED_COLUMNS)

    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(time_values, utc=True, errors="coerce"),
            "temperature_2m": hourly.get("temperature_2m", []),
            "relative_humidity_2m": hourly.get("relative_humidity_2m", []),
            "precipitation": hourly.get("precipitation", []),
            "wind_speed_10m": hourly.get("wind_speed_10m", []),
        }
    )

    frame["city"] = payload.get("city")
    frame["latitude"] = payload.get("requested_latitude", payload.get("latitude"))
    frame["longitude"] = payload.get("requested_longitude", payload.get("longitude"))

    frame = frame[[
        "city",
        "latitude",
        "longitude",
        "timestamp",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
    ]]

    numeric_columns = [
        "latitude",
        "longitude",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.dropna(subset=["city", "timestamp"]).reset_index(drop=True)
    return frame


def transform_payloads_to_hourly_df(payloads: List[Dict]) -> pd.DataFrame:
    """Transform multiple payloads and return a single normalized DataFrame."""

    if not payloads:
        return pd.DataFrame(columns=EXPECTED_COLUMNS)

    frames = [transform_payload_to_hourly_df(payload) for payload in payloads]
    valid_frames = [frame for frame in frames if not frame.empty]
    if not valid_frames:
        return pd.DataFrame(columns=EXPECTED_COLUMNS)

    return pd.concat(valid_frames, ignore_index=True)
