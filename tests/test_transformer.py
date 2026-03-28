"""Unit tests for transformation logic."""

from __future__ import annotations

from app.transformer import transform_payload_to_hourly_df, transform_payloads_to_hourly_df


def test_transform_payload_to_hourly_df_maps_columns_and_types() -> None:
    payload = {
        "city": "Warsaw",
        "requested_latitude": 52.2297,
        "requested_longitude": 21.0122,
        "hourly": {
            "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
            "temperature_2m": [1.2, 1.0],
            "relative_humidity_2m": [91, 93],
            "precipitation": [0.0, 0.1],
            "wind_speed_10m": [13.5, 12.1],
        },
    }

    frame = transform_payload_to_hourly_df(payload)

    assert len(frame) == 2
    assert list(frame.columns) == [
        "city",
        "latitude",
        "longitude",
        "timestamp",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
    ]
    assert frame.loc[0, "city"] == "Warsaw"
    assert str(frame["timestamp"].dtype).startswith("datetime64")


def test_transform_payloads_to_hourly_df_concatenates_multiple_payloads() -> None:
    payloads = [
        {
            "city": "Warsaw",
            "requested_latitude": 52.2297,
            "requested_longitude": 21.0122,
            "hourly": {
                "time": ["2026-01-01T00:00"],
                "temperature_2m": [0.5],
                "relative_humidity_2m": [88],
                "precipitation": [0.0],
                "wind_speed_10m": [10.0],
            },
        },
        {
            "city": "Berlin",
            "requested_latitude": 52.5200,
            "requested_longitude": 13.4050,
            "hourly": {
                "time": ["2026-01-01T00:00"],
                "temperature_2m": [2.1],
                "relative_humidity_2m": [80],
                "precipitation": [0.0],
                "wind_speed_10m": [8.0],
            },
        },
    ]

    frame = transform_payloads_to_hourly_df(payloads)

    assert len(frame) == 2
    assert set(frame["city"].tolist()) == {"Warsaw", "Berlin"}


def test_transform_payload_to_hourly_df_empty_payload_returns_empty_frame() -> None:
    payload = {"city": "Warsaw", "hourly": {"time": []}}

    frame = transform_payload_to_hourly_df(payload)

    assert frame.empty
