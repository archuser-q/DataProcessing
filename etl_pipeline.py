import logging
import os
from datetime import datetime, timezone

import psycopg2
import requests

API_URL = "https://api.open-meteo.com/v1/forecast"
DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
    "dbname": os.getenv("PGDATABASE", "weather_etl"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
}
TIMEZONE = "Asia/Bangkok"
FORECAST_DAYS = 7
REQUEST_TIMEOUT_SECONDS = 30

CITIES = {
    "Hanoi": (21.0285, 105.8542),
    "Ho Chi Minh City": (10.8231, 106.6297),
    "Da Nang": (16.0544, 108.2022),
}

HOURLY_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
]

RAIN_THRESHOLD_MM = 0.1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("weather_etl")


def extract(city_name, latitude, longitude):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(HOURLY_FIELDS),
        "timezone": TIMEZONE,
        "forecast_days": FORECAST_DAYS,
    }

    logger.info("Extract: %s", city_name)
    response = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    raw_data = response.json()
    return raw_data


def transform(city_name, raw_data):
    hourly = raw_data["hourly"]
    timestamps = hourly["time"]
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    rows = []
    skipped = 0

    for index, timestamp in enumerate(timestamps):
        temperature = hourly["temperature_2m"][index]
        humidity = hourly["relative_humidity_2m"][index]
        precipitation = hourly["precipitation"][index]
        wind_speed = hourly["wind_speed_10m"][index]

        values = [temperature, humidity, precipitation, wind_speed]
        has_missing_value = any(value is None for value in values)
        if has_missing_value:
            skipped += 1
            continue

        # "2026-10-06T14:00" -> date + hour
        forecast_datetime = datetime.fromisoformat(timestamp)
        forecast_date = forecast_datetime.date().isoformat()
        forecast_hour = forecast_datetime.hour

        is_rainy = 1 if precipitation >= RAIN_THRESHOLD_MM else 0

        row = {
            "city": city_name,
            "forecast_time": timestamp,
            "forecast_date": forecast_date,
            "forecast_hour": forecast_hour,
            "temperature_c": temperature,
            "humidity_pct": humidity,
            "precipitation_mm": precipitation,
            "wind_speed_kmh": wind_speed,
            "is_rainy": is_rainy,
            "ingested_at": ingested_at,
        }
        rows.append(row)

    logger.info("Transform: %s -> %d rows (%d skipped)", city_name, len(rows), skipped)
    return rows


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_hourly (
    city              TEXT           NOT NULL,
    forecast_time     TIMESTAMP      NOT NULL,
    forecast_date     DATE           NOT NULL,
    forecast_hour     SMALLINT       NOT NULL,
    temperature_c     NUMERIC(5, 2),
    humidity_pct      NUMERIC(5, 2),
    precipitation_mm  NUMERIC(7, 2),
    wind_speed_kmh    NUMERIC(6, 2),
    is_rainy          SMALLINT,
    ingested_at       TIMESTAMPTZ    NOT NULL,
    PRIMARY KEY (city, forecast_time)
);
"""

UPSERT_SQL = """
INSERT INTO weather_hourly (
    city, forecast_time, forecast_date, forecast_hour,
    temperature_c, humidity_pct, precipitation_mm, wind_speed_kmh,
    is_rainy, ingested_at
) VALUES (
    %(city)s, %(forecast_time)s, %(forecast_date)s, %(forecast_hour)s,
    %(temperature_c)s, %(humidity_pct)s, %(precipitation_mm)s, %(wind_speed_kmh)s,
    %(is_rainy)s, %(ingested_at)s
)
ON CONFLICT (city, forecast_time) DO UPDATE SET
    temperature_c    = EXCLUDED.temperature_c,
    humidity_pct     = EXCLUDED.humidity_pct,
    precipitation_mm = EXCLUDED.precipitation_mm,
    wind_speed_kmh   = EXCLUDED.wind_speed_kmh,
    is_rainy         = EXCLUDED.is_rainy,
    ingested_at      = EXCLUDED.ingested_at;
"""


def load(connection, rows):
    if not rows:
        logger.warning("Load: no rows to load")
        return

    with connection.cursor() as cursor:
        cursor.executemany(UPSERT_SQL, rows)
    connection.commit()
    logger.info("Load: upserted %d rows", len(rows))


def run():
    connection = psycopg2.connect(**DB_CONFIG)

    with connection.cursor() as cursor:
        cursor.execute(CREATE_TABLE_SQL)
    connection.commit()

    failed_cities = []

    for city_name, (latitude, longitude) in CITIES.items():
        try:
            raw_data = extract(city_name, latitude, longitude)
            rows = transform(city_name, raw_data)
            load(connection, rows)
        except Exception:
            connection.rollback()
            logger.exception("Pipeline failed for %s", city_name)
            failed_cities.append(city_name)

    connection.close()

    if failed_cities:
        raise SystemExit(f"Failed cities: {', '.join(failed_cities)}")

    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    run()