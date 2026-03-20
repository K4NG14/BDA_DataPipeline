#!/usr/bin/env python3
"""
End-to-end project pipeline for the BDA Project 1 datasets.

The script implements the stages described in the project PDF:
1. Data collectors and landing zone.
2. Formatted zone with homogeneous schemas.
3. Trusted zone with data-quality rules, quality assessment, and cleaning.
4. Exploitation zone with integrated analytical datasets.
5. Two data-analysis pipelines:
   - Visualization and descriptive reporting.
   - A clustering pipeline over Airbnb prices, zones, and listing features.

Spark is the preferred engine for formatting/ETL if `pyspark` is installed.
If Spark is not available, the script falls back to a pure-Python execution
path so the project remains runnable in lean environments.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "project_output"
LANDING = OUTPUT / "landing"
FORMATTED = OUTPUT / "formatted"
TRUSTED = OUTPUT / "trusted"
EXPLOITATION = OUTPUT / "exploitation"
ANALYSIS = OUTPUT / "analysis"
REPORTS = OUTPUT / "reports"

DATASETS = {
    "pics": {
        "path": ROOT / "opendatabcn_pics-csv.csv",
        "encoding": "utf-16",
    },
    "weather": {
        "path": ROOT / "dataset_prat_temps_2025-26.csv",
        "encoding": "utf-8-sig",
    },
    "hotels": {
        "path": ROOT / "opendatabcn_allotjament_hotels-csv.csv",
        "encoding": "utf-16",
    },
    "airbnb_listings": {
        "path": ROOT / "listings.csv",
        "encoding": "utf-8-sig",
    },
    "airbnb_neighbourhoods": {
        "path": ROOT / "neighbourhoods.csv",
        "encoding": "utf-8-sig",
    },
    "airbnb_reviews": {
        "path": ROOT / "reviews_airbnb.csv",
        "encoding": "utf-8-sig",
    },
    "airbnb_calendar": {
        "path": ROOT / "calendar.csv",
        "encoding": "utf-8-sig",
    },
}

try:
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover - optional dependency
    plt = None

try:
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - optional dependency
    SparkSession = None

try:
    import duckdb
except Exception:  # pragma: no cover - optional dependency
    duckdb = None


@dataclass
class QualityResult:
    dataset: str
    original_rows: int
    valid_rows: int
    removed_rows: int
    rules: dict[str, int]


def ensure_dirs() -> None:
    for path in [OUTPUT, LANDING, FORMATTED, TRUSTED, EXPLOITATION, ANALYSIS, REPORTS]:
        path.mkdir(parents=True, exist_ok=True)


def utc_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def canonical_name(name: str) -> str:
    normalized = []
    for char in name.strip().lower():
        if char.isalnum():
            normalized.append(char)
        else:
            normalized.append("_")
    cleaned = "".join(normalized)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")


def strip_text(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).replace("\ufeff", "").strip()


def parse_float(value: str | None) -> float | None:
    text = strip_text(value)
    if not text:
        return None
    try:
        return float(text.replace(",", "."))
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    number = parse_float(value)
    if number is None:
        return None
    return int(number)


def parse_bool(value: str | None) -> int | None:
    text = strip_text(value).lower()
    if not text:
        return None
    if text in {"true", "1", "yes", "y", "t"}:
        return 1
    if text in {"false", "0", "no", "n", "f"}:
        return 0
    return None


def parse_dt(value: str | None, fmt: str | None = None) -> str:
    text = strip_text(value)
    if not text:
        return ""
    try:
        if fmt:
            return datetime.strptime(text, fmt).isoformat()
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return text


def parse_price(value: str | None) -> float | None:
    text = strip_text(value)
    if not text:
        return None
    cleaned = text.replace("$", "").replace("€", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def load_rows(path: Path, encoding: str) -> list[dict[str, str]]:
    with path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        return [{canonical_name(k): strip_text(v) for k, v in row.items()} for row in reader]


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_csv_stream(path: Path, fieldnames: list[str], rows: Iterable[dict[str, object]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def infer_duckdb_type(values: Iterable[object]) -> str:
    seen = {type(value) for value in values if value not in ("", None)}
    if not seen:
        return "TEXT"
    if seen.issubset({int}):
        return "BIGINT"
    if seen.issubset({int, float}):
        return "DOUBLE"
    return "TEXT"


def write_duckdb_table(db_path: Path, table_name: str, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    if duckdb is None:
        raise RuntimeError(
            "DuckDB no está instalado. Instálalo con 'python3 -m pip install duckdb' "
            "o ejecuta el script en un entorno donde el paquete esté disponible."
        )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        columns = list(rows[0].keys())
        types = {
            col: infer_duckdb_type(row.get(col) for row in rows)
            for col in columns
        }
        quoted_columns = ", ".join(f'"{col}" {types[col]}' for col in columns)
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(f'CREATE TABLE "{table_name}" ({quoted_columns})')
        placeholders = ", ".join("?" for _ in columns)
        sql = f'INSERT INTO "{table_name}" ({", ".join(f"""\"{c}\"""" for c in columns)}) VALUES ({placeholders})'
        conn.executemany(sql, [[row.get(col) for col in columns] for row in rows])
        conn.commit()
    finally:
        conn.close()


def write_duckdb_table_from_csv(db_path: Path, table_name: str, csv_path: Path) -> None:
    if duckdb is None:
        raise RuntimeError(
            "DuckDB no está instalado. Instálalo con 'python3 -m pip install duckdb' "
            "o ejecuta el script en un entorno donde el paquete esté disponible."
        )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(
            f"""
            CREATE TABLE "{table_name}" AS
            SELECT *
            FROM read_csv_auto('{csv_path.as_posix()}', header=true, sample_size=-1)
            """
        )
        conn.commit()
    finally:
        conn.close()


def run_collectors(run_id: str) -> dict[str, Path]:
    landing_paths: dict[str, Path] = {}
    for dataset, meta in DATASETS.items():
        source = meta["path"]
        target_dir = LANDING / dataset
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"{run_id}_{source.name}"
        shutil.copy2(source, target)
        landing_paths[dataset] = target
    write_json(
        REPORTS / "landing_manifest.json",
        {
            "run_id": run_id,
            "created_at_utc": datetime.now(UTC).isoformat(),
            "datasets": {name: str(path.relative_to(ROOT)) for name, path in landing_paths.items()},
        },
    )
    return landing_paths


def normalize_pics(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    normalized = []
    for row in rows:
        normalized.append(
            {
                "dataset": "pics",
                "record_id": strip_text(row.get("register_id")),
                "name": strip_text(row.get("name")),
                "district_id": strip_text(row.get("addresses_district_id")),
                "district_name": strip_text(row.get("addresses_district_name")),
                "neighborhood_id": strip_text(row.get("addresses_neighborhood_id")),
                "neighborhood_name": strip_text(row.get("addresses_neighborhood_name")),
                "road_name": strip_text(row.get("addresses_road_name")),
                "zip_code": strip_text(row.get("addresses_zip_code")),
                "town": strip_text(row.get("addresses_town")),
                "attribute_category": strip_text(row.get("values_category")),
                "attribute_name": strip_text(row.get("values_attribute_name")),
                "attribute_value": strip_text(row.get("values_value")),
                "main_address": parse_bool(row.get("addresses_main_address")),
                "latitude": parse_float(row.get("geo_epgs_4326_lat")),
                "longitude": parse_float(row.get("geo_epgs_4326_lon")),
                "created_at": parse_dt(row.get("created")),
                "modified_at": parse_dt(row.get("modified")),
            }
        )
    return normalized


def normalize_hotels(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    normalized = []
    for row in rows:
        normalized.append(
            {
                "dataset": "hotels",
                "record_id": strip_text(row.get("register_id")),
                "name": strip_text(row.get("name")),
                "district_id": strip_text(row.get("addresses_district_id")),
                "district_name": strip_text(row.get("addresses_district_name")),
                "neighborhood_id": strip_text(row.get("addresses_neighborhood_id")),
                "neighborhood_name": strip_text(row.get("addresses_neighborhood_name")),
                "road_name": strip_text(row.get("addresses_road_name")),
                "zip_code": strip_text(row.get("addresses_zip_code")),
                "town": strip_text(row.get("addresses_town")),
                "category_name": strip_text(row.get("secondary_filters_name")),
                "category_path": strip_text(row.get("secondary_filters_fullpath")),
                "phone_label": strip_text(row.get("values_attribute_name")),
                "phone_value": strip_text(row.get("values_value")),
                "main_address": parse_bool(row.get("addresses_main_address")),
                "latitude": parse_float(row.get("geo_epgs_4326_lat")),
                "longitude": parse_float(row.get("geo_epgs_4326_lon")),
                "created_at": parse_dt(row.get("created")),
                "modified_at": parse_dt(row.get("modified")),
            }
        )
    return normalized


def normalize_weather(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    normalized = []
    for row in rows:
        timestamp = parse_dt(row.get("timestamp"), "%Y-%m-%d %H:%M:%S")
        date_part = timestamp[:10] if timestamp else ""
        normalized.append(
            {
                "dataset": "weather",
                "record_id": strip_text(row.get("_id")),
                "timestamp": timestamp,
                "date": date_part,
                "record_index": parse_int(row.get("record")),
                "wind_speed_avg": parse_float(row.get("vv_s_wvt")),
                "wind_direction_avg": parse_float(row.get("dv_d1_wvt")),
                "wind_speed_max": parse_float(row.get("vv10m3seg_max")),
                "wind_direction_smm": parse_float(row.get("dv10m3seg_smm")),
                "pressure_avg": parse_float(row.get("pre_avg")),
                "temperature_avg": parse_float(row.get("tem_avg")),
                "humidity_avg": parse_float(row.get("hum_avg")),
                "radiation_avg": parse_float(row.get("rad_avg")),
                "rain_total": parse_float(row.get("plu_tot")),
                "source_file": strip_text(row.get("origen_archivo")),
            }
        )
    return normalized

# Airbnb datasets
def normalize_airbnb_listings(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    normalized = []
    for row in rows:
        normalized.append(
            {
                "dataset": "airbnb_listings",
                "listing_id": strip_text(row.get("id")),
                "name": strip_text(row.get("name")),
                "district_name": strip_text(row.get("neighbourhood_group_cleansed")),
                "neighborhood_name": strip_text(row.get("neighbourhood_cleansed")),
                "neighbourhood_raw": strip_text(row.get("neighbourhood")),
                "latitude": parse_float(row.get("latitude")),
                "longitude": parse_float(row.get("longitude")),
                "property_type": strip_text(row.get("property_type")),
                "room_type": strip_text(row.get("room_type")),
                "accommodates": parse_int(row.get("accommodates")),
                "bathrooms": parse_float(row.get("bathrooms")),
                "bathrooms_text": strip_text(row.get("bathrooms_text")),
                "bedrooms": parse_float(row.get("bedrooms")),
                "beds": parse_float(row.get("beds")),
                "price": parse_price(row.get("price")),
                "minimum_nights": parse_int(row.get("minimum_nights")),
                "maximum_nights": parse_int(row.get("maximum_nights")),
                "availability_365": parse_int(row.get("availability_365")),
                "number_of_reviews": parse_int(row.get("number_of_reviews")),
                "reviews_per_month": parse_float(row.get("reviews_per_month")),
                "review_scores_rating": parse_float(row.get("review_scores_rating")),
                "review_scores_location": parse_float(row.get("review_scores_location")),
                "review_scores_value": parse_float(row.get("review_scores_value")),
                "host_is_superhost": parse_bool(row.get("host_is_superhost")),
                "host_identity_verified": parse_bool(row.get("host_identity_verified")),
                "instant_bookable": parse_bool(row.get("instant_bookable")),
                "calculated_host_listings_count": parse_int(row.get("calculated_host_listings_count")),
                "estimated_occupancy_l365d": parse_float(row.get("estimated_occupancy_l365d")),
                "estimated_revenue_l365d": parse_float(row.get("estimated_revenue_l365d")),
                "last_scraped": parse_dt(row.get("last_scraped"), "%Y-%m-%d"),
                "last_review": parse_dt(row.get("last_review"), "%Y-%m-%d"),
            }
        )
    return normalized


def normalize_airbnb_calendar_row(row: dict[str, str]) -> dict[str, object]:
    return {
        "dataset": "airbnb_calendar",
        "listing_id": strip_text(row.get("listing_id")),
        "date": parse_dt(row.get("date"), "%Y-%m-%d")[:10],
        "available": parse_bool(row.get("available")),
        "price": parse_price(row.get("price")),
        "adjusted_price": parse_price(row.get("adjusted_price")),
        "minimum_nights": parse_int(row.get("minimum_nights")),
        "maximum_nights": parse_int(row.get("maximum_nights")),
    }


def process_airbnb_calendar_formatted(spark: SparkSession | None) -> int:
    source = DATASETS["airbnb_calendar"]["path"]
    encoding = DATASETS["airbnb_calendar"]["encoding"]
    output_csv = FORMATTED / "airbnb_calendar" / "airbnb_calendar.csv"
    fieldnames = [
        "dataset",
        "listing_id",
        "date",
        "available",
        "price",
        "adjusted_price",
        "minimum_nights",
        "maximum_nights",
    ]

    def rows() -> Iterable[dict[str, object]]:
        with source.open("r", encoding=encoding, newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                yield normalize_airbnb_calendar_row(row)

    count = write_csv_stream(output_csv, fieldnames, rows())
    if spark is not None:
        dataframe = spark.read.option("header", True).csv(str(output_csv))
        dataframe.coalesce(1).write.mode("overwrite").option("header", True).csv(str(FORMATTED / "airbnb_calendar" / "csv"))
        dataframe.write.mode("overwrite").parquet(str(FORMATTED / "airbnb_calendar" / "parquet"))
    write_duckdb_table_from_csv(FORMATTED / "formatted_zone.duckdb", "airbnb_calendar", output_csv)
    return count


def normalize_airbnb_neighbourhoods(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    normalized = []
    for row in rows:
        normalized.append(
            {
                "dataset": "airbnb_neighbourhoods",
                "district_name": strip_text(row.get("neighbourhood_group")),
                "neighborhood_name": strip_text(row.get("neighbourhood")),
            }
        )
    return normalized


def aggregate_airbnb_reviews(path: Path, encoding: str) -> list[dict[str, object]]:
    stats: dict[str, dict[str, object]] = {}
    with path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            listing_id = strip_text(row.get("listing_id"))
            if not listing_id:
                continue
            entry = stats.setdefault(
                listing_id,
                {
                    "dataset": "airbnb_reviews",
                    "listing_id": listing_id,
                    "review_count": 0,
                    "avg_comment_length": 0.0,
                    "last_review_date": "",
                },
            )
            entry["review_count"] = int(entry["review_count"]) + 1
            comment = strip_text(row.get("comments"))
            comment_len = len(comment)
            current_count = int(entry["review_count"])
            previous_avg = float(entry["avg_comment_length"])
            entry["avg_comment_length"] = previous_avg + ((comment_len - previous_avg) / current_count)
            date = strip_text(row.get("date"))
            if date and date > str(entry["last_review_date"]):
                entry["last_review_date"] = date
    return [
        {
            **entry,
            "avg_comment_length": round(float(entry["avg_comment_length"]), 4),
            "last_review_date": parse_dt(str(entry["last_review_date"]), "%Y-%m-%d"),
        }
        for entry in stats.values()
    ]


def run_spark_formatter(spark: SparkSession, formatted_rows: dict[str, list[dict[str, object]]]) -> str:
    for dataset, rows in formatted_rows.items():
        if not rows:
            continue
        spark_rows = []
        for row in rows:
            spark_rows.append({key: ("" if value is None else value) for key, value in row.items()})
        dataframe = spark.createDataFrame(spark_rows)
        dataframe.coalesce(1).write.mode("overwrite").option("header", True).csv(str(FORMATTED / dataset / "csv"))
        dataframe.write.mode("overwrite").parquet(str(FORMATTED / dataset / "parquet"))
    return "spark"


def run_formatted_zone(engine: str = "spark") -> dict[str, list[dict[str, object]]]:
    raw_rows = {
        name: load_rows(meta["path"], meta["encoding"])
        for name, meta in DATASETS.items()
        if name not in {"airbnb_reviews", "airbnb_calendar"}
    }
    formatted_rows = {
        "pics": normalize_pics(raw_rows["pics"]),
        "hotels": normalize_hotels(raw_rows["hotels"]),
        "weather": normalize_weather(raw_rows["weather"]),
        "airbnb_listings": normalize_airbnb_listings(raw_rows["airbnb_listings"]),
        "airbnb_neighbourhoods": normalize_airbnb_neighbourhoods(raw_rows["airbnb_neighbourhoods"]),
        "airbnb_reviews": aggregate_airbnb_reviews(
            DATASETS["airbnb_reviews"]["path"],
            DATASETS["airbnb_reviews"]["encoding"],
        ),
    }

    if engine != "spark":
        raise RuntimeError("La formatted zone debe ejecutarse con Spark según el PDF. Usa --engine spark.")
    if SparkSession is None:
        raise RuntimeError("pyspark no está instalado. La formatted zone requiere Spark según el PDF.")
    spark = SparkSession.builder.appName("bda-project-pipeline").master("local[*]").getOrCreate()
    try:
        selected_engine = run_spark_formatter(spark, formatted_rows)
        airbnb_calendar_count = process_airbnb_calendar_formatted(spark)
    finally:
        spark.stop()

    duckdb_db = FORMATTED / "formatted_zone.duckdb"
    for dataset, rows in formatted_rows.items():
        write_csv(FORMATTED / dataset / f"{dataset}.csv", rows)
        write_duckdb_table(duckdb_db, dataset, rows)

    write_json(
        REPORTS / "formatted_zone_report.json",
        {
            "engine_used": selected_engine,
            "datasets": {
                **{dataset: len(rows) for dataset, rows in formatted_rows.items()},
                "airbnb_calendar": airbnb_calendar_count,
            },
            "duckdb_database": str(duckdb_db.relative_to(ROOT)),
        },
    )
    return formatted_rows


def is_barcelona_coordinate(latitude: float | None, longitude: float | None) -> bool:
    if latitude is None or longitude is None:
        return False
    return 41.2 <= latitude <= 41.5 and 2.0 <= longitude <= 2.3


def evaluate_and_clean_formatted(formatted_rows: dict[str, list[dict[str, object]]]) -> tuple[dict[str, list[dict[str, object]]], list[QualityResult]]:
    trusted_rows: dict[str, list[dict[str, object]]] = {}
    results: list[QualityResult] = []

    pic_seen = set()
    valid_pics = []
    pic_rules = Counter()
    for row in formatted_rows["pics"]:
        key = (row["record_id"], row["attribute_name"], row["attribute_value"])
        if not row["record_id"] or not row["name"]:
            pic_rules["missing_identity"] += 1
            continue
        if not row["district_name"]:
            pic_rules["missing_district"] += 1
            continue
        if not is_barcelona_coordinate(row["latitude"], row["longitude"]):
            pic_rules["invalid_coordinates"] += 1
            continue
        if key in pic_seen:
            pic_rules["duplicate_row"] += 1
            continue
        pic_seen.add(key)
        valid_pics.append(row)

    hotel_seen = set()
    valid_hotels = []
    hotel_rules = Counter()
    for row in formatted_rows["hotels"]:
        key = row["record_id"]
        if not row["record_id"] or not row["name"]:
            hotel_rules["missing_identity"] += 1
            continue
        if not row["district_name"]:
            hotel_rules["missing_district"] += 1
            continue
        if not is_barcelona_coordinate(row["latitude"], row["longitude"]):
            hotel_rules["invalid_coordinates"] += 1
            continue
        if key in hotel_seen:
            hotel_rules["duplicate_row"] += 1
            continue
        hotel_seen.add(key)
        valid_hotels.append(row)

    weather_seen = set()
    valid_weather = []
    weather_rules = Counter()
    for row in formatted_rows["weather"]:
        key = row["timestamp"]
        temperature = row["temperature_avg"]
        humidity = row["humidity_avg"]
        pressure = row["pressure_avg"]
        radiation = row["radiation_avg"]
        rain = row["rain_total"]
        if not row["timestamp"]:
            weather_rules["missing_timestamp"] += 1
            continue
        if key in weather_seen:
            weather_rules["duplicate_timestamp"] += 1
            continue
        if temperature is None or not (-20 <= temperature <= 50):
            weather_rules["invalid_temperature"] += 1
            continue
        if humidity is None or not (0 <= humidity <= 100):
            weather_rules["invalid_humidity"] += 1
            continue
        if pressure is None or not (850 <= pressure <= 1100):
            weather_rules["invalid_pressure"] += 1
            continue
        if radiation is None or radiation < 0:
            weather_rules["invalid_radiation"] += 1
            continue
        if rain is None or rain < 0:
            weather_rules["invalid_rain"] += 1
            continue
        weather_seen.add(key)
        valid_weather.append(row)

    valid_neighbourhoods = []
    neighbourhood_seen = set()
    neighbourhood_rules = Counter()
    for row in formatted_rows["airbnb_neighbourhoods"]:
        key = (row["district_name"], row["neighborhood_name"])
        if not row["district_name"] or not row["neighborhood_name"]:
            neighbourhood_rules["missing_zone_name"] += 1
            continue
        if key in neighbourhood_seen:
            neighbourhood_rules["duplicate_row"] += 1
            continue
        neighbourhood_seen.add(key)
        valid_neighbourhoods.append(row)

    valid_reviews = []
    review_seen = set()
    review_rules = Counter()
    for row in formatted_rows["airbnb_reviews"]:
        key = row["listing_id"]
        if not key:
            review_rules["missing_listing_id"] += 1
            continue
        if key in review_seen:
            review_rules["duplicate_listing"] += 1
            continue
        review_seen.add(key)
        valid_reviews.append(row)

    neighborhood_to_district = {
        row["neighborhood_name"]: row["district_name"]
        for row in valid_neighbourhoods
    }
    valid_airbnb = []
    airbnb_seen = set()
    airbnb_rules = Counter()
    for row in formatted_rows["airbnb_listings"]:
        key = row["listing_id"]
        price = row["price"]
        accommodates = row["accommodates"]
        rating = row["review_scores_rating"]
        if not key or not row["name"]:
            airbnb_rules["missing_identity"] += 1
            continue
        if key in airbnb_seen:
            airbnb_rules["duplicate_listing"] += 1
            continue
        if not is_barcelona_coordinate(row["latitude"], row["longitude"]):
            airbnb_rules["invalid_coordinates"] += 1
            continue
        if not row["neighborhood_name"]:
            airbnb_rules["missing_neighborhood"] += 1
            continue
        if not row["district_name"]:
            mapped_district = neighborhood_to_district.get(str(row["neighborhood_name"]))
            if mapped_district:
                row["district_name"] = mapped_district
        if not row["district_name"]:
            airbnb_rules["missing_district"] += 1
            continue
        if price is None or not (15 <= price <= 5000):
            airbnb_rules["invalid_price"] += 1
            continue
        if accommodates is None or not (1 <= accommodates <= 20):
            airbnb_rules["invalid_accommodates"] += 1
            continue
        if rating is not None and not (0 <= rating <= 5):
            airbnb_rules["invalid_rating"] += 1
            continue
        airbnb_seen.add(key)
        valid_airbnb.append(row)

    trusted_rows["pics"] = valid_pics
    trusted_rows["hotels"] = valid_hotels
    trusted_rows["weather"] = valid_weather
    trusted_rows["airbnb_neighbourhoods"] = valid_neighbourhoods
    trusted_rows["airbnb_reviews"] = valid_reviews
    trusted_rows["airbnb_listings"] = valid_airbnb

    for dataset, original, cleaned, rules in [
        ("pics", formatted_rows["pics"], valid_pics, pic_rules),
        ("hotels", formatted_rows["hotels"], valid_hotels, hotel_rules),
        ("weather", formatted_rows["weather"], valid_weather, weather_rules),
        ("airbnb_neighbourhoods", formatted_rows["airbnb_neighbourhoods"], valid_neighbourhoods, neighbourhood_rules),
        ("airbnb_reviews", formatted_rows["airbnb_reviews"], valid_reviews, review_rules),
        ("airbnb_listings", formatted_rows["airbnb_listings"], valid_airbnb, airbnb_rules),
    ]:
        results.append(
            QualityResult(
                dataset=dataset,
                original_rows=len(original),
                valid_rows=len(cleaned),
                removed_rows=len(original) - len(cleaned),
                rules=dict(rules),
            )
        )
    return trusted_rows, results


def run_trusted_zone(formatted_rows: dict[str, list[dict[str, object]]]) -> dict[str, list[dict[str, object]]]:
    trusted_rows, quality_results = evaluate_and_clean_formatted(formatted_rows)
    duckdb_db = TRUSTED / "trusted_zone.duckdb"
    for dataset, rows in trusted_rows.items():
        write_csv(TRUSTED / dataset / f"{dataset}.csv", rows)
        write_duckdb_table(duckdb_db, dataset, rows)

    valid_listing_ids = {str(row["listing_id"]) for row in trusted_rows.get("airbnb_listings", [])}
    formatted_calendar_path = FORMATTED / "airbnb_calendar" / "airbnb_calendar.csv"
    trusted_calendar_path = TRUSTED / "airbnb_calendar" / "airbnb_calendar.csv"
    calendar_fieldnames = [
        "dataset",
        "listing_id",
        "date",
        "available",
        "price",
        "adjusted_price",
        "minimum_nights",
        "maximum_nights",
    ]

    calendar_total = 0
    calendar_valid = 0

    def trusted_calendar_rows() -> Iterable[dict[str, object]]:
        nonlocal calendar_total, calendar_valid
        with formatted_calendar_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                calendar_total += 1
                listing_id = strip_text(row.get("listing_id"))
                date = strip_text(row.get("date"))
                if not listing_id or listing_id not in valid_listing_ids:
                    continue
                if not date:
                    continue
                normalized = {
                    "dataset": "airbnb_calendar",
                    "listing_id": listing_id,
                    "date": date,
                    "available": parse_bool(row.get("available")),
                    "price": parse_price(row.get("price")),
                    "adjusted_price": parse_price(row.get("adjusted_price")),
                    "minimum_nights": parse_int(row.get("minimum_nights")),
                    "maximum_nights": parse_int(row.get("maximum_nights")),
                }
                calendar_valid += 1
                yield normalized

    write_csv_stream(trusted_calendar_path, calendar_fieldnames, trusted_calendar_rows())
    write_duckdb_table_from_csv(duckdb_db, "airbnb_calendar", trusted_calendar_path)
    quality_results.append(
        QualityResult(
            dataset="airbnb_calendar",
            original_rows=calendar_total,
            valid_rows=calendar_valid,
            removed_rows=calendar_total - calendar_valid,
            rules={"filtered_by_missing_or_unknown_listing": calendar_total - calendar_valid},
        )
    )
    write_json(
        REPORTS / "trusted_zone_quality_report.json",
        [result.__dict__ for result in quality_results],
    )
    return trusted_rows


def aggregate_weather_daily(weather_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in weather_rows:
        grouped[str(row["date"])].append(row)

    daily_rows = []
    for date in sorted(grouped):
        rows = grouped[date]
        daily_rows.append(
            {
                "date": date,
                "temperature_avg": round(statistics.fmean(r["temperature_avg"] for r in rows if r["temperature_avg"] is not None), 4),
                "humidity_avg": round(statistics.fmean(r["humidity_avg"] for r in rows if r["humidity_avg"] is not None), 4),
                "pressure_avg": round(statistics.fmean(r["pressure_avg"] for r in rows if r["pressure_avg"] is not None), 4),
                "radiation_avg": round(statistics.fmean(r["radiation_avg"] for r in rows if r["radiation_avg"] is not None), 4),
                "wind_speed_avg": round(statistics.fmean(r["wind_speed_avg"] for r in rows if r["wind_speed_avg"] is not None), 4),
                "rain_total": round(sum(r["rain_total"] or 0 for r in rows), 4),
                "records_per_day": len(rows),
            }
        )
    return daily_rows


def build_district_profile(trusted_rows: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    hotel_by_district = defaultdict(list)
    pic_by_district = defaultdict(list)
    for row in trusted_rows["hotels"]:
        hotel_by_district[str(row["district_name"])].append(row)
    for row in trusted_rows["pics"]:
        pic_by_district[str(row["district_name"])].append(row)

    districts = sorted(set(hotel_by_district) | set(pic_by_district))
    profile = []
    for district in districts:
        hotels = hotel_by_district[district]
        pics = pic_by_district[district]
        neighborhood_count = len({row["neighborhood_name"] for row in hotels + pics if row["neighborhood_name"]})
        categories = Counter(row["category_name"] for row in hotels if row["category_name"])
        top_category = categories.most_common(1)[0][0] if categories else ""
        profile.append(
            {
                "district_name": district,
                "hotels_count": len(hotels),
                "pics_count": len(pics),
                "neighborhood_count": neighborhood_count,
                "top_hotel_category": top_category,
                "tourism_asset_score": len(hotels) * 2 + len(pics),
            }
        )
    return profile


def build_neighborhood_tourism_profile(trusted_rows: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    pics_by_neigh = defaultdict(list)
    hotels_by_neigh = defaultdict(list)
    district_by_neigh = {}
    for row in trusted_rows["pics"]:
        name = str(row["neighborhood_name"])
        pics_by_neigh[name].append(row)
        district_by_neigh[name] = str(row["district_name"])
    for row in trusted_rows["hotels"]:
        name = str(row["neighborhood_name"])
        hotels_by_neigh[name].append(row)
        district_by_neigh[name] = str(row["district_name"])
    for row in trusted_rows.get("airbnb_neighbourhoods", []):
        district_by_neigh[str(row["neighborhood_name"])] = str(row["district_name"])

    neighborhoods = sorted(set(district_by_neigh) | set(pics_by_neigh) | set(hotels_by_neigh))
    profile = []
    for name in neighborhoods:
        pics = pics_by_neigh[name]
        hotels = hotels_by_neigh[name]
        profile.append(
            {
                "district_name": district_by_neigh.get(name, ""),
                "neighborhood_name": name,
                "pics_count": len(pics),
                "hotels_count": len(hotels),
                "tourism_asset_score": len(hotels) * 2 + len(pics),
            }
        )
    return profile


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(statistics.median(values))


def build_airbnb_listing_enriched(
    trusted_rows: dict[str, list[dict[str, object]]],
    district_profile: list[dict[str, object]],
    neighborhood_profile: list[dict[str, object]],
) -> list[dict[str, object]]:
    district_map = {row["district_name"]: row for row in district_profile}
    neighborhood_map = {row["neighborhood_name"]: row for row in neighborhood_profile}
    review_map = {row["listing_id"]: row for row in trusted_rows.get("airbnb_reviews", [])}
    enriched = []
    for row in trusted_rows.get("airbnb_listings", []):
        review = review_map.get(row["listing_id"], {})
        neigh = neighborhood_map.get(row["neighborhood_name"], {})
        district = district_map.get(row["district_name"], {})
        enriched.append(
            {
                **row,
                "review_count_from_reviews_csv": review.get("review_count", 0),
                "avg_comment_length": review.get("avg_comment_length", 0.0),
                "neighborhood_pics_count": neigh.get("pics_count", 0),
                "neighborhood_hotels_count": neigh.get("hotels_count", 0),
                "neighborhood_tourism_asset_score": neigh.get("tourism_asset_score", 0),
                "district_pics_count": district.get("pics_count", 0),
                "district_hotels_count": district.get("hotels_count", 0),
                "district_tourism_asset_score": district.get("tourism_asset_score", 0),
            }
        )
    return enriched


def build_airbnb_zone_features(listing_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in listing_rows:
        grouped[(str(row["district_name"]), str(row["neighborhood_name"]))].append(row)

    features = []
    for (district, neighborhood), rows in sorted(grouped.items()):
        prices = [float(r["price"]) for r in rows if r["price"] is not None]
        ratings = [float(r["review_scores_rating"]) for r in rows if r["review_scores_rating"] is not None]
        accommodates = [float(r["accommodates"]) for r in rows if r["accommodates"] is not None]
        bedrooms = [float(r["bedrooms"]) for r in rows if r["bedrooms"] is not None]
        availabilities = [float(r["availability_365"]) for r in rows if r["availability_365"] is not None]
        reviews_per_month = [float(r["reviews_per_month"]) for r in rows if r["reviews_per_month"] is not None]
        rows_count = len(rows)
        features.append(
            {
                "district_name": district,
                "neighborhood_name": neighborhood,
                "listing_count": rows_count,
                "avg_price": round(mean(prices), 4),
                "median_price": round(median(prices), 4),
                "avg_rating": round(mean(ratings), 4) if ratings else 0.0,
                "avg_accommodates": round(mean(accommodates), 4) if accommodates else 0.0,
                "avg_bedrooms": round(mean(bedrooms), 4) if bedrooms else 0.0,
                "avg_availability_365": round(mean(availabilities), 4) if availabilities else 0.0,
                "avg_reviews_per_month": round(mean(reviews_per_month), 4) if reviews_per_month else 0.0,
                "entire_home_ratio": round(sum(1 for r in rows if r["room_type"] == "Entire home/apt") / rows_count, 4),
                "private_room_ratio": round(sum(1 for r in rows if r["room_type"] == "Private room") / rows_count, 4),
                "superhost_ratio": round(sum(1 for r in rows if r["host_is_superhost"] == 1) / rows_count, 4),
                "instant_bookable_ratio": round(sum(1 for r in rows if r["instant_bookable"] == 1) / rows_count, 4),
                "avg_review_count": round(mean([float(r["review_count_from_reviews_csv"]) for r in rows]), 4),
                "avg_comment_length": round(mean([float(r["avg_comment_length"]) for r in rows]), 4),
                "neighborhood_pics_count": rows[0]["neighborhood_pics_count"],
                "neighborhood_hotels_count": rows[0]["neighborhood_hotels_count"],
                "neighborhood_tourism_asset_score": rows[0]["neighborhood_tourism_asset_score"],
                "district_tourism_asset_score": rows[0]["district_tourism_asset_score"],
            }
        )
    return features


def build_airbnb_zone_day_features(
    trusted_rows: dict[str, list[dict[str, object]]],
    airbnb_listing_enriched: list[dict[str, object]],
    weather_daily: list[dict[str, object]],
) -> list[dict[str, object]]:
    listing_map = {str(row["listing_id"]): row for row in airbnb_listing_enriched}
    weather_map = {str(row["date"]): row for row in weather_daily}
    grouped: dict[tuple[str, str, str], dict[str, float | int | str]] = {}
    calendar_path = TRUSTED / "airbnb_calendar" / "airbnb_calendar.csv"

    with calendar_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            listing = listing_map.get(strip_text(row.get("listing_id")))
            if not listing:
                continue
            date = strip_text(row.get("date"))
            key = (str(listing["district_name"]), str(listing["neighborhood_name"]), date)
            bucket = grouped.setdefault(
                key,
                {
                    "district_name": str(listing["district_name"]),
                    "neighborhood_name": str(listing["neighborhood_name"]),
                    "date": date,
                    "listing_count": 0,
                    "available_count": 0,
                    "booked_count": 0,
                    "price_sum": 0.0,
                    "price_count": 0,
                    "minimum_nights_sum": 0.0,
                    "minimum_nights_count": 0,
                    "neighborhood_tourism_asset_score": float(listing["neighborhood_tourism_asset_score"]),
                    "district_tourism_asset_score": float(listing["district_tourism_asset_score"]),
                },
            )
            bucket["listing_count"] = int(bucket["listing_count"]) + 1
            available = parse_bool(row.get("available"))
            if available == 1:
                bucket["available_count"] = int(bucket["available_count"]) + 1
            elif available == 0:
                bucket["booked_count"] = int(bucket["booked_count"]) + 1
            row_price = parse_price(row.get("adjusted_price"))
            if row_price is None:
                row_price = parse_price(row.get("price"))
            if row_price is None:
                row_price = float(listing["price"]) if listing["price"] is not None else None
            if row_price is not None:
                bucket["price_sum"] = float(bucket["price_sum"]) + row_price
                bucket["price_count"] = int(bucket["price_count"]) + 1
            row_min = parse_int(row.get("minimum_nights"))
            if row_min is not None:
                bucket["minimum_nights_sum"] = float(bucket["minimum_nights_sum"]) + row_min
                bucket["minimum_nights_count"] = int(bucket["minimum_nights_count"]) + 1

    final_rows = []
    for key in sorted(grouped):
        row = grouped[key]
        count = int(row["listing_count"])
        date = str(row["date"])
        weather = weather_map.get(date, {})
        final_rows.append(
            {
                "district_name": row["district_name"],
                "neighborhood_name": row["neighborhood_name"],
                "date": date,
                "listing_count": count,
                "available_count": int(row["available_count"]),
                "booked_count": int(row["booked_count"]),
                "availability_rate": round(int(row["available_count"]) / count, 4) if count else 0.0,
                "booked_rate": round(int(row["booked_count"]) / count, 4) if count else 0.0,
                "avg_calendar_price": round(float(row["price_sum"]) / int(row["price_count"]), 4) if int(row["price_count"]) else 0.0,
                "avg_minimum_nights": round(float(row["minimum_nights_sum"]) / int(row["minimum_nights_count"]), 4) if int(row["minimum_nights_count"]) else 0.0,
                "neighborhood_tourism_asset_score": row["neighborhood_tourism_asset_score"],
                "district_tourism_asset_score": row["district_tourism_asset_score"],
                "temperature_avg": weather.get("temperature_avg", 0.0),
                "humidity_avg": weather.get("humidity_avg", 0.0),
                "pressure_avg": weather.get("pressure_avg", 0.0),
                "radiation_avg": weather.get("radiation_avg", 0.0),
                "wind_speed_avg": weather.get("wind_speed_avg", 0.0),
                "rain_total": weather.get("rain_total", 0.0),
            }
        )
    return final_rows


def build_district_day_features(district_profile: list[dict[str, object]], weather_daily: list[dict[str, object]]) -> list[dict[str, object]]:
    rows = []
    for district in district_profile:
        for weather in weather_daily:
            rows.append(
                {
                    "date": weather["date"],
                    "district_name": district["district_name"],
                    "hotels_count": district["hotels_count"],
                    "pics_count": district["pics_count"],
                    "neighborhood_count": district["neighborhood_count"],
                    "tourism_asset_score": district["tourism_asset_score"],
                    "temperature_avg": weather["temperature_avg"],
                    "humidity_avg": weather["humidity_avg"],
                    "pressure_avg": weather["pressure_avg"],
                    "radiation_avg": weather["radiation_avg"],
                    "wind_speed_avg": weather["wind_speed_avg"],
                    "rain_total": weather["rain_total"],
                }
            )
    return rows


def run_exploitation_zone(trusted_rows: dict[str, list[dict[str, object]]]) -> dict[str, list[dict[str, object]]]:
    district_profile = build_district_profile(trusted_rows)
    neighborhood_profile = build_neighborhood_tourism_profile(trusted_rows)
    weather_daily = aggregate_weather_daily(trusted_rows["weather"])
    district_day_features = build_district_day_features(district_profile, weather_daily)
    airbnb_listing_enriched = build_airbnb_listing_enriched(trusted_rows, district_profile, neighborhood_profile)
    airbnb_zone_features = build_airbnb_zone_features(airbnb_listing_enriched)
    airbnb_zone_day_features = build_airbnb_zone_day_features(trusted_rows, airbnb_listing_enriched, weather_daily)

    outputs = {
        "district_profile": district_profile,
        "neighborhood_profile": neighborhood_profile,
        "weather_daily": weather_daily,
        "district_day_features": district_day_features,
        "airbnb_listing_enriched": airbnb_listing_enriched,
        "airbnb_zone_features": airbnb_zone_features,
        "airbnb_zone_day_features": airbnb_zone_day_features,
    }
    duckdb_db = EXPLOITATION / "exploitation_zone.duckdb"
    for dataset, rows in outputs.items():
        write_csv(EXPLOITATION / f"{dataset}.csv", rows)
        write_duckdb_table(duckdb_db, dataset, rows)
    write_json(
        REPORTS / "exploitation_zone_report.json",
        {name: len(rows) for name, rows in outputs.items()},
    )
    return outputs


def save_plot(path: Path, title: str, x_values: list[object], y_values: list[float], kind: str = "bar") -> str | None:
    if plt is None:
        return None
    plt.figure(figsize=(10, 5))
    if kind == "line":
        plt.plot(x_values, y_values, marker="o", linewidth=1.5)
    else:
        plt.bar(x_values, y_values)
        plt.xticks(rotation=45, ha="right")
    plt.title(title)
    plt.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path, dpi=150)
    plt.close()
    return str(path.relative_to(ROOT))


def run_visualization_pipeline(exploitation_rows: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    airbnb_zones = sorted(
        exploitation_rows["airbnb_zone_features"],
        key=lambda row: row["avg_price"],
        reverse=True,
    )
    weather_daily = exploitation_rows["weather_daily"]

    top_districts = airbnb_zones[:10]
    profile_summary = [
        {
            "district_name": row["district_name"],
            "neighborhood_name": row["neighborhood_name"],
            "listing_count": row["listing_count"],
            "avg_price": row["avg_price"],
            "neighborhood_tourism_asset_score": row["neighborhood_tourism_asset_score"],
        }
        for row in top_districts
    ]
    write_csv(ANALYSIS / "visualization" / "airbnb_zone_top10.csv", profile_summary)
    write_csv(ANALYSIS / "visualization" / "daily_weather.csv", weather_daily)

    hotels_plot = save_plot(
        ANALYSIS / "visualization" / "airbnb_avg_price_by_zone.png",
        "Precio medio Airbnb por zona",
        [f"{row['district_name']} / {row['neighborhood_name']}" for row in top_districts],
        [row["avg_price"] for row in top_districts],
    )
    weather_plot = save_plot(
        ANALYSIS / "visualization" / "daily_temperature.png",
        "Temperatura media diaria",
        [row["date"] for row in weather_daily],
        [row["temperature_avg"] for row in weather_daily],
        kind="line",
    )

    result = {
        "top_districts": profile_summary,
        "generated_plots": [plot for plot in [hotels_plot, weather_plot] if plot],
    }
    write_json(ANALYSIS / "visualization" / "summary.json", result)
    return result


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def euclidean_distance(a: list[float], b: list[float]) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def standardize_matrix(matrix: list[list[float]]) -> tuple[list[list[float]], list[float], list[float]]:
    columns = list(zip(*matrix))
    means = [mean(list(col)) for col in columns]
    stds = []
    for idx, col in enumerate(columns):
        variance = mean([(value - means[idx]) ** 2 for value in col])
        stds.append(math.sqrt(variance) if variance > 0 else 1.0)
    scaled = []
    for row in matrix:
        scaled.append([(value - means[idx]) / stds[idx] for idx, value in enumerate(row)])
    return scaled, means, stds


def run_kmeans(matrix: list[list[float]], k: int = 4, max_iter: int = 40) -> tuple[list[int], list[list[float]], float]:
    if not matrix:
        return [], [], 0.0
    k = max(1, min(k, len(matrix)))
    seed_indexes = sorted({int(i * len(matrix) / k) for i in range(k)})
    centroids = [matrix[idx][:] for idx in seed_indexes]
    labels = [0] * len(matrix)
    for _ in range(max_iter):
        new_labels = []
        for row in matrix:
            distances = [euclidean_distance(row, centroid) for centroid in centroids]
            new_labels.append(min(range(len(distances)), key=distances.__getitem__))
        if new_labels == labels:
            break
        labels = new_labels
        for cluster_id in range(len(centroids)):
            cluster_rows = [row for row, label in zip(matrix, labels) if label == cluster_id]
            if not cluster_rows:
                continue
            centroids[cluster_id] = [
                mean([row[col_idx] for row in cluster_rows])
                for col_idx in range(len(cluster_rows[0]))
            ]
    inertia = 0.0
    for row, label in zip(matrix, labels):
        inertia += euclidean_distance(row, centroids[label]) ** 2
    return labels, centroids, round(inertia, 6)


def describe_clusters(rows: list[dict[str, object]], labels: list[int]) -> list[dict[str, object]]:
    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row, label in zip(rows, labels):
        grouped[label].append(row)
    summaries = []
    for label in sorted(grouped):
        cluster_rows = grouped[label]
        summaries.append(
            {
                "cluster_id": label,
                "zone_count": len(cluster_rows),
                "districts": sorted({str(row["district_name"]) for row in cluster_rows}),
                "avg_price": round(mean([float(row["avg_price"]) for row in cluster_rows]), 4),
                "median_price": round(mean([float(row["median_price"]) for row in cluster_rows]), 4),
                "avg_rating": round(mean([float(row["avg_rating"]) for row in cluster_rows]), 4),
                "avg_accommodates": round(mean([float(row["avg_accommodates"]) for row in cluster_rows]), 4),
                "avg_tourism_asset_score": round(mean([float(row["neighborhood_tourism_asset_score"]) for row in cluster_rows]), 4),
                "avg_listing_count": round(mean([float(row["listing_count"]) for row in cluster_rows]), 4),
            }
        )
    return summaries


def run_clustering_pipeline(exploitation_rows: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    zone_rows = [row for row in exploitation_rows["airbnb_zone_features"] if row["listing_count"] >= 15]
    feature_names = [
        "avg_price",
        "median_price",
        "avg_rating",
        "avg_accommodates",
        "avg_bedrooms",
        "avg_availability_365",
        "entire_home_ratio",
        "private_room_ratio",
        "superhost_ratio",
        "instant_bookable_ratio",
        "avg_review_count",
        "neighborhood_tourism_asset_score",
        "district_tourism_asset_score",
    ]
    matrix = [[float(row[name]) for name in feature_names] for row in zone_rows]
    scaled, means, stds = standardize_matrix(matrix)
    labels, centroids, inertia = run_kmeans(scaled, k=4)

    enriched_rows = []
    for row, label in zip(zone_rows, labels):
        enriched = dict(row)
        enriched["cluster_id"] = label
        enriched_rows.append(enriched)

    cluster_summary = describe_clusters(zone_rows, labels)
    write_csv(ANALYSIS / "clustering" / "airbnb_zone_clusters.csv", enriched_rows)
    write_json(
        ANALYSIS / "clustering" / "cluster_summary.json",
        {
            "feature_names": feature_names,
            "scaled_feature_means": means,
            "scaled_feature_stds": stds,
            "inertia": inertia,
            "cluster_summary": cluster_summary,
        },
    )

    cluster_plot = save_plot(
        ANALYSIS / "clustering" / "cluster_avg_price.png",
        "Precio medio por zona clusterizada",
        [f"{row['district_name']} / {row['neighborhood_name']}" for row in enriched_rows[:20]],
        [row["avg_price"] for row in enriched_rows[:20]],
    )
    return {
        "zones_clustered": len(enriched_rows),
        "k": 4,
        "inertia": inertia,
        "feature_names": feature_names,
        "cluster_summary": cluster_summary,
        "generated_plots": [plot for plot in [cluster_plot] if plot],
    }


def write_project_summary(run_id: str, analysis_outputs: dict[str, object]) -> None:
    summary = {
        "run_id": run_id,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "zones": {
            "landing": str(LANDING.relative_to(ROOT)),
            "formatted": str(FORMATTED.relative_to(ROOT)),
            "trusted": str(TRUSTED.relative_to(ROOT)),
            "exploitation": str(EXPLOITATION.relative_to(ROOT)),
            "analysis": str(ANALYSIS.relative_to(ROOT)),
        },
        "analysis_outputs": analysis_outputs,
    }
    write_json(REPORTS / "project_summary.json", summary)


def run_all(engine: str) -> None:
    ensure_dirs()
    run_id = utc_run_id()
    run_collectors(run_id)
    formatted_rows = run_formatted_zone(engine=engine)
    trusted_rows = run_trusted_zone(formatted_rows)
    exploitation_rows = run_exploitation_zone(trusted_rows)
    analysis_outputs = {
        "visualization": run_visualization_pipeline(exploitation_rows),
        "clustering": run_clustering_pipeline(exploitation_rows),
    }
    write_project_summary(run_id, analysis_outputs)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BDA Project 1 end-to-end pipeline.")
    parser.add_argument(
        "--engine",
        choices=["spark"],
        default="spark",
        help="Execution engine for the formatted zone. Spark is mandatory per the project PDF.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_all(engine=args.engine)


if __name__ == "__main__":
    main()
