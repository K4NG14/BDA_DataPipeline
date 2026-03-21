#!/usr/bin/env python3

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline_common import (
    DATASETS,
    FORMATTED_DB,
    FORMATTED_REPORTS,
    SparkSession,
    ensure_zone_dirs,
    load_rows,
    parse_bool,
    parse_dt,
    parse_float,
    parse_int,
    parse_price,
    resolve_input_path,
    strip_text,
    write_csv_stream,
    write_duckdb_table,
    write_duckdb_table_from_csv,
    write_json,
    temp_csv_path,
)


def normalize_pics(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    return [
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
        for row in rows
    ]


def normalize_hotels(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    return [
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
        for row in rows
    ]


def normalize_weather(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    normalized = []
    for row in rows:
        timestamp = parse_dt(row.get("timestamp"), "%Y-%m-%d %H:%M:%S")
        normalized.append(
            {
                "dataset": "weather",
                "record_id": strip_text(row.get("_id")),
                "timestamp": timestamp,
                "date": timestamp[:10] if timestamp else "",
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
    source = resolve_input_path("airbnb_calendar")
    encoding = DATASETS["airbnb_calendar"]["encoding"]
    output_csv = temp_csv_path("formatted_airbnb_calendar_")
    fieldnames = ["dataset", "listing_id", "date", "available", "price", "adjusted_price", "minimum_nights", "maximum_nights"]

    def rows() -> Iterable[dict[str, object]]:
        with source.open("r", encoding=encoding, newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                yield normalize_airbnb_calendar_row(row)

    count = write_csv_stream(output_csv, fieldnames, rows())
    if spark is not None:
        dataframe = spark.read.option("header", True).csv(str(output_csv))
        dataframe.count()
    write_duckdb_table_from_csv(FORMATTED_DB, "airbnb_calendar", output_csv)
    output_csv.unlink(missing_ok=True)
    return count


def normalize_airbnb_neighbourhoods(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    return [
        {
            "dataset": "airbnb_neighbourhoods",
            "district_name": strip_text(row.get("neighbourhood_group")),
            "neighborhood_name": strip_text(row.get("neighbourhood")),
        }
        for row in rows
    ]


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
                {"dataset": "airbnb_reviews", "listing_id": listing_id, "review_count": 0, "avg_comment_length": 0.0, "last_review_date": ""},
            )
            entry["review_count"] = int(entry["review_count"]) + 1
            comment = strip_text(row.get("comments"))
            current_count = int(entry["review_count"])
            previous_avg = float(entry["avg_comment_length"])
            entry["avg_comment_length"] = previous_avg + ((len(comment) - previous_avg) / current_count)
            date = strip_text(row.get("date"))
            if date and date > str(entry["last_review_date"]):
                entry["last_review_date"] = date
    return [{**entry, "avg_comment_length": round(float(entry["avg_comment_length"]), 4), "last_review_date": parse_dt(str(entry["last_review_date"]), "%Y-%m-%d")} for entry in stats.values()]


def run_spark_formatter(spark: SparkSession, formatted_rows: dict[str, list[dict[str, object]]]) -> str:
    for dataset, rows in formatted_rows.items():
        if not rows:
            continue
        dataframe = spark.createDataFrame([{key: ("" if value is None else value) for key, value in row.items()} for row in rows])
        dataframe.count()
    return "spark"


def run_formatted_zone() -> dict[str, list[dict[str, object]]]:
    raw_rows = {
        name: load_rows(resolve_input_path(name), meta["encoding"])
        for name, meta in DATASETS.items()
        if name not in {"airbnb_reviews", "airbnb_calendar"}
    }
    formatted_rows = {
        "pics": normalize_pics(raw_rows["pics"]),
        "hotels": normalize_hotels(raw_rows["hotels"]),
        "weather": normalize_weather(raw_rows["weather"]),
        "airbnb_listings": normalize_airbnb_listings(raw_rows["airbnb_listings"]),
        "airbnb_neighbourhoods": normalize_airbnb_neighbourhoods(raw_rows["airbnb_neighbourhoods"]),
        "airbnb_reviews": aggregate_airbnb_reviews(resolve_input_path("airbnb_reviews"), DATASETS["airbnb_reviews"]["encoding"]),
    }
    if SparkSession is None:
        raise RuntimeError("pyspark no está instalado. La formatted zone requiere Spark.")
    spark = SparkSession.builder.appName("bda-formatted-zone").master("local[*]").getOrCreate()
    try:
        selected_engine = run_spark_formatter(spark, formatted_rows)
        airbnb_calendar_count = process_airbnb_calendar_formatted(spark)
    finally:
        spark.stop()
    for dataset, rows in formatted_rows.items():
        write_duckdb_table(FORMATTED_DB, dataset, rows)
    write_json(
        FORMATTED_REPORTS / "formatted_zone_report.json",
        {
            "engine_used": selected_engine,
            "datasets": {**{dataset: len(rows) for dataset, rows in formatted_rows.items()}, "airbnb_calendar": airbnb_calendar_count},
            "duckdb_database": str(FORMATTED_DB.relative_to(ROOT)),
        },
    )
    return formatted_rows


def main() -> None:
    ensure_zone_dirs()
    run_formatted_zone()


if __name__ == "__main__":
    main()
