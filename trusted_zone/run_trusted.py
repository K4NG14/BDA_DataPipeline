#!/usr/bin/env python3

from __future__ import annotations

import csv
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline_common import (
    CORE_ZONE_TABLES,
    DATASETS,
    FORMATTED_DB,
    TRUSTED_DB,
    TRUSTED_REPORTS,
    ensure_zone_dirs,
    load_tables_from_duckdb,
    parse_bool,
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


@dataclass
class QualityResult:
    dataset: str
    original_rows: int
    valid_rows: int
    removed_rows: int
    rules: dict[str, int]


def is_barcelona_coordinate(latitude: float | None, longitude: float | None) -> bool:
    return latitude is not None and longitude is not None and 41.2 <= latitude <= 41.5 and 2.0 <= longitude <= 2.3


def evaluate_and_clean_formatted(formatted_rows: dict[str, list[dict[str, object]]]) -> tuple[dict[str, list[dict[str, object]]], list[QualityResult]]:
    trusted_rows: dict[str, list[dict[str, object]]] = {}
    results: list[QualityResult] = []

    pic_seen, hotel_seen, weather_seen, neighbourhood_seen, review_seen, airbnb_seen = set(), set(), set(), set(), set(), set()
    valid_pics, valid_hotels, valid_weather, valid_neighbourhoods, valid_reviews, valid_airbnb = [], [], [], [], [], []
    pic_rules, hotel_rules, weather_rules, neighbourhood_rules, review_rules, airbnb_rules = Counter(), Counter(), Counter(), Counter(), Counter(), Counter()

    for row in formatted_rows["pics"]:
        key = (row["record_id"], row["attribute_name"], row["attribute_value"])
        if not row["record_id"] or not row["name"]:
            pic_rules["missing_identity"] += 1
        elif not row["district_name"]:
            pic_rules["missing_district"] += 1
        elif not is_barcelona_coordinate(row["latitude"], row["longitude"]):
            pic_rules["invalid_coordinates"] += 1
        elif key in pic_seen:
            pic_rules["duplicate_row"] += 1
        else:
            pic_seen.add(key)
            valid_pics.append(row)

    for row in formatted_rows["hotels"]:
        key = row["record_id"]
        if not row["record_id"] or not row["name"]:
            hotel_rules["missing_identity"] += 1
        elif not row["district_name"]:
            hotel_rules["missing_district"] += 1
        elif not is_barcelona_coordinate(row["latitude"], row["longitude"]):
            hotel_rules["invalid_coordinates"] += 1
        elif key in hotel_seen:
            hotel_rules["duplicate_row"] += 1
        else:
            hotel_seen.add(key)
            valid_hotels.append(row)

    for row in formatted_rows["weather"]:
        key = row["timestamp"]
        if not row["timestamp"]:
            weather_rules["missing_timestamp"] += 1
        elif key in weather_seen:
            weather_rules["duplicate_timestamp"] += 1
        elif row["temperature_avg"] is None or not (-20 <= row["temperature_avg"] <= 50):
            weather_rules["invalid_temperature"] += 1
        elif row["humidity_avg"] is None or not (0 <= row["humidity_avg"] <= 100):
            weather_rules["invalid_humidity"] += 1
        elif row["pressure_avg"] is None or not (850 <= row["pressure_avg"] <= 1100):
            weather_rules["invalid_pressure"] += 1
        elif row["radiation_avg"] is None or row["radiation_avg"] < 0:
            weather_rules["invalid_radiation"] += 1
        elif row["rain_total"] is None or row["rain_total"] < 0:
            weather_rules["invalid_rain"] += 1
        else:
            weather_seen.add(key)
            valid_weather.append(row)

    for row in formatted_rows["airbnb_neighbourhoods"]:
        key = (row["district_name"], row["neighborhood_name"])
        if not row["district_name"] or not row["neighborhood_name"]:
            neighbourhood_rules["missing_zone_name"] += 1
        elif key in neighbourhood_seen:
            neighbourhood_rules["duplicate_row"] += 1
        else:
            neighbourhood_seen.add(key)
            valid_neighbourhoods.append(row)

    for row in formatted_rows["airbnb_reviews"]:
        key = row["listing_id"]
        if not key:
            review_rules["missing_listing_id"] += 1
        elif key in review_seen:
            review_rules["duplicate_listing"] += 1
        else:
            review_seen.add(key)
            valid_reviews.append(row)

    neighborhood_to_district = {row["neighborhood_name"]: row["district_name"] for row in valid_neighbourhoods}
    for row in formatted_rows["airbnb_listings"]:
        key = row["listing_id"]
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
            row["district_name"] = neighborhood_to_district.get(str(row["neighborhood_name"]), "")
        if not row["district_name"]:
            airbnb_rules["missing_district"] += 1
            continue
        if row["price"] is None or not (15 <= row["price"] <= 5000):
            airbnb_rules["invalid_price"] += 1
            continue
        if row["accommodates"] is None or not (1 <= row["accommodates"] <= 20):
            airbnb_rules["invalid_accommodates"] += 1
            continue
        if row["review_scores_rating"] is not None and not (0 <= row["review_scores_rating"] <= 5):
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
        results.append(QualityResult(dataset, len(original), len(cleaned), len(original) - len(cleaned), dict(rules)))
    return trusted_rows, results


def run_trusted_zone(formatted_rows: dict[str, list[dict[str, object]]]) -> dict[str, list[dict[str, object]]]:
    trusted_rows, quality_results = evaluate_and_clean_formatted(formatted_rows)
    for dataset, rows in trusted_rows.items():
        write_duckdb_table(TRUSTED_DB, dataset, rows)

    valid_listing_ids = {str(row["listing_id"]) for row in trusted_rows.get("airbnb_listings", [])}
    trusted_calendar_path = temp_csv_path("trusted_calendar_filtered_")
    fieldnames = ["dataset", "listing_id", "date", "available", "price", "adjusted_price", "minimum_nights", "maximum_nights"]
    calendar_total = 0
    calendar_valid = 0

    def trusted_calendar_rows() -> Iterable[dict[str, object]]:
        nonlocal calendar_total, calendar_valid
        handle = resolve_input_path("airbnb_calendar").open("r", encoding=DATASETS["airbnb_calendar"]["encoding"], newline="")
        reader = csv.DictReader(handle)
        with handle:
            for row in reader:
                calendar_total += 1
                listing_id = strip_text(row.get("listing_id"))
                date = strip_text(row.get("date"))
                if not listing_id or listing_id not in valid_listing_ids or not date:
                    continue
                calendar_valid += 1
                yield {
                    "dataset": "airbnb_calendar",
                    "listing_id": listing_id,
                    "date": date,
                    "available": parse_bool(row.get("available")),
                    "price": parse_price(row.get("price")),
                    "adjusted_price": parse_price(row.get("adjusted_price")),
                    "minimum_nights": parse_int(row.get("minimum_nights")),
                    "maximum_nights": parse_int(row.get("maximum_nights")),
                }

    write_csv_stream(trusted_calendar_path, fieldnames, trusted_calendar_rows())
    write_duckdb_table_from_csv(TRUSTED_DB, "airbnb_calendar", trusted_calendar_path)
    trusted_calendar_path.unlink(missing_ok=True)
    quality_results.append(QualityResult("airbnb_calendar", calendar_total, calendar_valid, calendar_total - calendar_valid, {"filtered_by_missing_or_unknown_listing": calendar_total - calendar_valid}))
    write_json(TRUSTED_REPORTS / "trusted_zone_quality_report.json", [result.__dict__ for result in quality_results])
    return trusted_rows


def main() -> None:
    ensure_zone_dirs()
    formatted_rows = load_tables_from_duckdb(FORMATTED_DB, CORE_ZONE_TABLES)
    run_trusted_zone(formatted_rows)


if __name__ == "__main__":
    main()
