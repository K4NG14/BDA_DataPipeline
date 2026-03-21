#!/usr/bin/env python3

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline_common import (
    ANALYSIS_CLUSTERING,
    ANALYSIS_PREDICTION,
    ANALYSIS_REPORTS,
    ANALYSIS_VISUALIZATION,
    EXPLOITATION_DB,
    add_calendar_features,
    ensure_zone_dirs,
    knn_predict,
    load_tables_from_duckdb,
    mae,
    mean,
    r2_score,
    read_duckdb_rows,
    rmse,
    run_kmeans,
    save_plot,
    standardize_matrix,
    train_linear_regression_gradient_descent,
    predict_linear_regression,
    write_csv,
    write_json,
)


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


def run_visualization_pipeline() -> dict[str, object]:
    airbnb_zones = sorted(read_duckdb_rows(EXPLOITATION_DB, "airbnb_zone_features"), key=lambda row: row["avg_price"], reverse=True)
    weather_daily = read_duckdb_rows(EXPLOITATION_DB, "weather_daily", order_by="date")
    top_zones = airbnb_zones[:10]
    summary_rows = [
        {
            "district_name": row["district_name"],
            "neighborhood_name": row["neighborhood_name"],
            "listing_count": row["listing_count"],
            "avg_price": row["avg_price"],
            "neighborhood_tourism_asset_score": row["neighborhood_tourism_asset_score"],
        }
        for row in top_zones
    ]
    write_csv(ANALYSIS_VISUALIZATION / "airbnb_zone_top10.csv", summary_rows)
    write_csv(ANALYSIS_VISUALIZATION / "daily_weather.csv", weather_daily)
    zone_plot = save_plot(
        ANALYSIS_VISUALIZATION / "airbnb_avg_price_by_zone.png",
        "Precio medio Airbnb por zona",
        [f"{row['district_name']} / {row['neighborhood_name']}" for row in top_zones],
        [row["avg_price"] for row in top_zones],
    )
    weather_plot = save_plot(
        ANALYSIS_VISUALIZATION / "daily_temperature.png",
        "Temperatura media diaria",
        [row["date"] for row in weather_daily],
        [row["temperature_avg"] for row in weather_daily],
        kind="line",
    )
    result = {"top_districts": summary_rows, "generated_plots": [plot for plot in [zone_plot, weather_plot] if plot]}
    write_json(ANALYSIS_VISUALIZATION / "summary.json", result)
    return result


def run_clustering_pipeline() -> dict[str, object]:
    zone_rows = [row for row in read_duckdb_rows(EXPLOITATION_DB, "airbnb_zone_features") if row["listing_count"] >= 15]
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
    labels, _, inertia = run_kmeans(scaled, k=4)
    enriched_rows = []
    for row, label in zip(zone_rows, labels):
        enriched = dict(row)
        enriched["cluster_id"] = label
        enriched_rows.append(enriched)
    cluster_summary = describe_clusters(zone_rows, labels)
    write_csv(ANALYSIS_CLUSTERING / "airbnb_zone_clusters.csv", enriched_rows)
    write_json(
        ANALYSIS_CLUSTERING / "cluster_summary.json",
        {"feature_names": feature_names, "scaled_feature_means": means, "scaled_feature_stds": stds, "inertia": inertia, "cluster_summary": cluster_summary},
    )
    cluster_plot = save_plot(
        ANALYSIS_CLUSTERING / "cluster_avg_price.png",
        "Precio medio por zona clusterizada",
        [f"{row['district_name']} / {row['neighborhood_name']}" for row in enriched_rows[:20]],
        [row["avg_price"] for row in enriched_rows[:20]],
    )
    return {"zones_clustered": len(enriched_rows), "k": 4, "inertia": inertia, "feature_names": feature_names, "cluster_summary": cluster_summary, "generated_plots": [plot for plot in [cluster_plot] if plot]}


def run_prediction_pipeline() -> dict[str, object]:
    zone_day_rows = add_calendar_features(read_duckdb_rows(EXPLOITATION_DB, "airbnb_zone_day_features", order_by="date, district_name, neighborhood_name"))
    zone_day_rows = [row for row in zone_day_rows if row["listing_count"] >= 15]
    feature_names = [
        "avg_calendar_price",
        "avg_minimum_nights",
        "listing_count",
        "neighborhood_tourism_asset_score",
        "district_tourism_asset_score",
        "temperature_avg",
        "humidity_avg",
        "radiation_avg",
        "wind_speed_avg",
        "rain_total",
        "month",
        "day_of_week",
        "is_weekend",
    ]
    matrix = [[float(row[name]) for name in feature_names] for row in zone_day_rows]
    targets = [float(row["booked_rate"]) for row in zone_day_rows]
    scaled_matrix, means, stds = standardize_matrix(matrix)
    split = max(1, int(len(scaled_matrix) * 0.8))
    x_train, x_test = scaled_matrix[:split], scaled_matrix[split:] or scaled_matrix[:]
    y_train, y_test = targets[:split], targets[split:] or targets[:]
    test_rows = zone_day_rows[split:] or zone_day_rows[:]

    baseline_value = mean(y_train)
    baseline_predictions = [baseline_value] * len(x_test)
    linear_weights = train_linear_regression_gradient_descent(x_train, y_train)
    linear_predictions = [min(1.0, max(0.0, value)) for value in predict_linear_regression(linear_weights, x_test)]
    knn_k = max(3, min(15, int(len(x_train) ** 0.5)))
    knn_predictions = [min(1.0, max(0.0, value)) for value in knn_predict(x_train, y_train, x_test, k=knn_k)]

    model_metrics = [
        {"model": "baseline_mean", "rmse": round(rmse(y_test, baseline_predictions), 6), "mae": round(mae(y_test, baseline_predictions), 6), "r2": round(r2_score(y_test, baseline_predictions), 6)},
        {"model": "linear_regression", "rmse": round(rmse(y_test, linear_predictions), 6), "mae": round(mae(y_test, linear_predictions), 6), "r2": round(r2_score(y_test, linear_predictions), 6)},
        {"model": f"knn_regression_k_{knn_k}", "rmse": round(rmse(y_test, knn_predictions), 6), "mae": round(mae(y_test, knn_predictions), 6), "r2": round(r2_score(y_test, knn_predictions), 6)},
    ]
    best_model = min(model_metrics, key=lambda item: item["rmse"])
    prediction_rows = []
    for index, row in enumerate(test_rows):
        prediction_rows.append(
            {
                "date": row["date"],
                "district_name": row["district_name"],
                "neighborhood_name": row["neighborhood_name"],
                "booked_rate_observed": round(y_test[index], 6),
                "booked_rate_baseline_mean": round(baseline_predictions[index], 6),
                "booked_rate_linear_regression": round(linear_predictions[index], 6),
                f"booked_rate_knn_regression_k_{knn_k}": round(knn_predictions[index], 6),
                "avg_calendar_price": row["avg_calendar_price"],
                "listing_count": row["listing_count"],
                "temperature_avg": row["temperature_avg"],
                "rain_total": row["rain_total"],
            }
        )
    metrics = {
        "target": "booked_rate",
        "train_rows": len(x_train),
        "test_rows": len(x_test),
        "models": model_metrics,
        "best_model": best_model["model"],
        "feature_names": feature_names,
        "standardization_means": means,
        "standardization_stds": stds,
        "linear_regression_weights": linear_weights,
        "knn_k": knn_k,
        "source_table": "airbnb_zone_day_features",
        "source_database": str(EXPLOITATION_DB.relative_to(ROOT)),
    }
    write_csv(ANALYSIS_PREDICTION / "booked_rate_predictions.csv", prediction_rows)
    write_json(ANALYSIS_PREDICTION / "model_comparison.json", metrics)
    rmse_plot = save_plot(ANALYSIS_PREDICTION / "rmse_comparison.png", "Comparativa RMSE de modelos", [item["model"] for item in model_metrics], [item["rmse"] for item in model_metrics])
    best_model_plot = save_plot(
        ANALYSIS_PREDICTION / "best_model_prediction_sample.png",
        f"Booked rate observado vs predicho ({best_model['model']})",
        [f"{row['district_name']} / {row['neighborhood_name']}" for row in prediction_rows[:20]],
        [row["booked_rate_observed"] for row in prediction_rows[:20]],
    )
    return {"target": "booked_rate", "source_table": "airbnb_zone_day_features", "train_rows": len(x_train), "test_rows": len(x_test), "models": model_metrics, "best_model": best_model["model"], "feature_names": feature_names, "generated_plots": [plot for plot in [rmse_plot, best_model_plot] if plot]}


def main() -> None:
    ensure_zone_dirs()
    results = {
        "visualization": run_visualization_pipeline(),
        "clustering": run_clustering_pipeline(),
        "prediction": run_prediction_pipeline(),
        "exploitation_database": {"path": str(EXPLOITATION_DB.relative_to(ROOT))},
    }
    write_json(ANALYSIS_REPORTS / "analysis_summary.json", results)


if __name__ == "__main__":
    main()
