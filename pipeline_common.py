from __future__ import annotations

import csv
import json
import math
import os
import statistics
import tempfile
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".matplotlib"))

LANDING_ZONE = ROOT / "landing_zone"
FORMATTED_ZONE = ROOT / "formatted_zone"
TRUSTED_ZONE = ROOT / "trusted_zone"
EXPLOITATION_ZONE = ROOT / "exploitation_zone"
ANALYSIS_ZONE = ROOT / "analysis_zone"

LANDING_REPORTS = LANDING_ZONE / "reports"

FORMATTED_REPORTS = FORMATTED_ZONE / "reports"
FORMATTED_DB = FORMATTED_ZONE / "formatted_zone.duckdb"

TRUSTED_REPORTS = TRUSTED_ZONE / "reports"
TRUSTED_DB = TRUSTED_ZONE / "trusted_zone.duckdb"

EXPLOITATION_REPORTS = EXPLOITATION_ZONE / "reports"
EXPLOITATION_DB = EXPLOITATION_ZONE / "exploitation_zone.duckdb"

ANALYSIS_VISUALIZATION = ANALYSIS_ZONE / "visualization"
ANALYSIS_CLUSTERING = ANALYSIS_ZONE / "clustering"
ANALYSIS_PREDICTION = ANALYSIS_ZONE / "prediction"
ANALYSIS_REPORTS = ANALYSIS_ZONE / "reports"

DATASETS = {
    "pics": {"path": ROOT / "opendatabcn_pics-csv.csv", "encoding": "utf-16"},
    "weather": {"path": ROOT / "dataset_prat_temps_2025-26.csv", "encoding": "utf-8-sig"},
    "hotels": {"path": ROOT / "opendatabcn_allotjament_hotels-csv.csv", "encoding": "utf-16"},
    "airbnb_listings": {"path": ROOT / "listings.csv", "encoding": "utf-8-sig"},
    "airbnb_neighbourhoods": {"path": ROOT / "neighbourhoods.csv", "encoding": "utf-8-sig"},
    "airbnb_reviews": {"path": ROOT / "reviews_airbnb.csv", "encoding": "utf-8-sig"},
    "airbnb_calendar": {"path": ROOT / "calendar.csv", "encoding": "utf-8-sig"},
}

CORE_ZONE_TABLES = [
    "pics",
    "hotels",
    "weather",
    "airbnb_listings",
    "airbnb_neighbourhoods",
    "airbnb_reviews",
]

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:
    plt = None

try:
    from pyspark.sql import SparkSession
except Exception:
    SparkSession = None

try:
    import duckdb
except Exception:
    duckdb = None


def ensure_zone_dirs() -> None:
    for path in [
        LANDING_REPORTS,
        FORMATTED_REPORTS,
        TRUSTED_REPORTS,
        EXPLOITATION_REPORTS,
        ANALYSIS_VISUALIZATION,
        ANALYSIS_CLUSTERING,
        ANALYSIS_PREDICTION,
        ANALYSIS_REPORTS,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def utc_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def canonical_name(name: str) -> str:
    normalized = []
    for char in name.strip().lower():
        normalized.append(char if char.isalnum() else "_")
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
    return int(number) if number is not None else None


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
        raise RuntimeError("DuckDB no está instalado.")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        columns = list(rows[0].keys())
        types = {col: infer_duckdb_type(row.get(col) for row in rows) for col in columns}
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(f'CREATE TABLE "{table_name}" ({", ".join(f"""\"{col}\" {types[col]}""" for col in columns)})')
        placeholders = ", ".join("?" for _ in columns)
        sql = f'INSERT INTO "{table_name}" ({", ".join(f"""\"{c}\"""" for c in columns)}) VALUES ({placeholders})'
        conn.executemany(sql, [[row.get(col) for col in columns] for row in rows])
        conn.commit()
    finally:
        conn.close()


def write_duckdb_table_from_csv(db_path: Path, table_name: str, csv_path: Path) -> None:
    if duckdb is None:
        raise RuntimeError("DuckDB no está instalado.")
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


def read_duckdb_rows(db_path: Path, table_name: str, order_by: str | None = None) -> list[dict[str, object]]:
    if duckdb is None:
        raise RuntimeError("DuckDB no está instalado.")
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        query = f'SELECT * FROM "{table_name}"'
        if order_by:
            query += f" ORDER BY {order_by}"
        cursor = conn.execute(query)
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()


def load_tables_from_duckdb(db_path: Path, table_names: list[str]) -> dict[str, list[dict[str, object]]]:
    return {table_name: read_duckdb_rows(db_path, table_name) for table_name in table_names}


def latest_landing_copy(dataset_name: str) -> Path | None:
    return None


def resolve_input_path(dataset_name: str) -> Path:
    return DATASETS[dataset_name]["path"]


def temp_csv_path(prefix: str) -> Path:
    handle = tempfile.NamedTemporaryFile(prefix=prefix, suffix=".csv", delete=False)
    handle.close()
    return Path(handle.name)


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def median(values: list[float]) -> float:
    return float(statistics.median(values)) if values else 0.0


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


def mae(y_true: list[float], y_pred: list[float]) -> float:
    return mean([abs(y - y_hat) for y, y_hat in zip(y_true, y_pred)])


def rmse(y_true: list[float], y_pred: list[float]) -> float:
    return math.sqrt(mean([(y - y_hat) ** 2 for y, y_hat in zip(y_true, y_pred)]))


def r2_score(y_true: list[float], y_pred: list[float]) -> float:
    y_mean = mean(y_true)
    ss_tot = sum((y - y_mean) ** 2 for y in y_true)
    ss_res = sum((y - y_hat) ** 2 for y, y_hat in zip(y_true, y_pred))
    return 0.0 if ss_tot == 0 else 1 - (ss_res / ss_tot)


def dot_product(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def train_linear_regression_gradient_descent(
    x_train: list[list[float]],
    y_train: list[float],
    learning_rate: float = 0.03,
    epochs: int = 1200,
) -> list[float]:
    if not x_train:
        return []
    weights = [0.0] * (len(x_train[0]) + 1)
    sample_count = len(x_train)
    for _ in range(epochs):
        gradients = [0.0] * len(weights)
        for row, target in zip(x_train, y_train):
            prediction = weights[0] + dot_product(weights[1:], row)
            error = prediction - target
            gradients[0] += error
            for idx, value in enumerate(row, start=1):
                gradients[idx] += error * value
        for idx in range(len(weights)):
            weights[idx] -= learning_rate * (gradients[idx] / sample_count)
    return weights


def predict_linear_regression(weights: list[float], x_rows: list[list[float]]) -> list[float]:
    if not weights:
        return []
    return [weights[0] + dot_product(weights[1:], row) for row in x_rows]


def knn_predict(
    x_train: list[list[float]],
    y_train: list[float],
    x_test: list[list[float]],
    k: int = 7,
) -> list[float]:
    if not x_train or not x_test:
        return []
    effective_k = max(1, min(k, len(x_train)))
    predictions = []
    for row in x_test:
        distances = [(euclidean_distance(row, train_row), target) for train_row, target in zip(x_train, y_train)]
        neighbors = sorted(distances, key=lambda item: item[0])[:effective_k]
        predictions.append(mean([target for _, target in neighbors]))
    return predictions


def add_calendar_features(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    enriched = []
    for row in rows:
        date_value = datetime.fromisoformat(str(row["date"]))
        enriched_row = dict(row)
        enriched_row["month"] = date_value.month
        enriched_row["day_of_week"] = date_value.weekday()
        enriched_row["is_weekend"] = 1 if date_value.weekday() >= 5 else 0
        enriched.append(enriched_row)
    return enriched


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
