#!/usr/bin/env python3

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline_common import DATASETS, LANDING_REPORTS, ensure_zone_dirs, utc_run_id, write_json


def run_collectors(run_id: str) -> dict[str, Path]:
    landing_paths: dict[str, Path] = {dataset: meta["path"] for dataset, meta in DATASETS.items()}
    write_json(
        LANDING_REPORTS / "landing_manifest.json",
        {
            "run_id": run_id,
            "created_at_utc": datetime.now(UTC).isoformat(),
            "datasets": {name: str(path.relative_to(ROOT)) for name, path in landing_paths.items()},
        },
    )
    return landing_paths


def main() -> None:
    ensure_zone_dirs()
    run_collectors(utc_run_id())


if __name__ == "__main__":
    main()
