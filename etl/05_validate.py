"""
===============================================================================
05_validate.py — Post-ETL Parquet & Web JSON Integrity Validator
-------------------------------------------------------------------------------
Verifies schema completeness, coordinate hygiene, and cross-file referential
integrity between `etl/processed/02_clean_people.parquet` and the exported web
JSON bundles (`map_lite.json`, `people_details.json`, `search_index.json`,
and `locations.json`).
===============================================================================
"""

from __future__ import annotations

import math
import re
import sys
from pathlib import Path
from typing import Any

import orjson
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "etl" / "processed"
PARQUET_CLEAN = PROCESSED_DIR / "02_clean_people.parquet"

QID_RE = re.compile(r"^Q[1-9]\d*$")


def _resolve_data_dir() -> Path:
    for candidate in (PROJECT_ROOT / "web" / "data", PROJECT_ROOT / "data"):
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        "Could not find exported web data directory (checked web/data and data)."
    )


def validate_parquet(parquet_path: Path, errors: list[str]) -> int:
    if not parquet_path.is_file():
        errors.append(f"Missing Parquet artifact: {parquet_path}")
        return 0

    df = pl.read_parquet(parquet_path)
    if df.height == 0:
        errors.append(f"{parquet_path.name} is empty (0 rows).")
        return 0

    required_cols = {
        "qid",
        "name_en",
        "birthplace_en",
        "region_en",
        "country_en",
        "geo_tree",
        "loc_key",
        "lat",
        "lon",
        "occupation_qids",
        "occupation_labels",
        "sitelinks_count",
        "born_in_ukraine",
        "ukrainian_nationality_or_heritage",
        "origin_type",
        "provenance_tags",
        "birth_year",
        "death_year",
        "esu_id",
        "ieu_id",
        "viaf_id",
    }
    missing = sorted(required_cols - set(df.columns))
    if missing:
        errors.append(f"{parquet_path.name} missing required columns: {missing}")

    unique_qids = df.select(pl.col("qid").n_unique()).item()
    if unique_qids != df.height:
        errors.append(
            f"{parquet_path.name} has duplicate Q-IDs: {df.height} rows vs {unique_qids} unique."
        )

    bad_coords = df.filter(
        pl.col("lat").is_null()
        | pl.col("lon").is_null()
        | ~pl.col("lat").is_between(-90.0, 90.0)
        | ~pl.col("lon").is_between(-180.0, 180.0)
        | ((pl.col("lat") == 0.0) & (pl.col("lon") == 0.0))
    ).height
    if bad_coords > 0:
        errors.append(f"{parquet_path.name} contains {bad_coords} invalid coordinates.")

    leaked_qids = df.filter(
        pl.col("name_en").fill_null("").str.contains(r"^Q[1-9]\d*$")
        | pl.col("birthplace_en").fill_null("").str.contains(r"^Q[1-9]\d*$")
    ).height
    if leaked_qids > 0:
        errors.append(f"{parquet_path.name} has {leaked_qids} leaked Q-ID labels.")

    return df.height


def validate_web_json(data_dir: Path, errors: list[str]) -> dict[str, int]:
    expected_files = (
        "map_lite.json",
        "people_details.json",
        "search_index.json",
        "locations.json",
    )
    loaded: dict[str, Any] = {}
    for fname in expected_files:
        fpath = data_dir / fname
        if not fpath.is_file() or fpath.stat().st_size == 0:
            errors.append(f"Missing or empty web JSON file: {fpath}")
        else:
            loaded[fname] = orjson.loads(fpath.read_bytes())

    if len(loaded) < len(expected_files):
        return {}

    raw_lite = loaded["map_lite.json"]
    people_lite = (
        raw_lite["people"]
        if isinstance(raw_lite, dict) and "people" in raw_lite
        else raw_lite
    )
    details_by_id: dict[str, Any] = loaded["people_details.json"]
    search_index = loaded["search_index.json"]
    raw_locations = loaded["locations.json"]

    if isinstance(raw_locations, dict):
        loc_keys = set(raw_locations.keys())
    elif isinstance(raw_locations, list):
        loc_keys = {
            item["locKey"]
            for item in raw_locations
            if isinstance(item, dict) and "locKey" in item
        }
    else:
        loc_keys = set()
        errors.append("locations.json has unexpected root JSON type.")

    if not isinstance(people_lite, list) or len(people_lite) == 0:
        errors.append("map_lite.json contains no person records.")
        return {}

    missing_details = 0
    missing_locations = 0
    bad_lite_coords = 0

    for person in people_lite:
        pid = person.get("id")
        lat = person.get("lat")
        lon = person.get("lon")
        loc_key = person.get("locKey")

        if pid not in details_by_id:
            missing_details += 1
        if loc_key and loc_keys and loc_key not in loc_keys:
            missing_locations += 1
        if (
            not isinstance(lat, (int, float))
            or not isinstance(lon, (int, float))
            or not (math.isfinite(lat) and math.isfinite(lon))
            or not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0)
            or (lat == 0.0 and lon == 0.0)
        ):
            bad_lite_coords += 1

    if missing_details:
        errors.append(
            f"{missing_details} IDs in map_lite.json are missing from people_details.json."
        )
    if missing_locations:
        errors.append(
            f"{missing_locations} locKeys in map_lite.json are missing from locations.json."
        )
    if bad_lite_coords:
        errors.append(
            f"{bad_lite_coords} records in map_lite.json have invalid coordinates."
        )

    search_count = len(search_index) if isinstance(search_index, (list, dict)) else 0
    if search_count == 0:
        errors.append("search_index.json is empty.")

    return {
        "map_lite": len(people_lite),
        "people_details": len(details_by_id),
        "search_index": search_count,
        "locations": len(loc_keys),
    }


def main() -> None:
    errors: list[str] = []
    parquet_rows = validate_parquet(PARQUET_CLEAN, errors)
    data_dir = _resolve_data_dir()
    counts = validate_web_json(data_dir, errors)

    if errors:
        print("\n=== ETL Validation FAILED ===")
        for err in errors:
            print(f"  [ERROR] {err}")
        sys.exit(1)

    print("\n=== ETL Validation PASSED ===")
    print(f"02_clean_people.parquet : {parquet_rows:,} rows")
    print(f"Web JSON directory      : {data_dir.relative_to(PROJECT_ROOT)}")
    print(f"  - map_lite.json       : {counts['map_lite']:,} people")
    print(f"  - people_details.json : {counts['people_details']:,} biographies")
    print(f"  - search_index.json   : {counts['search_index']:,} entries")
    print(f"  - locations.json      : {counts['locations']:,} locations")


if __name__ == "__main__":
    main()
