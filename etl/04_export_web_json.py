"""
===============================================================================
04_export_web_json.py — Production Web JSON Exporter & Contract Verifier
-------------------------------------------------------------------------------
Reads `etl/processed/03_categorized.parquet` and serializes the 4 static,
minified JSON payloads consumed by `web/js/dataLoader.js` and `web/js/ui.js`:

  1. `web/data/map_lite.json`      — Spatial & category filter tuples
  2. `web/data/topic_map.json`     — Alphabetical category/sub-category counts
  3. `web/data/people_details.json`— Biography, portrait & Wikipedia dictionary
  4. `web/data/search_index.json`  — Snapped locations, zones & trilingual search

After writing all 4 files with `orjson` (compact, zero-whitespace output),
prints a Payload Budget Report (record counts, raw size, gzip transfer size)
and executes `pytest tests/test_web_contracts.py` to guarantee 100% schema
and referential integrity compliance.
===============================================================================
"""

from __future__ import annotations

import argparse
import gzip
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import orjson
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "etl" / "processed"
WEB_DATA_DIR = PROJECT_ROOT / "web" / "data"

PARQUET_IN = PROCESSED_DIR / "03_categorized.parquet"

HTTP_URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def _clean_url(url: str | None) -> str | None:
    """Returns a valid http(s) URL string or None."""
    if not url:
        return None
    stripped = url.strip()
    return stripped if HTTP_URL_RE.match(stripped) else None


def _build_search_string(parts: list[str | None]) -> str:
    """Combines unique non-empty trilingual name & place tokens into a lowercased string."""
    seen: set[str] = set()
    tokens: list[str] = []
    for part in parts:
        if not part:
            continue
        lowered = " ".join(part.strip().lower().split())
        if lowered and lowered not in seen:
            seen.add(lowered)
            tokens.append(lowered)
    return " ".join(tokens)


def _format_bytes(num_bytes: int) -> str:
    """Formats a byte count into human-readable KB or MB."""
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:,.1f} KB"
    return f"{num_bytes / (1024 * 1024):,.2f} MB"


def export_web_json(skip_tests: bool = False) -> None:
    """Builds all 4 production JSON files, prints size report, and runs pytest."""
    if not PARQUET_IN.is_file():
        raise FileNotFoundError(f"Missing input Parquet file: {PARQUET_IN}")

    df = (
        pl.read_parquet(PARQUET_IN)
        .sort(
            by=["remarkability", "sitelinks_count", "qid"],
            descending=[True, True, False],
        )
        .unique(subset=["qid"], keep="first", maintain_order=True)
    )

    map_lite: list[list[Any]] = []
    people_details: dict[str, dict[str, Any]] = {}
    locations_map: dict[str, list[Any]] = {}
    zones_set: set[str] = set()
    search_people: list[list[Any]] = []
    category_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for row in df.iter_rows(named=True):
        qid: str = row["qid"]
        lat = round(float(row["lat"]), 5)
        lon = round(float(row["lon"]), 5)

        raw_categories = row["categories"] or [["Other", "Unknown"]]
        categories: list[list[str]] = [
            [str(pair[0]), str(pair[1])] for pair in raw_categories if len(pair) == 2
        ]
        if not categories:
            categories = [["Other", "Unknown"]]

        for main_cat, sub_cat in categories:
            category_counts[main_cat][sub_cat] += 1

        loc_key: str | None = row["loc_key"]
        geo_tree: list[str] = [str(node) for node in (row["geo_tree"] or []) if node]

        # Collect zone filter values from geoTree (country + oblast/special city)
        if len(geo_tree) >= 1:
            zones_set.add(geo_tree[0])
        if len(geo_tree) >= 2:
            zones_set.add(geo_tree[1])

        birthplace_en: str = row["birthplace_en"] or row["birthplace_uk"] or "Unknown"
        region_en: str = row["region_en"] or birthplace_en
        country_en: str = row["country_en"] or (geo_tree[0] if geo_tree else "Ukraine")

        # 1. map_lite tuple: [qid, lat, lon, categories, locKey, geoTree]
        map_lite.append([qid, lat, lon, categories, loc_key, geo_tree])

        # 2. people_details entry (capping occupations to top 5 readable labels)
        occupations_raw = row["occupation_labels"] or []
        occupations = [
            str(occ).strip() for occ in occupations_raw if occ and str(occ).strip()
        ][:5]

        people_details[qid] = {
            "name_en": row["name_en"],
            "name_uk": row["name_uk"],
            "name_ru": row["name_ru"],
            "image": row["image"],
            "birthplace": birthplace_en,
            "occupations": occupations,
            "wiki_en": _clean_url(row["wiki_en"]),
            "wiki_uk": _clean_url(row["wiki_uk"]),
            "wiki_ru": _clean_url(row["wiki_ru"]),
            "remarkability": round(float(row["remarkability"]), 2),
        }

        # 3. search_index locations entry
        if loc_key and loc_key not in locations_map:
            locations_map[loc_key] = [lat, lon, region_en, birthplace_en]

        # 4. search_index people tuple (pre-sorted by remarkability descending)
        default_display_name = (
            row["name_en"] or row["name_uk"] or row["name_ru"] or qid
        ).strip()

        search_string = _build_search_string(
            [
                row["name_en"],
                row["name_uk"],
                row["name_ru"],
                row["birthplace_en"],
                row["birthplace_uk"],
                row["birthplace_ru"],
                row["region_en"],
                row["region_uk"],
                row["region_ru"],
            ]
        )

        search_people.append(
            [
                qid,
                search_string,
                default_display_name,
                loc_key,
                birthplace_en,
                country_en,
            ]
        )

    # Sort topic_map alphabetically by main category and sub-category
    topic_map: dict[str, dict[str, int]] = {
        main_cat: {
            sub_cat: category_counts[main_cat][sub_cat]
            for sub_cat in sorted(category_counts[main_cat].keys())
        }
        for main_cat in sorted(category_counts.keys())
    }

    # Assemble search_index with sorted locations and sorted unique zones
    sorted_locations: dict[str, Any] = {
        loc_key: locations_map[loc_key] for loc_key in sorted(locations_map.keys())
    }
    sorted_locations["zones"] = sorted(zones_set)

    search_index: dict[str, Any] = {
        "locations": sorted_locations,
        "people": search_people,
    }

    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    artifacts: list[tuple[str, Any, int]] = [
        ("map_lite.json", map_lite, len(map_lite)),
        (
            "topic_map.json",
            topic_map,
            sum(len(subs) for subs in topic_map.values()),
        ),
        ("people_details.json", people_details, len(people_details)),
        ("search_index.json", search_index, len(search_people)),
    ]

    print("\n=== T6 Production JSON Payload Budget Report ===")
    print(f"{'Artifact':<22} | {'Records':>9} | {'Raw Size':>11} | {'Gzip Size':>11}")
    print("-" * 62)

    total_raw = 0
    total_gzip = 0

    for filename, payload, count in artifacts:
        raw_bytes = orjson.dumps(payload)
        gz_bytes = gzip.compress(raw_bytes, compresslevel=6)

        out_path = WEB_DATA_DIR / filename
        out_path.write_bytes(raw_bytes)

        total_raw += len(raw_bytes)
        total_gzip += len(gz_bytes)

        print(
            f"{filename:<22} | {count:>9,} | {_format_bytes(len(raw_bytes)):>11} | "
            f"{_format_bytes(len(gz_bytes)):>11}"
        )

    print("-" * 62)
    print(
        f"{'TOTAL':<22} | {len(map_lite):>9,} | {_format_bytes(total_raw):>11} | "
        f"{_format_bytes(total_gzip):>11}"
    )

    if not skip_tests:
        print("\nRunning automated data contract tests against web/data/...")
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/test_web_contracts.py", "-v"],
            cwd=PROJECT_ROOT,
        )
        if result.returncode != 0:
            raise SystemExit(result.returncode)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export minified web/data/*.json artifacts and verify contracts."
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Skip running pytest after writing JSON artifacts.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    export_web_json(skip_tests=args.skip_tests)
