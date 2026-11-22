"""
===============================================================================
01_extract.py — Resumable, Partitioned Wikidata SPARQL Extractor
-------------------------------------------------------------------------------
Fetches notable humans (wd:Q5) who either:
  1. Were born in Ukraine (`wdt:P19 / wdt:P131* wd:Q212`), OR
  2. Hold Ukrainian citizenship (`wdt:P27 wd:Q212`),
and possess at least one English, Ukrainian, or Russian Wikipedia article.

Architecture Highlights:
  - Query Partitioning: Splits queries into birth-year (P569) slices and
    separates `born` vs. `citizen` patterns so each SPARQL request completes
    well under Wikidata's 60-second execution timeout.
  - Checkpointing: Writes raw SPARQL JSON responses to `etl/raw/` immediately
    after each slice completes; re-running skips existing non-empty slices
    unless `--force` is specified.
  - Rate-Limit Compliance: Sends a descriptive Wikimedia User-Agent, pauses
    between requests, and respects `Retry-After` headers with exponential
    backoff on HTTP 429 / 503 / 504 responses.
  - Parquet Consolidation: Normalizes Q-IDs, WKT `Point(lon lat)` coordinates,
    and Commons filenames into `etl/processed/01_raw_people.parquet`.
===============================================================================
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import httpx
import orjson
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "etl" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "etl" / "processed"
PARQUET_OUT = PROCESSED_DIR / "01_raw_people.parquet"

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = (
    "UkraineNotablesMapETL/1.0 "
    "(https://github.com/example/ukr_notables; educational historical map project) "
    "httpx"
)

# Regex for Wikidata WKT coordinate literals: "Point(30.5236 50.45)" -> (lon, lat)
WKT_POINT_RE = re.compile(
    r"^Point\(\s*([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s*\)$",
    re.IGNORECASE,
)

PARQUET_SCHEMA = pl.Schema(
    {
        "qid": pl.String,
        "source_query": pl.String,
        "slice_label": pl.String,
        "name_en": pl.String,
        "name_uk": pl.String,
        "name_ru": pl.String,
        "image": pl.String,
        "wiki_en": pl.String,
        "wiki_uk": pl.String,
        "wiki_ru": pl.String,
        "birthplace_qid": pl.String,
        "birthplace_en": pl.String,
        "birthplace_uk": pl.String,
        "lat": pl.Float64,
        "lon": pl.Float64,
        "occupation_qid": pl.String,
        "occupation_en": pl.String,
        "sitelinks_count": pl.Int64,
    }
)


@dataclass(frozen=True)
class QuerySlice:
    """Represents one deterministic birth-date partition for SPARQL querying."""

    label: str
    sparql_filter: str


def build_query_slices() -> list[QuerySlice]:
    """Generates deterministic birth-year slices from pre-1800 through today."""
    current_year = datetime.now(UTC).year
    slices: list[QuerySlice] = [
        QuerySlice(
            label="pre_1800",
            sparql_filter=(
                "?person wdt:P569 ?birthDate .\n"
                '  FILTER(?birthDate < "1800-01-01T00:00:00Z"^^xsd:dateTime)'
            ),
        ),
        QuerySlice(
            label="1800_1849",
            sparql_filter=(
                "?person wdt:P569 ?birthDate .\n"
                '  FILTER(?birthDate >= "1800-01-01T00:00:00Z"^^xsd:dateTime && '
                '?birthDate < "1850-01-01T00:00:00Z"^^xsd:dateTime)'
            ),
        ),
        QuerySlice(
            label="1850_1899",
            sparql_filter=(
                "?person wdt:P569 ?birthDate .\n"
                '  FILTER(?birthDate >= "1850-01-01T00:00:00Z"^^xsd:dateTime && '
                '?birthDate < "1900-01-01T00:00:00Z"^^xsd:dateTime)'
            ),
        ),
    ]

    # 5-year intervals from 1900 to present keep dense 20th-century slices fast
    for start_year in range(1900, current_year + 1, 5):
        end_year = min(start_year + 5, current_year + 1)
        slices.append(
            QuerySlice(
                label=f"{start_year}_{end_year - 1}",
                sparql_filter=(
                    "?person wdt:P569 ?birthDate .\n"
                    f'  FILTER(?birthDate >= "{start_year}-01-01T00:00:00Z"^^xsd:dateTime && '
                    f'?birthDate < "{end_year}-01-01T00:00:00Z"^^xsd:dateTime)'
                ),
            )
        )

    slices.append(
        QuerySlice(
            label="no_birthdate",
            sparql_filter="FILTER NOT EXISTS { ?person wdt:P569 ?birthDate }",
        )
    )
    return slices


def build_sparql_query(query_slice: QuerySlice, source_type: str) -> str:
    """Constructs a SPARQL query for either `born` or `citizen` within a date slice."""
    if source_type == "born":
        core_pattern = "?person wdt:P19 / wdt:P131* wd:Q212 ."
    elif source_type == "citizen":
        core_pattern = "?person wdt:P27 wd:Q212 ."
    else:
        raise ValueError(f"Unsupported source_type: {source_type!r}")

    return f"""
SELECT DISTINCT
  ?person
  ?name_en ?name_uk ?name_ru
  ?image
  ?wiki_en ?wiki_uk ?wiki_ru
  ?birthplace ?birthplace_en ?birthplace_uk
  ?coord
  ?occupation ?occupation_en
  ?sitelinks
WHERE {{
  ?person wdt:P31 wd:Q5 .
  {core_pattern}
  ?person wikibase:sitelinks ?sitelinks .
  FILTER(?sitelinks >= 1)
  {query_slice.sparql_filter}

  OPTIONAL {{
    ?wiki_en schema:about ?person ;
             schema:isPartOf <https://en.wikipedia.org/> .
  }}
  OPTIONAL {{
    ?wiki_uk schema:about ?person ;
             schema:isPartOf <https://uk.wikipedia.org/> .
  }}
  OPTIONAL {{
    ?wiki_ru schema:about ?person ;
             schema:isPartOf <https://ru.wikipedia.org/> .
  }}
  FILTER(BOUND(?wiki_en) || BOUND(?wiki_uk) || BOUND(?wiki_ru))

  OPTIONAL {{ ?person rdfs:label ?name_en . FILTER(LANG(?name_en) = "en") }}
  OPTIONAL {{ ?person rdfs:label ?name_uk . FILTER(LANG(?name_uk) = "uk") }}
  OPTIONAL {{ ?person rdfs:label ?name_ru . FILTER(LANG(?name_ru) = "ru") }}
  OPTIONAL {{ ?person wdt:P18 ?image . }}

  OPTIONAL {{
    ?person wdt:P19 ?birthplace .
    OPTIONAL {{ ?birthplace rdfs:label ?birthplace_en . FILTER(LANG(?birthplace_en) = "en") }}
    OPTIONAL {{ ?birthplace rdfs:label ?birthplace_uk . FILTER(LANG(?birthplace_uk) = "uk") }}
    OPTIONAL {{ ?birthplace wdt:P625 ?coord . }}
  }}

  OPTIONAL {{
    ?person wdt:P106 ?occupation .
    OPTIONAL {{ ?occupation rdfs:label ?occupation_en . FILTER(LANG(?occupation_en) = "en") }}
  }}
}}
""".strip()


def _parse_sparql_json(raw_bytes: bytes) -> dict[str, Any]:
    """Parses SPARQL JSON with orjson first, falling back to non-strict stdlib json
    when Wikidata emits unescaped control characters inside string literals."""
    try:
        return orjson.loads(raw_bytes)
    except orjson.JSONDecodeError:
        return json.loads(raw_bytes.decode("utf-8", errors="replace"), strict=False)


def execute_sparql_with_retries(
    client: httpx.Client,
    sparql: str,
    max_retries: int = 5,
) -> dict[str, Any]:
    """Executes a SPARQL POST query with Retry-After and exponential backoff."""
    retryable_statuses = {429, 502, 503, 504}

    for attempt in range(1, max_retries + 1):
        try:
            response = client.post(
                SPARQL_ENDPOINT,
                data={"query": sparql, "format": "json"},
            )

            if response.status_code == 200:
                return _parse_sparql_json(response.content)

            if response.status_code in retryable_statuses and attempt < max_retries:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = (
                    float(retry_after)
                    if retry_after and retry_after.isdigit()
                    else min(60.0, 2.0**attempt)
                )
                print(
                    f"  [HTTP {response.status_code}] Throttled/busy. "
                    f"Retrying in {wait_seconds:.1f}s (attempt {attempt}/{max_retries})..."
                )
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()

        except (
            httpx.TimeoutException,
            httpx.TransportError,
            json.JSONDecodeError,
        ) as exc:
            if attempt >= max_retries:
                raise
            wait_seconds = min(60.0, 2.0**attempt)
            print(
                f"  [Transient {type(exc).__name__}] Retrying in {wait_seconds:.1f}s "
                f"(attempt {attempt}/{max_retries})..."
            )
            time.sleep(wait_seconds)

    raise RuntimeError("Exceeded maximum SPARQL retries")


def _extract_qid(uri: str | None) -> str | None:
    """Converts 'http://www.wikidata.org/entity/Q134958' -> 'Q134958'."""
    if not uri:
        return None
    tail = uri.rsplit("/", 1)[-1]
    return tail if tail.startswith("Q") and tail[1:].isdigit() else None


def _extract_image_filename(uri: str | None) -> str | None:
    """Converts Commons Special:FilePath URI into a clean decoded filename."""
    if not uri:
        return None
    filename = unquote(uri.rsplit("/", 1)[-1]).strip()
    if filename.startswith("File:"):
        filename = filename[5:].strip()
    return filename or None


def _parse_wkt_point(wkt: str | None) -> tuple[float | None, float | None]:
    """Parses Wikidata WKT 'Point(lon lat)' into (lat, lon) float tuple."""
    if not wkt:
        return None, None
    match = WKT_POINT_RE.match(wkt.strip())
    if not match:
        return None, None
    lon = float(match.group(1))
    lat = float(match.group(2))
    return lat, lon


def _binding_value(binding: dict[str, Any], key: str) -> str | None:
    """Safely extracts string value from a SPARQL JSON binding cell."""
    cell = binding.get(key)
    if not isinstance(cell, dict):
        return None
    val = cell.get("value")
    return str(val) if val is not None else None


def consolidate_raw_slices(raw_dir: Path, parquet_path: Path) -> pl.DataFrame:
    """Reads all checkpointed JSON slices and writes `01_raw_people.parquet`."""
    slice_files = sorted(raw_dir.glob("slice_*.json"))
    if not slice_files:
        raise FileNotFoundError(f"No slice_*.json files found in {raw_dir}")

    rows: list[dict[str, Any]] = []

    for file_path in slice_files:
        payload = orjson.loads(file_path.read_bytes())
        meta = payload.get("_meta", {})
        source_query = meta.get("source_query", "unknown")
        slice_label = meta.get("slice_label", file_path.stem)

        bindings = payload.get("results", {}).get("bindings", [])
        for b in bindings:
            qid = _extract_qid(_binding_value(b, "person"))
            if not qid:
                continue

            lat, lon = _parse_wkt_point(_binding_value(b, "coord"))
            sitelinks_raw = _binding_value(b, "sitelinks")

            rows.append(
                {
                    "qid": qid,
                    "source_query": source_query,
                    "slice_label": slice_label,
                    "name_en": _binding_value(b, "name_en"),
                    "name_uk": _binding_value(b, "name_uk"),
                    "name_ru": _binding_value(b, "name_ru"),
                    "image": _extract_image_filename(_binding_value(b, "image")),
                    "wiki_en": _binding_value(b, "wiki_en"),
                    "wiki_uk": _binding_value(b, "wiki_uk"),
                    "wiki_ru": _binding_value(b, "wiki_ru"),
                    "birthplace_qid": _extract_qid(_binding_value(b, "birthplace")),
                    "birthplace_en": _binding_value(b, "birthplace_en"),
                    "birthplace_uk": _binding_value(b, "birthplace_uk"),
                    "lat": lat,
                    "lon": lon,
                    "occupation_qid": _extract_qid(_binding_value(b, "occupation")),
                    "occupation_en": _binding_value(b, "occupation_en"),
                    "sitelinks_count": int(sitelinks_raw) if sitelinks_raw else 0,
                }
            )

    df = pl.DataFrame(rows, schema=PARQUET_SCHEMA)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(parquet_path, compression="zstd")
    return df


def run_extraction(
    force: bool = False,
    test_slice: bool = False,
    delay_seconds: float = 1.5,
) -> None:
    """Runs resumable SPARQL extraction across all configured slices."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    all_slices = build_query_slices()
    if test_slice:
        # 1850_1899 is fast (< 10s) and includes major Ukrainian literary/historical figures
        all_slices = [s for s in all_slices if s.label == "1850_1899"]

    tasks = [
        (query_slice, source_type)
        for query_slice in all_slices
        for source_type in ("born", "citizen")
    ]

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/sparql-results+json",
    }

    print(f"Starting Wikidata extraction ({len(tasks)} total query tasks)...")

    with httpx.Client(headers=headers, timeout=90.0) as client:
        for idx, (query_slice, source_type) in enumerate(tasks, start=1):
            out_file = RAW_DIR / f"slice_{query_slice.label}_{source_type}.json"

            if not force and out_file.is_file() and out_file.stat().st_size > 0:
                print(
                    f"[{idx}/{len(tasks)}] SKIP {out_file.name} (already checkpointed)"
                )
                continue

            print(f"[{idx}/{len(tasks)}] FETCH {out_file.name} ...")
            t0 = time.perf_counter()
            sparql = build_sparql_query(query_slice, source_type)
            result = execute_sparql_with_retries(client, sparql)

            # Attach slice provenance metadata alongside raw SPARQL bindings
            result["_meta"] = {
                "slice_label": query_slice.label,
                "source_query": source_type,
                "fetched_at_utc": datetime.now(UTC).isoformat(),
            }

            row_count = len(result.get("results", {}).get("bindings", []))
            out_file.write_bytes(orjson.dumps(result))
            elapsed = time.perf_counter() - t0
            print(f"  -> Saved {row_count:,} bindings in {elapsed:.1f}s")

            if idx < len(tasks):
                time.sleep(delay_seconds)

    print("\nConsolidating raw JSON slices into Parquet...")
    df = consolidate_raw_slices(RAW_DIR, PARQUET_OUT)
    unique_qids = df.select(pl.col("qid").n_unique()).item()
    print(
        f"Done! Wrote {df.height:,} raw rows ({unique_qids:,} unique Q-IDs) "
        f"to {PARQUET_OUT.relative_to(PROJECT_ROOT)}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Partitioned, resumable Wikidata SPARQL extractor for Ukraine Notables."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch slices even if non-empty checkpoint files exist in etl/raw/.",
    )
    parser.add_argument(
        "--test-slice",
        action="store_true",
        help="Run only the 1850_1899 slice (born + citizen) to verify the pipeline quickly.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Polite pause in seconds between consecutive SPARQL requests (default: 1.5).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_extraction(
        force=args.force,
        test_slice=args.test_slice,
        delay_seconds=args.delay,
    )
