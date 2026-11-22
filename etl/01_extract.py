"""
===============================================================================
01_extract.py — Resumable, Time-Sliced Wikidata SPARQL Extractor (T3 + T12)
-------------------------------------------------------------------------------
Queries the Wikidata Query Service (WDQS) in chronological slices across four
explicit provenance channels so every person has verifiable origin metadata:
  1. `born`         : P19 (birthplace) located inside modern Ukraine (Q212)
  2. `citizen`      : P27 (citizenship) = Ukraine (Q212)
  3. `heritage`     : P172 (ethnic group) = Ukrainians (Q44806)
  4. `encyclopedia` : Listed in Encyclopedia of Modern Ukraine (P4613) or
                      Internet Encyclopedia of Ukraine (P9070)

Also extracts P569 (birth date), P570 (death date), and secondary authority
identifiers (P4613 ЕСУ, P9070 IEU, P214 VIAF).
===============================================================================
"""

from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass
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

WDQS_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = (
    "NotableUkrainiansMap/1.1 (https://github.com/danostr/notable-ukrainians-map; "
    "educational historical geography project)"
)

ALL_SOURCES: tuple[str, ...] = ("born", "citizen", "heritage", "encyclopedia")

WKT_POINT_RE = re.compile(
    r"Point\(\s*([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s*\)",
    re.IGNORECASE,
)

INVALID_JSON_ESCAPE_RE = re.compile(r'\\(?!["\\/bfnrtu])')


@dataclass(frozen=True)
class QuerySlice:
    label: str
    date_filter: str


def build_query_slices() -> list[QuerySlice]:
    """Partitions human records by P569 birth date so WDQS never times out."""
    slices: list[QuerySlice] = [
        QuerySlice(
            label="pre_1800",
            date_filter='FILTER(?birth_date < "1800-01-01T00:00:00Z"^^xsd:dateTime)',
        )
    ]

    ranges = [
        (1800, 1849),
        (1850, 1879),
        (1880, 1899),
        (1900, 1919),
        (1920, 1939),
        (1940, 1959),
        (1960, 1979),
        (1980, 1994),
        (1995, 2030),
    ]
    for start_year, end_year in ranges:
        slices.append(
            QuerySlice(
                label=f"{start_year}_{end_year}",
                date_filter=(
                    f'FILTER(?birth_date >= "{start_year}-01-01T00:00:00Z"^^xsd:dateTime && '
                    f'?birth_date < "{end_year + 1}-01-01T00:00:00Z"^^xsd:dateTime)'
                ),
            )
        )

    slices.append(
        QuerySlice(
            label="no_birthdate",
            date_filter="FILTER(!BOUND(?birth_date))",
        )
    )
    return slices


def build_sparql_query(query_slice: QuerySlice, source: str) -> str:
    """Builds a parameterized SPARQL query for one time slice and provenance source."""
    if source == "born":
        cohort_clause = """
  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 ?birthplace .
  ?birthplace wdt:P131* wd:Q212 .
"""
    elif source == "citizen":
        cohort_clause = """
  ?person wdt:P31 wd:Q5 .
  ?person wdt:P27 wd:Q212 .
  OPTIONAL { ?person wdt:P19 ?birthplace . }
"""
    elif source == "heritage":
        cohort_clause = """
  ?person wdt:P31 wd:Q5 .
  ?person wdt:P172 wd:Q44806 .
  OPTIONAL { ?person wdt:P19 ?birthplace . }
"""
    elif source == "encyclopedia":
        cohort_clause = """
  { ?person wdt:P4613 ?esu_anchor . }
  UNION
  { ?person wdt:P9070 ?ieu_anchor . }
  ?person wdt:P31 wd:Q5 .
  OPTIONAL { ?person wdt:P19 ?birthplace . }
"""
    else:
        raise ValueError(f"Unknown query source: {source!r}")

    if query_slice.label == "no_birthdate":
        date_clause = f"""
  OPTIONAL {{ ?person wdt:P569 ?birth_date . }}
  {query_slice.date_filter}
"""
    else:
        date_clause = f"""
  ?person wdt:P569 ?birth_date .
  {query_slice.date_filter}
"""

    return f"""
SELECT DISTINCT
  ?person ?name_en ?name_uk ?name_ru
  ?image ?wiki_en ?wiki_uk ?wiki_ru
  ?birthplace ?birthplace_en ?birthplace_uk ?birthplace_ru ?coord
  ?occupation ?occupation_en ?sitelinks
  ?birth_date ?death_date
  ?esu_id ?ieu_id ?viaf_id
WHERE {{
{cohort_clause}
{date_clause}

  # Require at least one Wikipedia sitelink to exclude empty stub items
  ?person wikibase:sitelinks ?sitelinks .
  FILTER(?sitelinks >= 1)

  OPTIONAL {{ ?person rdfs:label ?name_en . FILTER(LANG(?name_en) = "en") }}
  OPTIONAL {{ ?person rdfs:label ?name_uk . FILTER(LANG(?name_uk) = "uk") }}
  OPTIONAL {{ ?person rdfs:label ?name_ru . FILTER(LANG(?name_ru) = "ru") }}

  OPTIONAL {{ ?person wdt:P18 ?image . }}
  OPTIONAL {{ ?person wdt:P570 ?death_date . }}
  OPTIONAL {{ ?person wdt:P4613 ?esu_id . }}
  OPTIONAL {{ ?person wdt:P9070 ?ieu_id . }}
  OPTIONAL {{ ?person wdt:P214 ?viaf_id . }}

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

  OPTIONAL {{
    ?birthplace wdt:P625 ?coord .
    OPTIONAL {{ ?birthplace rdfs:label ?birthplace_en . FILTER(LANG(?birthplace_en) = "en") }}
    OPTIONAL {{ ?birthplace rdfs:label ?birthplace_uk . FILTER(LANG(?birthplace_uk) = "uk") }}
    OPTIONAL {{ ?birthplace rdfs:label ?birthplace_ru . FILTER(LANG(?birthplace_ru) = "ru") }}
  }}

  OPTIONAL {{
    ?person wdt:P106 ?occupation .
    OPTIONAL {{ ?occupation rdfs:label ?occupation_en . FILTER(LANG(?occupation_en) = "en") }}
  }}
}}
""".strip()


def _qid_from_uri(uri: str | None) -> str | None:
    if not uri:
        return None
    return uri.rsplit("/", 1)[-1]


def _extract_image_filename(image_uri: str | None) -> str | None:
    if not image_uri:
        return None
    raw = image_uri.rsplit("/", 1)[-1]
    if raw.startswith("File:"):
        raw = raw[5:]
    decoded = unquote(raw).strip()
    return decoded or None


def _parse_wkt_point(wkt: str | None) -> tuple[float | None, float | None]:
    if not wkt:
        return None, None
    match = WKT_POINT_RE.search(wkt)
    if not match:
        return None, None
    lon = float(match.group(1))
    lat = float(match.group(2))
    return lat, lon


def _binding_val(binding: dict[str, Any], key: str) -> str | None:
    cell = binding.get(key)
    if not cell:
        return None
    val = cell.get("value")
    return str(val).strip() if val is not None else None


def _parse_sparql_json(raw_bytes: bytes) -> dict[str, Any]:
    """Parses WDQS JSON even when literals contain stray control chars or escapes."""
    try:
        return orjson.loads(raw_bytes)
    except orjson.JSONDecodeError:
        import json

        text = raw_bytes.decode("utf-8", errors="replace")
        try:
            return json.loads(text, strict=False)
        except json.JSONDecodeError:
            sanitized = INVALID_JSON_ESCAPE_RE.sub(r"\\\\", text)
            return json.loads(sanitized, strict=False)


def fetch_slice_with_retries(
    client: httpx.Client,
    sparql: str,
    slice_name: str,
    max_retries: int = 5,
) -> dict[str, Any]:
    """Executes a SPARQL query with exponential backoff for 429/502/503/504 errors."""
    backoff = 4.0
    for attempt in range(1, max_retries + 1):
        try:
            response = client.post(
                WDQS_ENDPOINT,
                data={"query": sparql, "format": "json"},
                headers={
                    "Accept": "application/sparql-results+json",
                    "User-Agent": USER_AGENT,
                },
                timeout=90.0,
            )
            if response.status_code == 200:
                return _parse_sparql_json(response.content)

            if response.status_code in {429, 502, 503, 504}:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = (
                    float(retry_after)
                    if retry_after and retry_after.isdigit()
                    else backoff
                )
                print(
                    f"  [!] HTTP {response.status_code} on {slice_name} "
                    f"(attempt {attempt}/{max_retries}) — sleeping {wait_seconds:.1f}s..."
                )
                time.sleep(wait_seconds)
                backoff = min(backoff * 2.0, 60.0)
                continue

            response.raise_for_status()
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            if attempt == max_retries:
                raise RuntimeError(
                    f"Failed slice {slice_name} after {max_retries} attempts"
                ) from exc
            print(
                f"  [!] Network timeout/error on {slice_name} ({exc.__class__.__name__}) "
                f"(attempt {attempt}/{max_retries}) — sleeping {backoff:.1f}s..."
            )
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)

    raise RuntimeError(f"Exhausted retries for slice {slice_name}")


def consolidate_raw_slices(raw_dir: Path, parquet_out: Path) -> pl.DataFrame:
    """Parses all cached `slice_*.json` files into a typed Polars DataFrame."""
    slice_files = sorted(raw_dir.glob("slice_*.json"))
    if not slice_files:
        raise FileNotFoundError(f"No slice_*.json files found in {raw_dir}")

    rows: list[dict[str, Any]] = []
    for path in slice_files:
        payload = orjson.loads(path.read_bytes())
        meta = payload.get("_meta", {})
        slice_label = meta.get("slice_label", path.stem)
        source_query = meta.get("source_query", "unknown")
        bindings = payload.get("results", {}).get("bindings", [])

        for b in bindings:
            qid = _qid_from_uri(_binding_val(b, "person"))
            if not qid:
                continue

            lat, lon = _parse_wkt_point(_binding_val(b, "coord"))
            sitelinks_raw = _binding_val(b, "sitelinks")
            sitelinks = (
                int(sitelinks_raw) if sitelinks_raw and sitelinks_raw.isdigit() else 0
            )

            rows.append(
                {
                    "qid": qid,
                    "name_en": _binding_val(b, "name_en"),
                    "name_uk": _binding_val(b, "name_uk"),
                    "name_ru": _binding_val(b, "name_ru"),
                    "image": _extract_image_filename(_binding_val(b, "image")),
                    "wiki_en": _binding_val(b, "wiki_en"),
                    "wiki_uk": _binding_val(b, "wiki_uk"),
                    "wiki_ru": _binding_val(b, "wiki_ru"),
                    "birthplace_qid": _qid_from_uri(_binding_val(b, "birthplace")),
                    "birthplace_en": _binding_val(b, "birthplace_en"),
                    "birthplace_uk": _binding_val(b, "birthplace_uk"),
                    "birthplace_ru": _binding_val(b, "birthplace_ru"),
                    "lat": lat,
                    "lon": lon,
                    "occupation_qid": _qid_from_uri(_binding_val(b, "occupation")),
                    "occupation_en": _binding_val(b, "occupation_en"),
                    "sitelinks_count": sitelinks,
                    "birth_date": _binding_val(b, "birth_date"),
                    "death_date": _binding_val(b, "death_date"),
                    "esu_id": _binding_val(b, "esu_id"),
                    "ieu_id": _binding_val(b, "ieu_id"),
                    "viaf_id": _binding_val(b, "viaf_id"),
                    "source_query": source_query,
                    "slice_label": slice_label,
                }
            )

    schema = {
        "qid": pl.String,
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
        "birthplace_ru": pl.String,
        "lat": pl.Float64,
        "lon": pl.Float64,
        "occupation_qid": pl.String,
        "occupation_en": pl.String,
        "sitelinks_count": pl.Int64,
        "birth_date": pl.String,
        "death_date": pl.String,
        "esu_id": pl.String,
        "ieu_id": pl.String,
        "viaf_id": pl.String,
        "source_query": pl.String,
        "slice_label": pl.String,
    }

    df = pl.DataFrame(rows, schema=schema)
    parquet_out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(parquet_out, compression="zstd")
    return df


def run_extraction(
    refresh: bool = False,
    consolidate_only: bool = False,
    sources: list[str] | None = None,
) -> None:
    """Runs incremental slice extraction and consolidates into 01_raw_people.parquet."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    active_sources = sources or list(ALL_SOURCES)
    slices = build_query_slices()

    if not consolidate_only:
        total_tasks = len(slices) * len(active_sources)
        completed = 0
        print(
            f"Starting Wikidata SPARQL extraction ({len(slices)} slices × "
            f"{len(active_sources)} sources [{', '.join(active_sources)}] = {total_tasks} queries)..."
        )

        with httpx.Client(http2=False) as client:
            for source in active_sources:
                for q_slice in slices:
                    completed += 1
                    slice_file = RAW_DIR / f"slice_{q_slice.label}_{source}.json"
                    tag = f"[{completed}/{total_tasks}] {q_slice.label} ({source})"

                    if slice_file.exists() and not refresh:
                        print(f"  [skip] {tag} — cached")
                        continue

                    print(f"  [fetch] {tag}...")
                    sparql = build_sparql_query(q_slice, source)
                    data = fetch_slice_with_retries(client, sparql, tag)
                    bindings_count = len(data.get("results", {}).get("bindings", []))
                    data["_meta"] = {
                        "slice_label": q_slice.label,
                        "source_query": source,
                        "bindings_count": bindings_count,
                    }
                    slice_file.write_bytes(orjson.dumps(data))
                    print(f"  [ok]    {tag} -> {bindings_count:,} raw bindings saved")
                    time.sleep(1.5)

    print("\nConsolidating raw JSON slices into Parquet...")
    df = consolidate_raw_slices(RAW_DIR, PARQUET_OUT)
    unique_qids = df["qid"].n_unique()
    print(
        f"Saved {df.height:,} raw bindings ({unique_qids:,} unique Q-IDs) -> {PARQUET_OUT}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract notable Ukrainians from Wikidata SPARQL."
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-download existing slice_*.json files instead of skipping cached slices.",
    )
    parser.add_argument(
        "--consolidate-only",
        action="store_true",
        help="Skip network requests and rebuild 01_raw_people.parquet from cached slices.",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=list(ALL_SOURCES),
        default=list(ALL_SOURCES),
        help="Subset of provenance sources to query (default: born citizen heritage encyclopedia).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_extraction(
        refresh=args.refresh,
        consolidate_only=args.consolidate_only,
        sources=args.sources,
    )
