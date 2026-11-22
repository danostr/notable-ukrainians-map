"""
===============================================================================
01_extract.py — Resumable, Time-Sliced Wikidata SPARQL Extractor (T3 + T12)
-------------------------------------------------------------------------------
Queries the Wikidata Query Service (WDQS) across four explicit provenance
channels so every person has verifiable origin metadata:
  1. `born`         : P19 (birthplace) located inside modern Ukraine (Q212),
                      partitioned into chronological slices to avoid timeouts
  2. `citizen`      : P27 (citizenship) = Ukraine (Q212) or historical/Soviet
                      Ukrainian states (Q133356 UkrSSR, Q243610 UPR, Q457167 WUPR)
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
    "NotableUkrainiansMap/1.2 (https://github.com/danostr/notable-ukrainians-map; "
    "educational historical geography project)"
)

SCHEMA_VERSION = 2
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
    start_year: int | None = None
    end_year: int | None = None


def _make_year_slice(start_year: int, end_year: int) -> QuerySlice:
    return QuerySlice(
        label=f"{start_year}_{end_year}",
        date_filter=(
            f'FILTER(?birth_date >= "{start_year}-01-01T00:00:00Z"^^xsd:dateTime && '
            f'?birth_date < "{end_year + 1}-01-01T00:00:00Z"^^xsd:dateTime)'
        ),
        start_year=start_year,
        end_year=end_year,
    )


def build_query_slices(source: str = "born") -> list[QuerySlice]:
    """
    Partitions `born` records by P569 birth date so WDQS never times out.
    Smaller cohorts (`citizen`, `heritage`, `encyclopedia`) execute fastest as a
    single direct index lookup (`all_years`) without scanning P569 date ranges.
    """
    if source != "born":
        return [QuerySlice(label="all_years", date_filter="")]

    slices: list[QuerySlice] = [
        QuerySlice(
            label="pre_1700",
            date_filter='FILTER(?birth_date < "1700-01-01T00:00:00Z"^^xsd:dateTime)',
        )
    ]

    ranges = [
        (1700, 1799),
        (1800, 1849),
        (1850, 1869),
        (1870, 1879),
        *[(y, y + 4) for y in range(1880, 2000, 5)],
        (2000, 2026),
    ]
    for start_year, end_year in ranges:
        slices.append(_make_year_slice(start_year, end_year))

    slices.append(
        QuerySlice(
            label="no_birthdate",
            date_filter="FILTER NOT EXISTS { ?person wdt:P569 ?any_birth_date . }",
        )
    )
    return slices


def build_sparql_query(query_slice: QuerySlice, source: str) -> str:
    """
    Builds a Blazegraph-optimized SPARQL query using a named `%base` subquery
    to prevent Cartesian multiplication across optional labels and occupations.
    """
    is_unsliced = query_slice.label in {"no_birthdate", "all_years"}

    if is_unsliced:
        base_select_vars = "?person ?birthplace ?coord ?sitelinks"
        date_clause = (
            f"    {query_slice.date_filter}" if query_slice.date_filter else ""
        )
        outer_birth_date = "  OPTIONAL { ?person wdt:P569 ?birth_date . }"
    else:
        base_select_vars = "?person ?birthplace ?coord ?birth_date ?sitelinks"
        date_clause = (
            f"    ?person wdt:P569 ?birth_date .\n    {query_slice.date_filter}"
        )
        outer_birth_date = ""

    if source == "born":
        base_body = f"""
    ?birthplace wdt:P131* wd:Q212 ;
                wdt:P625 ?coord .
    ?person wdt:P19 ?birthplace ;
            wdt:P31 wd:Q5 ;
            wikibase:sitelinks ?sitelinks .
    FILTER(?sitelinks >= 1)
{date_clause}
"""
    elif source == "citizen":
        base_body = f"""
    VALUES ?country {{ wd:Q212 wd:Q133356 wd:Q243610 wd:Q457167 }}
    ?person wdt:P27 ?country ;
            wdt:P31 wd:Q5 ;
            wikibase:sitelinks ?sitelinks .
    FILTER(?sitelinks >= 1)
{date_clause}
    OPTIONAL {{
      ?person wdt:P19 ?birthplace .
      ?birthplace wdt:P625 ?coord .
    }}
"""
    elif source == "heritage":
        base_body = f"""
    ?person wdt:P172 wd:Q44806 ;
            wdt:P31 wd:Q5 ;
            wikibase:sitelinks ?sitelinks .
    FILTER(?sitelinks >= 1)
{date_clause}
    OPTIONAL {{
      ?person wdt:P19 ?birthplace .
      ?birthplace wdt:P625 ?coord .
    }}
"""
    elif source == "encyclopedia":
        base_body = f"""
    {{ ?person wdt:P4613 ?esu_anchor . }}
    UNION
    {{ ?person wdt:P9070 ?ieu_anchor . }}
    ?person wdt:P31 wd:Q5 ;
            wikibase:sitelinks ?sitelinks .
    FILTER(?sitelinks >= 1)
{date_clause}
    OPTIONAL {{
      ?person wdt:P19 ?birthplace .
      ?birthplace wdt:P625 ?coord .
    }}
"""
    else:
        raise ValueError(f"Unknown query source: {source!r}")

    return f"""
SELECT DISTINCT
  ?person ?name_en ?name_uk ?name_ru
  ?image ?wiki_en ?wiki_uk ?wiki_ru
  ?birthplace ?birthplace_en ?birthplace_uk ?birthplace_ru
  ?coord ?occupation ?occupation_en ?sitelinks
  ?birth_date ?death_date ?esu_id ?ieu_id ?viaf_id
WITH {{
  SELECT DISTINCT {base_select_vars} WHERE {{
{base_body.rstrip()}
  }}
}} AS %base
WHERE {{
  INCLUDE %base
{outer_birth_date}

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
    ?birthplace rdfs:label ?birthplace_en . FILTER(LANG(?birthplace_en) = "en")
  }}
  OPTIONAL {{
    ?birthplace rdfs:label ?birthplace_uk . FILTER(LANG(?birthplace_uk) = "uk")
  }}
  OPTIONAL {{
    ?birthplace rdfs:label ?birthplace_ru . FILTER(LANG(?birthplace_ru) = "ru")
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
    max_retries: int = 4,
) -> dict[str, Any]:
    """Executes a SPARQL query with exponential backoff for 429/500/502/503/504 errors."""
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

            if response.status_code in {429, 500, 502, 503, 504}:
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
                backoff = min(backoff * 2.0, 30.0)
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
            backoff = min(backoff * 2.0, 30.0)

    raise RuntimeError(f"Exhausted retries for slice {slice_name}")


def _is_valid_cached_slice(slice_file: Path) -> bool:
    """Checks that a cached slice exists, is non-empty, and matches SCHEMA_VERSION."""
    if not slice_file.is_file() or slice_file.stat().st_size == 0:
        return False
    try:
        payload = orjson.loads(slice_file.read_bytes())
        return payload.get("_meta", {}).get("schema_version") == SCHEMA_VERSION
    except Exception:
        return False


def _fetch_and_save_slice(
    client: httpx.Client,
    q_slice: QuerySlice,
    source: str,
    tag: str,
    refresh: bool,
) -> None:
    """Fetches a slice (auto-bisecting year ranges on WDQS timeout) and writes JSON."""
    slice_file = RAW_DIR / f"slice_{q_slice.label}_{source}.json"

    if not refresh and _is_valid_cached_slice(slice_file):
        print(f"  [skip] {tag} — cached (v{SCHEMA_VERSION})")
        return

    print(f"  [fetch] {tag}...")
    sparql = build_sparql_query(q_slice, source)
    can_split = (
        q_slice.start_year is not None
        and q_slice.end_year is not None
        and q_slice.start_year < q_slice.end_year
    )

    try:
        data = fetch_slice_with_retries(
            client,
            sparql,
            tag,
            max_retries=2 if can_split else 5,
        )
    except (RuntimeError, httpx.HTTPStatusError):
        if not can_split:
            raise
        assert q_slice.start_year is not None and q_slice.end_year is not None
        mid = (q_slice.start_year + q_slice.end_year) // 2
        left = _make_year_slice(q_slice.start_year, mid)
        right = _make_year_slice(mid + 1, q_slice.end_year)
        print(
            f"  [split] {tag} timed out; bisecting into {left.label} & {right.label}..."
        )
        if slice_file.exists():
            slice_file.unlink()
        _fetch_and_save_slice(client, left, source, f"{tag} -> {left.label}", refresh)
        _fetch_and_save_slice(client, right, source, f"{tag} -> {right.label}", refresh)
        return

    bindings_count = len(data.get("results", {}).get("bindings", []))
    data["_meta"] = {
        "schema_version": SCHEMA_VERSION,
        "slice_label": q_slice.label,
        "source_query": source,
        "bindings_count": bindings_count,
    }
    slice_file.write_bytes(orjson.dumps(data))
    print(f"  [ok]    {tag} -> {bindings_count:,} raw bindings saved")
    time.sleep(1.2)


def consolidate_raw_slices(raw_dir: Path, parquet_out: Path) -> pl.DataFrame:
    """Parses all v2 cached `slice_*.json` files into a typed Polars DataFrame."""
    slice_files = sorted(raw_dir.glob("slice_*.json"))
    if not slice_files:
        raise FileNotFoundError(f"No slice_*.json files found in {raw_dir}")

    rows: list[dict[str, Any]] = []
    used_files = 0
    for path in slice_files:
        payload = orjson.loads(path.read_bytes())
        meta = payload.get("_meta", {})
        if meta.get("schema_version") != SCHEMA_VERSION:
            continue
        used_files += 1
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

    if used_files == 0:
        raise RuntimeError(
            f"Found slice_*.json files in {raw_dir}, but none matched schema_version={SCHEMA_VERSION}. "
            "Run `python etl/01_extract.py` to fetch updated slices."
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

    if not consolidate_only:
        tasks: list[tuple[str, QuerySlice]] = [
            (source, q_slice)
            for source in active_sources
            for q_slice in build_query_slices(source)
        ]
        total_tasks = len(tasks)
        print(
            f"Starting Wikidata SPARQL extraction ({total_tasks} queries across "
            f"[{', '.join(active_sources)}])..."
        )

        with httpx.Client(http2=False) as client:
            for idx, (source, q_slice) in enumerate(tasks, start=1):
                tag = f"[{idx}/{total_tasks}] {q_slice.label} ({source})"
                _fetch_and_save_slice(client, q_slice, source, tag, refresh=refresh)

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
