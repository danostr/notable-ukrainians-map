"""
===============================================================================
02_clean_geocode.py — Entity Deduplication, Hygiene & GeoTree Builder (T4 + T12)
-------------------------------------------------------------------------------
Transforms `etl/processed/01_raw_people.parquet` into a clean, deduplicated
one-row-per-person dataset at `etl/processed/02_clean_people.parquet`.

Pipeline Steps:
  1. Hygiene, Coordinate Outlier Audit & Historical Gazetteer Normalization:
     - Strips leaked Wikidata Q-ID strings (e.g., "Q1899") appearing as labels.
     - Resolves renamed historical Ukrainian towns via
       `etl/config/historical_gazetteer.json` before coordinate snapping.
     - Auto-repairs swapped Ukrainian (lon, lat) pairs, drops (0.0, 0.0) /
       out-of-bounds coordinates, and rejects generic country-level `Q212`
       centroids lacking a specific settlement.
     - Drops records lacking any usable name across EN, UK, and RU.
  2. Deterministic Entity Deduplication & Village-Precision Ranking (Polars):
     - Collapses multi-row records per `qid` into unique people with
       deduplicated `occupation_qids` and `occupation_labels` lists.
     - Ranks candidate birthplaces so specific villages (`sela`) and towns
       inside Ukraine beat oblast-level centroids.
     - Computes explicit provenance flags (`born_in_ukraine`,
       `ukrainian_nationality_or_heritage`, `origin_type`, `provenance_tags`),
       lifespan years (`birth_year`, `death_year`), and secondary authority
       identifiers (`esu_id`, `ieu_id`, `viaf_id`).
  3. Cached Trilingual Administrative Hierarchy (`etl/raw/places_hierarchy.json`):
     - Queries Wikidata in batches of 200 unique `birthplace_qid`s to resolve
       EN / UK / RU labels for the settlement, modern Ukrainian Oblast
       (or foreign Admin-1 region), and country.
     - Maps historical governorates / graph gaps inside Ukraine to the 27
       modern first-level Ukrainian administrative divisions.
     - Snaps people sharing the same `birthplace_qid` to a single canonical
       `(lat, lon)` coordinate pair and assigns a `birthplace_qid`-scoped
       `loc_key` so distinct nearby villages never collide.
===============================================================================
"""

from __future__ import annotations

import argparse
import json
import math
import re
import time
from pathlib import Path
from typing import Any

import httpx
import orjson
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "etl" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "etl" / "processed"
GAZETTEER_PATH = PROJECT_ROOT / "etl" / "config" / "historical_gazetteer.json"

PARQUET_IN = PROCESSED_DIR / "01_raw_people.parquet"
PARQUET_OUT = PROCESSED_DIR / "02_clean_people.parquet"
PLACES_CACHE = RAW_DIR / "places_hierarchy.json"

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = (
    "UkraineNotablesMapETL/1.1 "
    "(https://github.com/danostr/notable-ukrainians-map; educational historical map project) "
    "httpx"
)

UKRAINE_QID = "Q212"
QID_RE = re.compile(r"^Q[1-9]\d*$")
SLUG_UNSAFE_RE = re.compile(r"[^a-zA-Z0-9]+")
YEAR_RE = re.compile(r"^([+-]?\d{1,4})")

# Canonical 27 first-level administrative divisions of Ukraine:
# qid -> (en, uk, ru, approx_lat, approx_lon)
UKRAINE_REGIONS: dict[str, tuple[str, str, str, float, float]] = {
    "Q1899": ("Kyiv", "Київ", "Киев", 50.4501, 30.5234),
    "Q7525": ("Sevastopol", "Севастополь", "Севастополь", 44.6167, 33.5254),
    "Q756294": (
        "Autonomous Republic of Crimea",
        "Автономна Республіка Крим",
        "Автономная Республика Крым",
        45.0,
        34.1,
    ),
    "Q166709": (
        "Vinnytsia Oblast",
        "Вінницька область",
        "Винницкая область",
        49.2331,
        28.4682,
    ),
    "Q167859": (
        "Volyn Oblast",
        "Волинська область",
        "Волынская область",
        50.7472,
        25.3254,
    ),
    "Q170672": (
        "Dnipropetrovsk Oblast",
        "Дніпропетровська область",
        "Днепропетровская область",
        48.4647,
        35.0462,
    ),
    "Q2012050": (
        "Donetsk Oblast",
        "Донецька область",
        "Донецкая область",
        48.0159,
        37.8028,
    ),
    "Q40637": (
        "Zhytomyr Oblast",
        "Житомирська область",
        "Житомирская область",
        50.2547,
        28.6587,
    ),
    "Q170213": (
        "Zakarpattia Oblast",
        "Закарпатська область",
        "Закарпатская область",
        48.6208,
        22.2879,
    ),
    "Q171334": (
        "Zaporizhzhia Oblast",
        "Запорізька область",
        "Запорожская область",
        47.8388,
        35.1396,
    ),
    "Q178269": (
        "Ivano-Frankivsk Oblast",
        "Івано-Франківська область",
        "Ивано-Франковская область",
        48.9226,
        24.7111,
    ),
    "Q170036": (
        "Kyiv Oblast",
        "Київська область",
        "Киевская область",
        50.0527,
        30.7667,
    ),
    "Q180981": (
        "Kirovohrad Oblast",
        "Кіровоградська область",
        "Кировоградская область",
        48.5079,
        32.2623,
    ),
    "Q171965": (
        "Luhansk Oblast",
        "Луганська область",
        "Луганская область",
        48.5740,
        39.3078,
    ),
    "Q164193": (
        "Lviv Oblast",
        "Львівська область",
        "Львовская область",
        49.8397,
        24.0297,
    ),
    "Q181633": (
        "Mykolaiv Oblast",
        "Миколаївська область",
        "Николаевская область",
        46.9750,
        31.9946,
    ),
    "Q171852": (
        "Odesa Oblast",
        "Одеська область",
        "Одесская область",
        46.4825,
        30.7233,
    ),
    "Q169501": (
        "Poltava Oblast",
        "Полтавська область",
        "Полтавская область",
        49.5883,
        34.5514,
    ),
    "Q174187": (
        "Rivne Oblast",
        "Рівненська область",
        "Ровненская область",
        50.6199,
        26.2516,
    ),
    "Q170446": ("Sumy Oblast", "Сумська область", "Сумская область", 50.9077, 34.7981),
    "Q173407": (
        "Ternopil Oblast",
        "Тернопільська область",
        "Тернопольская область",
        49.5535,
        25.5948,
    ),
    "Q170666": (
        "Kharkiv Oblast",
        "Харківська область",
        "Харьковская область",
        49.9935,
        36.2304,
    ),
    "Q163271": (
        "Kherson Oblast",
        "Херсонська область",
        "Херсонская область",
        46.6354,
        32.6169,
    ),
    "Q171331": (
        "Khmelnytskyi Oblast",
        "Хмельницька область",
        "Хмельницкая область",
        49.4229,
        26.9871,
    ),
    "Q161808": (
        "Cherkasy Oblast",
        "Черкаська область",
        "Черкасская область",
        49.4444,
        32.0598,
    ),
    "Q168856": (
        "Chernivtsi Oblast",
        "Чернівецька область",
        "Черновицкая область",
        48.2921,
        25.9358,
    ),
    "Q167874": (
        "Chernihiv Oblast",
        "Чернігівська область",
        "Черниговская область",
        51.4982,
        31.2893,
    ),
}

# Maps historical governorate / region substrings to canonical Ukrainian region Q-IDs
HISTORICAL_REGION_HINTS: list[tuple[str, str]] = [
    ("crimea", "Q756294"),
    ("taurida", "Q756294"),
    ("крим", "Q756294"),
    ("крым", "Q756294"),
    ("kyiv", "Q170036"),
    ("kiev", "Q170036"),
    ("київ", "Q170036"),
    ("киев", "Q170036"),
    ("lviv", "Q164193"),
    ("lemberg", "Q164193"),
    ("galicia", "Q164193"),
    ("львів", "Q164193"),
    ("львов", "Q164193"),
    ("kharkiv", "Q170666"),
    ("kharkov", "Q170666"),
    ("харків", "Q170666"),
    ("харьков", "Q170666"),
    ("odesa", "Q171852"),
    ("odessa", "Q171852"),
    ("одес", "Q171852"),
    ("podolia", "Q166709"),
    ("поділ", "Q166709"),
    ("подол", "Q166709"),
    ("volhyn", "Q167859"),
    ("волин", "Q167859"),
    ("волын", "Q167859"),
    ("poltava", "Q169501"),
    ("полтав", "Q169501"),
    ("chernihiv", "Q167874"),
    ("chernigov", "Q167874"),
    ("черніг", "Q167874"),
    ("черниг", "Q167874"),
    ("yekaterinoslav", "Q170672"),
    ("katerynoslav", "Q170672"),
    ("dnipro", "Q170672"),
    ("bukovina", "Q168856"),
    ("буковин", "Q168856"),
    ("carpathian", "Q170213"),
    ("subcarpath", "Q170213"),
    ("закарпат", "Q170213"),
]


def load_gazetteer(path: Path = GAZETTEER_PATH) -> dict[str, Any]:
    """Loads optional historical settlement & centroid outlier rules."""
    if not path.is_file() or path.stat().st_size == 0:
        return {"historical_settlements": {}, "country_centroid_outliers": []}
    return orjson.loads(path.read_bytes())


def _clean_label_expr(col_name: str) -> pl.Expr:
    """Trims whitespace and converts blank strings or leaked Q-IDs into null."""
    trimmed = pl.col(col_name).str.strip_chars()
    return (
        pl.when(
            trimmed.is_null() | (trimmed == "") | trimmed.str.contains(r"^Q[1-9]\d*$")
        )
        .then(None)
        .otherwise(trimmed)
        .alias(col_name)
    )


def _parse_year(date_str: str | None) -> int | None:
    """Extracts an integer year (supporting BCE negative years) from ISO date strings."""
    if not date_str:
        return None
    match = YEAR_RE.match(date_str.strip())
    if not match:
        return None
    try:
        year = int(match.group(1))
        return year if -3000 <= year <= 2030 else None
    except ValueError:
        return None


def _in_ukraine_bbox(lat: float, lon: float) -> bool:
    return 44.0 <= lat <= 52.6 and 22.0 <= lon <= 40.5


def _nearest_ukrainian_region(lat: float, lon: float) -> str:
    """Finds the closest modern Ukrainian administrative division by Euclidean distance."""
    best_qid = "Q170036"
    best_dist = float("inf")
    for reg_qid, (_, _, _, r_lat, r_lon) in UKRAINE_REGIONS.items():
        dist = (lat - r_lat) ** 2 + (lon - r_lon) ** 2
        if dist < best_dist:
            best_dist = dist
            best_qid = reg_qid
    return best_qid


def _match_historical_region(raw_text: str | None) -> str | None:
    """Attempts to match historical region/governorate labels to a modern region Q-ID."""
    if not raw_text:
        return None
    lowered = raw_text.lower()
    for needle, reg_qid in HISTORICAL_REGION_HINTS:
        if needle in lowered:
            return reg_qid
    return None


def _extract_qid(uri: str | None) -> str | None:
    if not uri:
        return None
    tail = uri.rsplit("/", 1)[-1]
    return tail if QID_RE.match(tail) else None


def _binding_val(binding: dict[str, Any], key: str) -> str | None:
    cell = binding.get(key)
    if not isinstance(cell, dict):
        return None
    val = cell.get("value")
    if val is None:
        return None
    s = str(val).strip()
    if not s or QID_RE.match(s):
        return None
    return s


def _parse_sparql_json(raw_bytes: bytes) -> dict[str, Any]:
    try:
        return orjson.loads(raw_bytes)
    except orjson.JSONDecodeError:
        return json.loads(raw_bytes.decode("utf-8", errors="replace"), strict=False)


def _build_place_hierarchy_sparql(place_qids: list[str]) -> str:
    """Builds a batched SPARQL query resolving EN/UK/RU labels, coords, and admin parents."""
    values_clause = " ".join(f"wd:{qid}" for qid in place_qids)
    return f"""
SELECT DISTINCT
  ?place
  ?place_en ?place_uk ?place_ru
  ?coord
  ?country ?country_en ?country_uk ?country_ru
  ?ua_region
  ?admin1 ?admin1_en ?admin1_uk ?admin1_ru
WHERE {{
  VALUES ?place {{ {values_clause} }}

  OPTIONAL {{ ?place rdfs:label ?place_en . FILTER(LANG(?place_en) = "en") }}
  OPTIONAL {{ ?place rdfs:label ?place_uk . FILTER(LANG(?place_uk) = "uk") }}
  OPTIONAL {{ ?place rdfs:label ?place_ru . FILTER(LANG(?place_ru) = "ru") }}
  OPTIONAL {{ ?place wdt:P625 ?coord . }}

  OPTIONAL {{
    ?place wdt:P17 ?country .
    OPTIONAL {{ ?country rdfs:label ?country_en . FILTER(LANG(?country_en) = "en") }}
    OPTIONAL {{ ?country rdfs:label ?country_uk . FILTER(LANG(?country_uk) = "uk") }}
    OPTIONAL {{ ?country rdfs:label ?country_ru . FILTER(LANG(?country_ru) = "ru") }}
  }}

  OPTIONAL {{
    ?place wdt:P131* ?ua_region .
    FILTER(
      ?ua_region IN (
        wd:Q1899, wd:Q7525, wd:Q756294,
        wd:Q166709, wd:Q167859, wd:Q170672, wd:Q2012050, wd:Q40637,
        wd:Q170213, wd:Q171334, wd:Q178269, wd:Q170036, wd:Q180981,
        wd:Q171965, wd:Q164193, wd:Q181633, wd:Q171852, wd:Q169501,
        wd:Q174187, wd:Q170446, wd:Q173407, wd:Q170666, wd:Q163271,
        wd:Q171331, wd:Q161808, wd:Q168856, wd:Q167874
      )
    )
  }}

  OPTIONAL {{
    ?place wdt:P131 ?admin1 .
    OPTIONAL {{ ?admin1 rdfs:label ?admin1_en . FILTER(LANG(?admin1_en) = "en") }}
    OPTIONAL {{ ?admin1 rdfs:label ?admin1_uk . FILTER(LANG(?admin1_uk) = "uk") }}
    OPTIONAL {{ ?admin1 rdfs:label ?admin1_ru . FILTER(LANG(?admin1_ru) = "ru") }}
  }}
}}
""".strip()


def _query_wikidata_places_batch(
    client: httpx.Client,
    batch_qids: list[str],
    max_retries: int = 5,
) -> list[dict[str, Any]]:
    """Executes one batched VALUES place query with Retry-After and exponential backoff."""
    sparql = _build_place_hierarchy_sparql(batch_qids)
    retryable = {429, 502, 503, 504}

    for attempt in range(1, max_retries + 1):
        try:
            resp = client.post(
                SPARQL_ENDPOINT, data={"query": sparql, "format": "json"}
            )
            if resp.status_code == 200:
                payload = _parse_sparql_json(resp.content)
                return payload.get("results", {}).get("bindings", [])
            if resp.status_code in retryable and attempt < max_retries:
                retry_after = resp.headers.get("Retry-After")
                wait_s = (
                    float(retry_after)
                    if retry_after and retry_after.isdigit()
                    else min(60.0, 2.0**attempt)
                )
                print(
                    f"  [HTTP {resp.status_code}] Place lookup throttled; sleeping {wait_s:.1f}s..."
                )
                time.sleep(wait_s)
                continue
            resp.raise_for_status()
        except (
            httpx.TimeoutException,
            httpx.TransportError,
            json.JSONDecodeError,
        ) as exc:
            if attempt >= max_retries:
                raise
            wait_s = min(60.0, 2.0**attempt)
            print(
                f"  [Transient {type(exc).__name__}] Retrying place lookup in {wait_s:.1f}s..."
            )
            time.sleep(wait_s)

    return []


def resolve_places_hierarchy(
    places_df: pl.DataFrame,
    cache_path: Path,
    refresh_cache: bool = False,
    batch_size: int = 200,
) -> dict[str, dict[str, Any]]:
    """
    Resolves and caches trilingual settlement, region, and country metadata
    for every unique birthplace in `places_df`.
    """
    cache: dict[str, dict[str, Any]] = {}
    if not refresh_cache and cache_path.is_file() and cache_path.stat().st_size > 0:
        cache = orjson.loads(cache_path.read_bytes())

    needed_qids = [
        qid for qid in places_df["birthplace_qid"].to_list() if qid and qid not in cache
    ]

    if needed_qids:
        print(
            f"Resolving {len(needed_qids):,} uncached birthplaces via Wikidata "
            f"(batches of {batch_size})..."
        )
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/sparql-results+json",
        }
        total_batches = math.ceil(len(needed_qids) / batch_size)

        with httpx.Client(headers=headers, timeout=90.0) as client:
            for b_idx in range(total_batches):
                chunk = needed_qids[b_idx * batch_size : (b_idx + 1) * batch_size]
                print(
                    f"  [Batch {b_idx + 1}/{total_batches}] Querying {len(chunk)} places..."
                )
                bindings = _query_wikidata_places_batch(client, chunk)

                grouped: dict[str, list[dict[str, Any]]] = {q: [] for q in chunk}
                for b in bindings:
                    pq = _extract_qid(_binding_val(b, "place"))
                    if pq in grouped:
                        grouped[pq].append(b)

                for pq, rows in grouped.items():
                    cache[pq] = _summarize_place_bindings(pq, rows)

                if b_idx + 1 < total_batches:
                    time.sleep(1.2)

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(orjson.dumps(cache, option=orjson.OPT_INDENT_2))
        print(
            f"Saved updated place hierarchy cache ({len(cache):,} places) -> {cache_path.name}"
        )
    else:
        print(
            f"All {places_df.height:,} unique birthplaces loaded from cache ({cache_path.name})."
        )

    # Merge fallback labels/coordinates from `places_df` into final normalized metadata
    normalized: dict[str, dict[str, Any]] = {}
    for row in places_df.iter_rows(named=True):
        pq = row["birthplace_qid"]
        lat = float(row["lat"])
        lon = float(row["lon"])
        raw_en = row["birthplace_en"]
        raw_uk = row["birthplace_uk"]
        raw_ru = row.get("birthplace_ru")

        entry = cache.get(pq, {}) if pq else {}
        city_en = entry.get("place_en") or raw_en or raw_uk or pq or "Unknown"
        city_uk = entry.get("place_uk") or raw_uk or city_en
        city_ru = entry.get("place_ru") or raw_ru or city_uk

        is_ukraine = bool(entry.get("is_ukraine")) or (
            entry.get("country_qid") == UKRAINE_QID
        )
        # Geographic bounding-box check for Ukraine when Wikidata P17 is missing
        if (
            not entry.get("country_qid")
            and (44.3 <= lat <= 52.4 and 22.1 <= lon <= 40.2)
            and row.get("is_born_in_ua")
        ):
            is_ukraine = True

        if is_ukraine:
            country_en, country_uk, country_ru = "Ukraine", "Україна", "Украина"
            ua_reg_qid = (
                pq
                if pq in UKRAINE_REGIONS
                else entry.get("ua_region_qid")
                or _match_historical_region(entry.get("admin1_en"))
                or _match_historical_region(entry.get("admin1_uk"))
                or _match_historical_region(city_en)
                or _nearest_ukrainian_region(lat, lon)
            )
            reg_en, reg_uk, reg_ru, _, _ = UKRAINE_REGIONS[ua_reg_qid]

            # Special-status cities (Kyiv / Sevastopol) avoid duplicating [Ukraine, Kyiv, Kyiv]
            if city_en == reg_en:
                geo_tree = [country_en, reg_en]
            else:
                geo_tree = [country_en, reg_en, city_en]
        else:
            country_en = entry.get("country_en") or "Other"
            country_uk = entry.get("country_uk") or country_en
            country_ru = entry.get("country_ru") or country_uk

            reg_en = entry.get("admin1_en") or city_en
            reg_uk = entry.get("admin1_uk") or reg_en
            reg_ru = entry.get("admin1_ru") or reg_uk

            if city_en == reg_en:
                geo_tree = [country_en, city_en]
            else:
                geo_tree = [country_en, reg_en, city_en]

        slug = SLUG_UNSAFE_RE.sub("_", city_en).strip("_") or (pq or "loc")
        loc_key = (
            f"{slug}_{pq}_{round(lat, 5)}_{round(lon, 5)}"
            if pq
            else f"{slug}_{round(lat, 5)}_{round(lon, 5)}"
        )

        normalized[pq or loc_key] = {
            "birthplace_en": city_en,
            "birthplace_uk": city_uk,
            "birthplace_ru": city_ru,
            "region_en": reg_en,
            "region_uk": reg_uk,
            "region_ru": reg_ru,
            "country_en": country_en,
            "country_uk": country_uk,
            "country_ru": country_ru,
            "geo_tree": geo_tree,
            "loc_key": loc_key,
            "lat": round(lat, 5),
            "lon": round(lon, 5),
            "is_ukraine": is_ukraine,
        }

    return normalized


def _summarize_place_bindings(
    place_qid: str, rows: list[dict[str, Any]]
) -> dict[str, Any]:
    """Collapses multiple SPARQL bindings for one place into a single clean dict."""
    place_en = place_uk = place_ru = None
    country_qid = country_en = country_uk = country_ru = None
    ua_region_qid = None
    admin1_en = admin1_uk = admin1_ru = None
    is_ukraine = place_qid in UKRAINE_REGIONS

    for b in rows:
        place_en = place_en or _binding_val(b, "place_en")
        place_uk = place_uk or _binding_val(b, "place_uk")
        place_ru = place_ru or _binding_val(b, "place_ru")

        c_qid = _extract_qid(_binding_val(b, "country"))
        if c_qid == UKRAINE_QID:
            is_ukraine = True
            country_qid = UKRAINE_QID
            country_en, country_uk, country_ru = "Ukraine", "Україна", "Украина"
        elif c_qid and country_qid != UKRAINE_QID:
            country_qid = country_qid or c_qid
            country_en = country_en or _binding_val(b, "country_en")
            country_uk = country_uk or _binding_val(b, "country_uk")
            country_ru = country_ru or _binding_val(b, "country_ru")

        u_reg = _extract_qid(_binding_val(b, "ua_region"))
        if u_reg and u_reg in UKRAINE_REGIONS:
            ua_region_qid = u_reg
            is_ukraine = True

        if not admin1_en:
            admin1_en = _binding_val(b, "admin1_en")
            admin1_uk = _binding_val(b, "admin1_uk")
            admin1_ru = _binding_val(b, "admin1_ru")

    return {
        "place_en": place_en,
        "place_uk": place_uk,
        "place_ru": place_ru,
        "is_ukraine": is_ukraine,
        "country_qid": country_qid,
        "country_en": country_en,
        "country_uk": country_uk,
        "country_ru": country_ru,
        "ua_region_qid": ua_region_qid,
        "admin1_en": admin1_en,
        "admin1_uk": admin1_uk,
        "admin1_ru": admin1_ru,
    }


def _apply_gazetteer_and_coord_audit(
    df: pl.DataFrame,
    gazetteer: dict[str, Any],
) -> pl.DataFrame:
    """
    Applies historical settlement name/coordinate overrides, unswaps inverted
    Ukrainian (lon, lat) coordinates, and rejects country-centroid fallbacks.
    """
    settlements = gazetteer.get("historical_settlements", {})
    centroid_outliers = gazetteer.get("country_centroid_outliers", [])

    audited_rows: list[dict[str, Any]] = []
    for row in df.iter_rows(named=True):
        bp_qid = row.get("birthplace_qid")
        bp_en = row.get("birthplace_en")
        bp_uk = row.get("birthplace_uk")
        bp_ru = row.get("birthplace_ru")
        lat = row.get("lat")
        lon = row.get("lon")

        # 1. Historical gazetteer resolution (e.g., Proskuriv -> Khmelnytskyi)
        gaz_hit = None
        for lbl in (bp_en, bp_uk, bp_ru):
            if lbl and lbl.lower() in settlements:
                gaz_hit = settlements[lbl.lower()]
                break

        if gaz_hit:
            bp_qid = gaz_hit["qid"]
            bp_en = gaz_hit["name_en"]
            bp_uk = gaz_hit["name_uk"]
            bp_ru = gaz_hit["name_ru"]
            lat = float(gaz_hit["lat"])
            lon = float(gaz_hit["lon"])

        # 2. Validate coordinates and auto-repair swapped Ukrainian (lon, lat)
        if (
            lat is None
            or lon is None
            or not (math.isfinite(lat) and math.isfinite(lon))
        ):
            continue
        if (lat == 0.0 and lon == 0.0) or not (
            -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0
        ):
            continue

        if (
            not _in_ukraine_bbox(lat, lon)
            and _in_ukraine_bbox(lon, lat)
            and row.get("source_query") == "born"
        ):
            lat, lon = lon, lat

        # 3. Reject generic country-centroid fallbacks ("Ukraine" Q212 without a settlement)
        if bp_qid == UKRAINE_QID:
            continue

        is_centroid_outlier = False
        for outlier in centroid_outliers:
            if bp_qid == outlier.get("qid"):
                rad = float(outlier.get("radius_deg", 0.03))
                if (
                    abs(lat - float(outlier["lat"])) <= rad
                    and abs(lon - float(outlier["lon"])) <= rad
                ):
                    is_centroid_outlier = True
                    break
        if is_centroid_outlier:
            continue

        updated = dict(row)
        updated["birthplace_qid"] = bp_qid
        updated["birthplace_en"] = bp_en
        updated["birthplace_uk"] = bp_uk
        updated["birthplace_ru"] = bp_ru
        updated["lat"] = float(lat)
        updated["lon"] = float(lon)
        audited_rows.append(updated)

    if not audited_rows:
        return df.clear()
    return pl.DataFrame(audited_rows, schema=df.schema)


def run_clean_geocode(refresh_places: bool = False) -> None:
    """Executes deduplication, coordinate/label hygiene, and GeoTree normalization."""
    if not PARQUET_IN.is_file():
        raise FileNotFoundError(f"Missing input Parquet file: {PARQUET_IN}")

    raw_df = pl.read_parquet(PARQUET_IN)
    raw_row_count = raw_df.height
    raw_unique_qids = raw_df.select(pl.col("qid").n_unique()).item()
    gazetteer = load_gazetteer(GAZETTEER_PATH)

    # Ensure backward compatibility when optional T12 columns are absent in older slices
    for opt_col in (
        "birthplace_ru",
        "birth_date",
        "death_date",
        "esu_id",
        "ieu_id",
        "viaf_id",
    ):
        if opt_col not in raw_df.columns:
            raw_df = raw_df.with_columns(pl.lit(None, dtype=pl.String).alias(opt_col))

    # 1. Clean string columns and strip leaked Q-IDs
    text_cols = [
        "name_en",
        "name_uk",
        "name_ru",
        "birthplace_en",
        "birthplace_uk",
        "birthplace_ru",
        "occupation_en",
    ]
    cleaned = raw_df.with_columns([_clean_label_expr(c) for c in text_cols])

    # 2. Drop rows missing usable names across all 3 languages
    cleaned = cleaned.filter(
        pl.col("name_en").is_not_null()
        | pl.col("name_uk").is_not_null()
        | pl.col("name_ru").is_not_null()
    )

    # 3. Apply historical gazetteer & coordinate outlier audit
    valid_coords = _apply_gazetteer_and_coord_audit(cleaned, gazetteer)

    valid_qids_after_coords = valid_coords.select(pl.col("qid").n_unique()).item()
    dropped_no_coords = raw_unique_qids - valid_qids_after_coords

    # 4. Deterministic sorting so `.first()` picks specific settlements inside Ukraine
    oblast_qids = list(UKRAINE_REGIONS.keys())
    prioritized = valid_coords.with_columns(
        [
            (pl.col("source_query") == "born").alias("is_born_in_ua"),
            (
                pl.col("birthplace_qid").is_not_null()
                & ~pl.col("birthplace_qid").is_in(oblast_qids)
                & (pl.col("birthplace_qid") != UKRAINE_QID)
            ).alias("is_specific_settlement"),
            pl.col("birthplace_qid").is_not_null().alias("has_bp_qid"),
        ]
    ).sort(
        by=[
            "qid",
            "is_born_in_ua",
            "is_specific_settlement",
            "has_bp_qid",
            "birthplace_qid",
        ],
        descending=[False, True, True, True, False],
    )

    # 5. Deduplicate per `qid` while preserving all occupations & provenance evidence
    people_df = prioritized.group_by("qid", maintain_order=True).agg(
        [
            pl.col("name_en").drop_nulls().first().alias("name_en"),
            pl.col("name_uk").drop_nulls().first().alias("name_uk"),
            pl.col("name_ru").drop_nulls().first().alias("name_ru"),
            pl.col("image").drop_nulls().first().alias("image"),
            pl.col("wiki_en").drop_nulls().first().alias("wiki_en"),
            pl.col("wiki_uk").drop_nulls().first().alias("wiki_uk"),
            pl.col("wiki_ru").drop_nulls().first().alias("wiki_ru"),
            pl.col("birthplace_qid").first().alias("birthplace_qid"),
            pl.col("birthplace_en").drop_nulls().first().alias("birthplace_en"),
            pl.col("birthplace_uk").drop_nulls().first().alias("birthplace_uk"),
            pl.col("birthplace_ru").drop_nulls().first().alias("birthplace_ru"),
            pl.col("lat").first().alias("lat"),
            pl.col("lon").first().alias("lon"),
            pl.col("birth_date").drop_nulls().first().alias("birth_date"),
            pl.col("death_date").drop_nulls().first().alias("death_date"),
            pl.col("esu_id").drop_nulls().first().alias("esu_id"),
            pl.col("ieu_id").drop_nulls().first().alias("ieu_id"),
            pl.col("viaf_id").drop_nulls().first().alias("viaf_id"),
            pl.col("occupation_qid")
            .drop_nulls()
            .unique()
            .sort()
            .alias("occupation_qids"),
            pl.col("occupation_en")
            .drop_nulls()
            .unique()
            .sort()
            .alias("occupation_labels"),
            pl.col("sitelinks_count").max().alias("sitelinks_count"),
            (pl.col("source_query") == "born").any().alias("raw_born_in_ua"),
            (pl.col("source_query") == "citizen").any().alias("ukrainian_citizen"),
            (pl.col("source_query") == "heritage").any().alias("ukrainian_heritage"),
        ]
    )

    # 6. Snap coordinates per unique `birthplace_qid` so city/village clusters align cleanly
    unique_places = (
        people_df.group_by("birthplace_qid")
        .agg(
            [
                pl.col("birthplace_en").drop_nulls().first().alias("birthplace_en"),
                pl.col("birthplace_uk").drop_nulls().first().alias("birthplace_uk"),
                pl.col("birthplace_ru").drop_nulls().first().alias("birthplace_ru"),
                pl.col("lat").median().round(5).alias("lat"),
                pl.col("lon").median().round(5).alias("lon"),
                pl.col("raw_born_in_ua").any().alias("is_born_in_ua"),
            ]
        )
        .sort("birthplace_qid")
    )

    place_lookup = resolve_places_hierarchy(
        unique_places,
        cache_path=PLACES_CACHE,
        refresh_cache=refresh_places,
    )

    # 7. Attach normalized trilingual geography, snapped coordinates, and explicit provenance
    enriched_rows: list[dict[str, Any]] = []
    for row in people_df.iter_rows(named=True):
        pq = row["birthplace_qid"]
        fallback_slug = SLUG_UNSAFE_RE.sub(
            "_", row["birthplace_en"] or row["birthplace_uk"] or "loc"
        ).strip("_")
        fallback_key = f"{fallback_slug}_{round(float(row['lat']), 5)}_{round(float(row['lon']), 5)}"

        geo = place_lookup[pq or fallback_key]
        born_in_ua = geo["country_en"] == "Ukraine"
        ua_heritage_or_nat = not born_in_ua

        provenance_tags: list[str] = []
        if born_in_ua:
            provenance_tags.append("born_in_ukraine")
        else:
            provenance_tags.append("born_abroad")
        if row["ukrainian_citizen"]:
            provenance_tags.append("citizenship_p27")
        if row["ukrainian_heritage"]:
            provenance_tags.append("ethnic_group_p172")
        if row["esu_id"]:
            provenance_tags.append("esu_p4613")
        if row["ieu_id"]:
            provenance_tags.append("ieu_p9070")

        origin_type = (
            "born_in_ukraine" if born_in_ua else "ukrainian_nationality_or_heritage"
        )

        enriched_rows.append(
            {
                "qid": row["qid"],
                "name_en": row["name_en"],
                "name_uk": row["name_uk"],
                "name_ru": row["name_ru"],
                "image": row["image"],
                "wiki_en": row["wiki_en"],
                "wiki_uk": row["wiki_uk"],
                "wiki_ru": row["wiki_ru"],
                "birthplace_qid": pq,
                "birthplace_en": geo["birthplace_en"],
                "birthplace_uk": geo["birthplace_uk"],
                "birthplace_ru": geo["birthplace_ru"],
                "region_en": geo["region_en"],
                "region_uk": geo["region_uk"],
                "region_ru": geo["region_ru"],
                "country_en": geo["country_en"],
                "country_uk": geo["country_uk"],
                "country_ru": geo["country_ru"],
                "geo_tree": geo["geo_tree"],
                "loc_key": geo["loc_key"],
                "lat": geo["lat"],
                "lon": geo["lon"],
                "occupation_qids": row["occupation_qids"],
                "occupation_labels": row["occupation_labels"],
                "sitelinks_count": row["sitelinks_count"],
                "born_in_ukraine": born_in_ua,
                "ukrainian_citizen": row["ukrainian_citizen"],
                "ukrainian_nationality_or_heritage": ua_heritage_or_nat,
                "origin_type": origin_type,
                "provenance_tags": provenance_tags,
                "birth_year": _parse_year(row["birth_date"]),
                "death_year": _parse_year(row["death_date"]),
                "esu_id": row["esu_id"],
                "ieu_id": row["ieu_id"],
                "viaf_id": row["viaf_id"],
            }
        )

    out_df = pl.DataFrame(enriched_rows).sort("sitelinks_count", descending=True)
    PARQUET_OUT.parent.mkdir(parents=True, exist_ok=True)
    out_df.write_parquet(PARQUET_OUT, compression="zstd")

    unique_locations = out_df.select(pl.col("loc_key").n_unique()).item()
    in_ua_count = out_df.filter(pl.col("born_in_ukraine")).height

    print("\n=== T4 + T12 Clean & Geocode Summary ===")
    print(
        f"Raw rows in 01_raw_people.parquet   : {raw_row_count:,} ({raw_unique_qids:,} unique Q-IDs)"
    )
    print(f"Dropped (missing/invalid coords/name): {dropped_no_coords:,}")
    print(f"Clean unique people written         : {out_df.height:,}")
    print(f"  - Born inside modern Ukraine       : {in_ua_count:,}")
    print(f"  - Ukrainian nationality/heritage   : {out_df.height - in_ua_count:,}")
    print(f"Unique normalized locations (locKey) : {unique_locations:,}")
    print(
        f"Output artifact                      : {PARQUET_OUT.relative_to(PROJECT_ROOT)}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deduplicate raw people, validate coordinates, and build trilingual GeoTrees."
    )
    parser.add_argument(
        "--refresh-places",
        action="store_true",
        help="Re-query Wikidata for place hierarchies instead of using etl/raw/places_hierarchy.json.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_clean_geocode(refresh_places=args.refresh_places)
