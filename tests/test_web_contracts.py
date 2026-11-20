"""
===============================================================================
test_web_contracts.py — Automated Schema & Referential Integrity Test Suite
-------------------------------------------------------------------------------
Enforces the JSON data contracts between the offline Polars ETL pipeline
(`etl/`) and the static browser client (`web/js/dataLoader.js`, `ui.js`).
===============================================================================
"""

from __future__ import annotations

import math
import re
from collections import Counter
from typing import TypeGuard

from conftest import WebDataBundle

QID_PATTERN = re.compile(r"^Q[1-9]\d*$")
HTTP_URL_PATTERN = re.compile(r"^https?://", re.IGNORECASE)

REQUIRED_DETAIL_KEYS = {
    "name_en",
    "name_uk",
    "name_ru",
    "image",
    "birthplace",
    "occupations",
    "wiki_en",
    "wiki_uk",
    "wiki_ru",
    "remarkability",
}


def _is_finite_number(val: object) -> TypeGuard[int | float]:
    return (
        isinstance(val, (int, float))
        and not isinstance(val, bool)
        and math.isfinite(val)
    )


def _is_valid_lat_lon(lat: object, lon: object) -> bool:
    """Checks Earth coordinate bounds and rejects (0.0, 0.0) Null Island defaults."""
    if not (_is_finite_number(lat) and _is_finite_number(lon)):
        return False
    if not (-90.0 <= float(lat) <= 90.0 and -180.0 <= float(lon) <= 180.0):
        return False
    return not (float(lat) == 0.0 and float(lon) == 0.0)


def test_map_lite_structure_and_types(dataset: WebDataBundle) -> None:
    """Validates positional tuple lengths, Q-ID format, and field types in map_lite.json."""
    assert isinstance(dataset.map_lite, list)
    assert len(dataset.map_lite) > 0, "map_lite.json must not be empty"

    seen_ids: set[str] = set()

    for idx, row in enumerate(dataset.map_lite):
        assert isinstance(row, list), f"Row {idx} must be a list tuple"
        assert len(row) == 6, f"Row {idx} expected length 6, got {len(row)}"

        qid, lat, lon, categories, loc_key, geo_tree = row

        assert isinstance(qid, str) and QID_PATTERN.match(qid), (
            f"Invalid Q-ID at row {idx}: {qid!r}"
        )
        assert qid not in seen_ids, f"Duplicate Q-ID in map_lite.json: {qid}"
        seen_ids.add(qid)

        assert _is_finite_number(lat), f"Invalid lat for {qid}: {lat!r}"
        assert _is_finite_number(lon), f"Invalid lon for {qid}: {lon!r}"

        assert isinstance(categories, list) and len(categories) >= 1, (
            f"{qid} must have at least one [main, sub] category pair"
        )
        seen_pairs: set[tuple[str, str]] = set()
        for pair in categories:
            assert isinstance(pair, list) and len(pair) == 2, (
                f"Invalid category tuple for {qid}: {pair!r}"
            )
            main_cat, sub_cat = pair
            assert (
                isinstance(main_cat, str)
                and main_cat.strip() == main_cat
                and len(main_cat) > 0
            )
            assert (
                isinstance(sub_cat, str)
                and sub_cat.strip() == sub_cat
                and len(sub_cat) > 0
            )
            assert (main_cat, sub_cat) not in seen_pairs, (
                f"Duplicate category {pair!r} on {qid}"
            )
            seen_pairs.add((main_cat, sub_cat))

        assert loc_key is None or (
            isinstance(loc_key, str) and len(loc_key.strip()) > 0
        ), f"Invalid locKey for {qid}: {loc_key!r}"

        assert isinstance(geo_tree, list), f"geoTree for {qid} must be a list"
        for node in geo_tree:
            assert isinstance(node, str) and len(node.strip()) > 0, (
                f"Invalid geoTree entry for {qid}: {node!r}"
            )


def test_geographic_bounds(dataset: WebDataBundle) -> None:
    """Ensures every coordinate in map_lite.json and search_index.json is within Earth bounds."""
    for row in dataset.map_lite:
        qid, lat, lon = row[0], row[1], row[2]
        assert _is_valid_lat_lon(lat, lon), (
            f"Out-of-bounds or Null Island coordinates for {qid}: ({lat}, {lon})"
        )

    locations = dataset.search_index.get("locations", {})
    for loc_key, loc_val in locations.items():
        if loc_key == "zones":
            continue
        assert _is_valid_lat_lon(loc_val[0], loc_val[1]), (
            f"Out-of-bounds coordinates for location {loc_key!r}: ({loc_val[0]}, {loc_val[1]})"
        )


def test_topic_map_structure_and_category_consistency(dataset: WebDataBundle) -> None:
    """Verifies topic_map.json counts match unique person category memberships in map_lite.json."""
    assert isinstance(dataset.topic_map, dict)
    assert len(dataset.topic_map) > 0, "topic_map.json must not be empty"

    expected_counts: Counter[tuple[str, str]] = Counter()
    for row in dataset.map_lite:
        for main_cat, sub_cat in row[3]:
            expected_counts[(main_cat, sub_cat)] += 1

    actual_counts: dict[tuple[str, str], int] = {}
    for main_cat, sub_map in dataset.topic_map.items():
        assert isinstance(main_cat, str) and len(main_cat.strip()) > 0
        assert isinstance(sub_map, dict) and len(sub_map) > 0, (
            f"Main category {main_cat!r} must contain at least one sub-category"
        )
        for sub_cat, count in sub_map.items():
            assert isinstance(sub_cat, str) and len(sub_cat.strip()) > 0
            assert (
                isinstance(count, int) and not isinstance(count, bool) and count > 0
            ), (
                f"Category [{main_cat} > {sub_cat}] count must be positive int, got {count!r}"
            )
            actual_counts[(main_cat, sub_cat)] = count

    assert actual_counts == dict(expected_counts), (
        "Mismatch between topic_map.json counts and map_lite.json category assignments"
    )


def test_people_details_schema_and_hygiene(dataset: WebDataBundle) -> None:
    """Validates required keys, name fallback presence, image filenames, and wiki URL schemes."""
    assert isinstance(dataset.people_details, dict)
    assert len(dataset.people_details) > 0

    for qid, record in dataset.people_details.items():
        assert isinstance(qid, str) and QID_PATTERN.match(qid), (
            f"Invalid Q-ID key in people_details: {qid!r}"
        )
        assert isinstance(record, dict), f"Record for {qid} must be a dict"
        assert set(record.keys()) == REQUIRED_DETAIL_KEYS, (
            f"Key mismatch for {qid}: missing={REQUIRED_DETAIL_KEYS - set(record.keys())}, "
            f"extra={set(record.keys()) - REQUIRED_DETAIL_KEYS}"
        )

        names = [record["name_en"], record["name_uk"], record["name_ru"]]
        valid_names = [n for n in names if isinstance(n, str) and len(n.strip()) > 0]
        assert len(valid_names) >= 1, f"{qid} has no non-empty name across EN/UK/RU"
        for name in valid_names:
            assert not QID_PATTERN.match(name.strip()), (
                f"{qid} leaked raw Q-ID as name: {name!r}"
            )

        image = record["image"]
        if image is not None:
            assert isinstance(image, str) and len(image.strip()) > 0, (
                f"Invalid image for {qid}: {image!r}"
            )
            assert not HTTP_URL_PATTERN.match(image), (
                f"Store Wikimedia filename instead of full URL for {qid}: {image!r}"
            )
            assert not image.startswith("File:"), (
                f"Strip 'File:' prefix from image for {qid}: {image!r}"
            )

        assert (
            isinstance(record["birthplace"], str)
            and len(record["birthplace"].strip()) > 0
        ), f"{qid} must have a non-empty birthplace string"

        assert isinstance(record["occupations"], list), (
            f"{qid} occupations must be a list"
        )
        for occ in record["occupations"]:
            assert isinstance(occ, str) and len(occ.strip()) > 0

        for wiki_key in ("wiki_en", "wiki_uk", "wiki_ru"):
            url = record[wiki_key]
            if url is not None:
                assert isinstance(url, str) and HTTP_URL_PATTERN.match(url), (
                    f"Invalid URL scheme in {wiki_key} for {qid}: {url!r}"
                )

        assert (
            _is_finite_number(record["remarkability"])
            and float(record["remarkability"]) >= 0.0
        ), f"Invalid remarkability score for {qid}: {record['remarkability']!r}"


def test_search_index_structure_and_hygiene(dataset: WebDataBundle) -> None:
    """Validates search_index.json locations, zones, tuple layouts, and lowercased searchString."""
    assert isinstance(dataset.search_index, dict)
    assert set(dataset.search_index.keys()) == {"locations", "people"}

    locations = dataset.search_index["locations"]
    assert isinstance(locations, dict)
    assert "zones" in locations and isinstance(locations["zones"], list)

    zones = locations["zones"]
    assert len(zones) == len(set(zones)), (
        "search_index.locations.zones contains duplicate entries"
    )
    for zone in zones:
        assert isinstance(zone, str) and len(zone.strip()) > 0

    for loc_key, tuple_val in locations.items():
        if loc_key == "zones":
            continue
        assert isinstance(loc_key, str) and len(loc_key.strip()) > 0
        assert isinstance(tuple_val, list) and len(tuple_val) == 4, (
            f"Location {loc_key!r} must be [lat, lon, region, birthplace]"
        )
        lat, lon, region, birthplace = tuple_val
        assert _is_valid_lat_lon(lat, lon)
        assert isinstance(region, str)
        assert isinstance(birthplace, str) and len(birthplace.strip()) > 0

    people_rows = dataset.search_index["people"]
    assert isinstance(people_rows, list) and len(people_rows) > 0

    seen_search_ids: set[str] = set()
    for idx, row in enumerate(people_rows):
        assert isinstance(row, list) and len(row) == 6, (
            f"search_index.people[{idx}] expected length 6, got {len(row)}"
        )
        qid, search_string, default_name, loc_key, birthplace, country = row

        assert isinstance(qid, str) and QID_PATTERN.match(qid)
        assert qid not in seen_search_ids, (
            f"Duplicate Q-ID in search_index.people: {qid}"
        )
        seen_search_ids.add(qid)

        assert isinstance(search_string, str) and len(search_string.strip()) > 0
        assert search_string == search_string.lower(), (
            f"searchString for {qid} must be pre-lowercased: {search_string!r}"
        )
        assert isinstance(default_name, str) and len(default_name.strip()) > 0
        assert loc_key is None or (
            isinstance(loc_key, str) and len(loc_key.strip()) > 0
        )
        assert isinstance(birthplace, str) and len(birthplace.strip()) > 0
        assert isinstance(country, str) and len(country.strip()) > 0


def test_cross_file_referential_integrity(dataset: WebDataBundle) -> None:
    """Verifies 1-to-1 ID alignment and valid locKey references across all JSON files."""
    lite_by_id = {row[0]: row for row in dataset.map_lite}
    details_ids = set(dataset.people_details.keys())
    search_by_id = {row[0]: row for row in dataset.search_index["people"]}

    lite_ids = set(lite_by_id.keys())
    search_ids = set(search_by_id.keys())

    assert lite_ids == details_ids, (
        f"ID mismatch between map_lite and people_details: "
        f"only_in_lite={lite_ids - details_ids}, only_in_details={details_ids - lite_ids}"
    )
    assert lite_ids == search_ids, (
        f"ID mismatch between map_lite and search_index: "
        f"only_in_lite={lite_ids - search_ids}, only_in_search={search_ids - lite_ids}"
    )

    locations = dataset.search_index["locations"]
    valid_loc_keys = {k for k in locations if k != "zones"}

    for qid, lite_row in lite_by_id.items():
        lite_loc_key = lite_row[4]
        search_loc_key = search_by_id[qid][3]
        assert lite_loc_key == search_loc_key, (
            f"locKey mismatch for {qid}: map_lite={lite_loc_key!r} vs search_index={search_loc_key!r}"
        )

        if lite_loc_key is not None:
            assert lite_loc_key in valid_loc_keys, (
                f"{qid} references missing locKey {lite_loc_key!r} in search_index.locations"
            )
            loc_lat, loc_lon = locations[lite_loc_key][0], locations[lite_loc_key][1]
            assert math.isclose(float(lite_row[1]), float(loc_lat), abs_tol=1e-5)
            assert math.isclose(float(lite_row[2]), float(loc_lon), abs_tol=1e-5)
