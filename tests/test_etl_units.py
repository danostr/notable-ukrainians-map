"""
Unit tests for ETL parsing, hygiene, geographic normalization, and taxonomy config.
Runs completely offline in < 0.2s.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ETL_DIR = PROJECT_ROOT / "etl"


def _load_etl_module(filename: str, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, ETL_DIR / filename)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


extract_mod = _load_etl_module("01_extract.py", "etl_extract")
clean_mod = _load_etl_module("02_clean_geocode.py", "etl_clean")
cat_mod = _load_etl_module("03_categorize.py", "etl_categorize")


# =============================================================================
# 01_extract.py Unit Tests
# =============================================================================


def test_parse_wkt_point_swaps_lon_lat_to_lat_lon():
    lat, lon = extract_mod._parse_wkt_point("Point(30.5236 50.45)")
    assert lat == pytest.approx(50.45)
    assert lon == pytest.approx(30.5236)
    assert extract_mod._parse_wkt_point("invalid") == (None, None)
    assert extract_mod._parse_wkt_point(None) == (None, None)


def test_extract_image_filename_decodes_commons_uri():
    uri = "http://commons.wikimedia.org/wiki/Special:FilePath/Taras%20Shevchenko%201859.jpg"
    assert extract_mod._extract_image_filename(uri) == "Taras Shevchenko 1859.jpg"
    assert extract_mod._extract_image_filename("File:Portrait.png") == "Portrait.png"
    assert extract_mod._extract_image_filename(None) is None


def test_parse_sparql_json_handles_unescaped_control_chars():
    # Wikidata occasionally emits raw tabs/control bytes inside JSON string literals
    malformed = b'{"results": {"bindings": [{"name": {"value": "Kyiv\tCity"}}]}}'
    parsed = extract_mod._parse_sparql_json(malformed)
    assert parsed["results"]["bindings"][0]["name"]["value"] == "Kyiv\tCity"


# =============================================================================
# 02_clean_geocode.py Unit Tests
# =============================================================================


@pytest.mark.parametrize(
    ("raw_region", "expected_qid"),
    [
        ("Kiev Governorate", "Q170036"),
        ("Kingdom of Galicia and Lodomeria", "Q164193"),
        ("Taurida Governorate", "Q756294"),
        ("Подольская губерния", "Q166709"),
        ("Буковина", "Q168856"),
    ],
)
def test_historical_region_mapping(raw_region: str, expected_qid: str):
    assert clean_mod._match_historical_region(raw_region) == expected_qid


def test_nearest_ukrainian_region_fallback():
    # Coordinates near Kharkiv -> Kharkiv Oblast (Q170666)
    assert clean_mod._nearest_ukrainian_region(49.99, 36.23) == "Q170666"
    # Coordinates near Lviv -> Lviv Oblast (Q164193)
    assert clean_mod._nearest_ukrainian_region(49.84, 24.03) == "Q164193"


# =============================================================================
# 03_categorize.py & category_map.json Unit Tests
# =============================================================================


def test_category_map_json_has_no_duplicate_qids_and_valid_pairs():
    taxonomy_df = cat_mod.load_taxonomy_df(cat_mod.CONFIG_PATH)
    assert taxonomy_df.height >= 200
    assert taxonomy_df["occupation_qid"].n_unique() == taxonomy_df.height
    assert (
        taxonomy_df.filter(
            (taxonomy_df["main_category"] == "") | (taxonomy_df["sub_category"] == "")
        ).height
        == 0
    )
