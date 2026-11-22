"""
Unit and offline end-to-end pipeline tests for ETL stages 01–04.
Runs completely offline in < 0.5s without touching production files.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import orjson
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
export_mod = _load_etl_module("04_export_web_json.py", "etl_export")


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
    malformed = b'{"results": {"bindings": [{"name": {"value": "Kyiv\tCity"}}]}}'
    parsed = extract_mod._parse_sparql_json(malformed)
    assert parsed["results"]["bindings"][0]["name"]["value"] == "Kyiv\tCity"


def test_build_query_slices_and_sparql_generation():
    slices = extract_mod.build_query_slices()
    labels = [s.label for s in slices]
    assert labels[0] == "pre_1800"
    assert labels[-1] == "no_birthdate"

    born_sparql = extract_mod.build_sparql_query(slices[0], "born")
    citizen_sparql = extract_mod.build_sparql_query(slices[0], "citizen")
    assert "wdt:P19 / wdt:P131* wd:Q212" in born_sparql
    assert "wdt:P27 wd:Q212" in citizen_sparql

    with pytest.raises(ValueError):
        extract_mod.build_sparql_query(slices[0], "invalid_source")


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
    assert (
        clean_mod._nearest_ukrainian_region(49.99, 36.23) == "Q170666"
    )  # Kharkiv Oblast
    assert clean_mod._nearest_ukrainian_region(49.84, 24.03) == "Q164193"  # Lviv Oblast


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


def test_load_taxonomy_rejects_duplicate_qids(tmp_path: Path):
    bad_config = tmp_path / "bad_category_map.json"
    bad_config.write_bytes(
        orjson.dumps(
            {
                "Cat A": {"Sub 1": {"Q49757": "poet"}},
                "Cat B": {"Sub 2": {"Q49757": "duplicate poet"}},
            }
        )
    )
    with pytest.raises(ValueError, match="Duplicate occupation Q-ID"):
        cat_mod.load_taxonomy_df(bad_config)


# =============================================================================
# End-to-End Offline ETL Pipeline Test (Stages 01 -> 02 -> 03 -> 04)
# =============================================================================


def test_full_etl_pipeline_offline_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    web_data_dir = tmp_path / "web_data"
    raw_dir.mkdir()
    processed_dir.mkdir()
    web_data_dir.mkdir()

    # 1. Seed a synthetic raw SPARQL slice with:
    #    - Q134958 (2 rows -> deduplicated into 2 occupations: poet + painter)
    #    - Q1001   (unmapped occupation -> [["Other", "Unknown"]] + leaked Q-ID label)
    #    - Q1002   (diaspora figure born in New York City)
    #    - Q1003   (Null Island 0.0, 0.0 -> dropped)
    #    - Q1004   (no usable name across EN/UK/RU -> dropped)
    slice_payload = {
        "_meta": {"slice_label": "1800_1849", "source_query": "born"},
        "results": {
            "bindings": [
                {
                    "person": {"value": "http://www.wikidata.org/entity/Q134958"},
                    "name_en": {"value": "Taras Shevchenko"},
                    "name_uk": {"value": "Тарас Шевченко"},
                    "name_ru": {"value": "Тарас Шевченко"},
                    "image": {
                        "value": "http://commons.wikimedia.org/wiki/Special:FilePath/Taras%20Shevchenko.jpg"
                    },
                    "wiki_en": {
                        "value": "https://en.wikipedia.org/wiki/Taras_Shevchenko"
                    },
                    "wiki_uk": {"value": "https://uk.wikipedia.org/wiki/Shevchenko"},
                    "wiki_ru": {"value": "https://ru.wikipedia.org/wiki/Shevchenko"},
                    "birthplace": {"value": "http://www.wikidata.org/entity/Q4307281"},
                    "birthplace_en": {"value": "Moryntsi"},
                    "birthplace_uk": {"value": "Моринці"},
                    "coord": {"value": "Point(30.9969 49.2519)"},
                    "occupation": {"value": "http://www.wikidata.org/entity/Q49757"},
                    "occupation_en": {"value": "poet"},
                    "sitelinks": {"value": "120"},
                },
                {
                    "person": {"value": "http://www.wikidata.org/entity/Q134958"},
                    "name_en": {"value": "Taras Shevchenko"},
                    "birthplace": {"value": "http://www.wikidata.org/entity/Q4307281"},
                    "coord": {"value": "Point(30.9969 49.2519)"},
                    "occupation": {"value": "http://www.wikidata.org/entity/Q1028181"},
                    "occupation_en": {"value": "painter"},
                    "sitelinks": {"value": "120"},
                },
                {
                    "person": {"value": "http://www.wikidata.org/entity/Q1001"},
                    "name_en": {"value": "Ivan Petrenko"},
                    "name_uk": {"value": "Іван Петренко"},
                    "wiki_uk": {"value": "https://uk.wikipedia.org/wiki/Ivan"},
                    "birthplace": {"value": "http://www.wikidata.org/entity/Q1899"},
                    "birthplace_en": {"value": "Q1899"},  # Leaked Q-ID must be stripped
                    "birthplace_uk": {"value": "Київ"},
                    "coord": {"value": "Point(30.5234 50.4501)"},
                    "occupation": {"value": "http://www.wikidata.org/entity/Q99999999"},
                    "occupation_en": {"value": "rare craftsperson"},
                    "sitelinks": {"value": "5"},
                },
                {
                    "person": {"value": "http://www.wikidata.org/entity/Q1002"},
                    "name_en": {"value": "Anna Kovalenko"},
                    "wiki_en": {"value": "https://en.wikipedia.org/wiki/Anna"},
                    "birthplace": {"value": "http://www.wikidata.org/entity/Q60"},
                    "birthplace_en": {"value": "New York City"},
                    "coord": {"value": "Point(-74.006 40.7128)"},
                    "occupation": {"value": "http://www.wikidata.org/entity/Q39631"},
                    "occupation_en": {"value": "physician"},
                    "sitelinks": {"value": "15"},
                },
                {
                    "person": {"value": "http://www.wikidata.org/entity/Q1003"},
                    "name_en": {"value": "Null Island Record"},
                    "wiki_en": {"value": "https://en.wikipedia.org/wiki/Null"},
                    "birthplace": {"value": "http://www.wikidata.org/entity/Q1899"},
                    "coord": {"value": "Point(0.0 0.0)"},
                    "sitelinks": {"value": "2"},
                },
                {
                    "person": {"value": "http://www.wikidata.org/entity/Q1004"},
                    "name_en": {"value": "Q1004"},
                    "wiki_en": {"value": "https://en.wikipedia.org/wiki/NoName"},
                    "birthplace": {"value": "http://www.wikidata.org/entity/Q1899"},
                    "coord": {"value": "Point(30.5234 50.4501)"},
                    "sitelinks": {"value": "1"},
                },
            ]
        },
    }
    (raw_dir / "slice_1800_1849_born.json").write_bytes(orjson.dumps(slice_payload))

    # 2. Pre-seed places_hierarchy.json cache so stage 02 makes 0 network calls
    places_cache = raw_dir / "places_hierarchy.json"
    places_cache.write_bytes(
        orjson.dumps(
            {
                "Q4307281": {
                    "place_en": "Moryntsi",
                    "place_uk": "Моринці",
                    "place_ru": "Моринцы",
                    "is_ukraine": True,
                    "country_qid": "Q212",
                    "country_en": "Ukraine",
                    "country_uk": "Україна",
                    "country_ru": "Украина",
                    "ua_region_qid": "Q161808",
                    "admin1_en": "Zvenyhorodka Raion",
                    "admin1_uk": "Звенигородський район",
                    "admin1_ru": "Звенигородский район",
                },
                "Q1899": {
                    "place_en": "Kyiv",
                    "place_uk": "Київ",
                    "place_ru": "Киев",
                    "is_ukraine": True,
                    "country_qid": "Q212",
                    "country_en": "Ukraine",
                    "country_uk": "Україна",
                    "country_ru": "Украина",
                    "ua_region_qid": "Q1899",
                    "admin1_en": None,
                    "admin1_uk": None,
                    "admin1_ru": None,
                },
                "Q60": {
                    "place_en": "New York City",
                    "place_uk": "Нью-Йорк",
                    "place_ru": "Нью-Йорк",
                    "is_ukraine": False,
                    "country_qid": "Q30",
                    "country_en": "United States",
                    "country_uk": "США",
                    "country_ru": "США",
                    "ua_region_qid": None,
                    "admin1_en": "New York",
                    "admin1_uk": "Нью-Йорк",
                    "admin1_ru": "Нью-Йорк",
                },
            }
        )
    )

    p01 = processed_dir / "01_raw_people.parquet"
    p02 = processed_dir / "02_clean_people.parquet"
    p03 = processed_dir / "03_categorized.parquet"

    # Stage 01: Consolidate raw JSON slice -> 01_raw_people.parquet
    df01 = extract_mod.consolidate_raw_slices(raw_dir, p01)
    assert df01.height == 6

    # Stage 02: Deduplicate, sanitize, and build trilingual GeoTrees -> 02_clean_people.parquet
    monkeypatch.setattr(clean_mod, "PARQUET_IN", p01)
    monkeypatch.setattr(clean_mod, "PARQUET_OUT", p02)
    monkeypatch.setattr(clean_mod, "PLACES_CACHE", places_cache)
    clean_mod.run_clean_geocode(refresh_places=False)

    # Stage 03: Categorize & compute remarkability -> 03_categorized.parquet
    monkeypatch.setattr(cat_mod, "PARQUET_IN", p02)
    monkeypatch.setattr(cat_mod, "PARQUET_OUT", p03)
    cat_mod.run_categorize()

    # Stage 04: Export minified JSON payloads -> tmp_path/web_data/
    monkeypatch.setattr(export_mod, "PARQUET_IN", p03)
    monkeypatch.setattr(export_mod, "WEB_DATA_DIR", web_data_dir)
    export_mod.export_web_json(skip_tests=True)

    # Verify exported artifacts
    map_lite = orjson.loads((web_data_dir / "map_lite.json").read_bytes())
    topic_map = orjson.loads((web_data_dir / "topic_map.json").read_bytes())
    people_details = orjson.loads((web_data_dir / "people_details.json").read_bytes())
    search_index = orjson.loads((web_data_dir / "search_index.json").read_bytes())

    # Q1003 (0,0) and Q1004 (unnamed) dropped -> 3 clean people remain
    assert len(map_lite) == 3
    assert set(people_details.keys()) == {"Q134958", "Q1001", "Q1002"}

    # Highest remarkability person (Q134958) must appear first in search_index
    assert search_index["people"][0][0] == "Q134958"
    assert "тарас шевченко" in search_index["people"][0][1]
    assert "моринцы" in search_index["people"][0][1]

    # Leaked "Q1899" birthplace label was sanitized to "Kyiv"
    assert people_details["Q1001"]["birthplace"] == "Kyiv"
    assert topic_map["Other"]["Unknown"] == 1
    assert topic_map["Culture & Arts"]["Literature"] == 1
    assert topic_map["Culture & Arts"]["Visual Arts"] == 1
