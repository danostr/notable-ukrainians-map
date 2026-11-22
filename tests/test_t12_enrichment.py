"""
tests/test_t12_enrichment.py — Unit & Contract Tests for T12 Data Enrichment
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, rel_path: str):
    spec = importlib.util.spec_from_file_location(name, PROJECT_ROOT / rel_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


clean_mod = _load_module("clean_geocode", "etl/02_clean_geocode.py")


def _sample_raw_df(rows: list[dict]) -> pl.DataFrame:
    base = {
        "qid": "Q100",
        "name_en": "Test Person",
        "name_uk": "Тестова Особа",
        "name_ru": "Тестовая Персона",
        "image": None,
        "wiki_en": "https://en.wikipedia.org/wiki/Test",
        "wiki_uk": None,
        "wiki_ru": None,
        "birthplace_qid": "Q171332",
        "birthplace_en": "Khmelnytskyi",
        "birthplace_uk": "Хмельницький",
        "birthplace_ru": "Хмельницкий",
        "lat": 49.4229,
        "lon": 26.9871,
        "occupation_qid": "Q36180",
        "occupation_en": "writer",
        "sitelinks_count": 25,
        "source_query": "born",
        "birth_date": "+1895-04-12T00:00:00Z",
        "death_date": "+1972-11-03T00:00:00Z",
        "esu_id": "12345",
        "ieu_id": "T/TestPerson",
        "viaf_id": "99887766",
    }
    full_rows = [{**base, **r} for r in rows]
    return pl.DataFrame(full_rows)


def test_parse_year_handles_ce_bce_and_invalid():
    assert clean_mod._parse_year("+1814-03-09T00:00:00Z") == 1814
    assert clean_mod._parse_year("-0450-01-01T00:00:00Z") == -450
    assert clean_mod._parse_year(None) is None
    assert clean_mod._parse_year("unknown") is None


def test_gazetteer_resolves_historical_settlement_names():
    gazetteer = {
        "historical_settlements": {
            "proskuriv": {
                "qid": "Q171332",
                "name_en": "Khmelnytskyi",
                "name_uk": "Хмельницький",
                "name_ru": "Хмельницкий",
                "lat": 49.4229,
                "lon": 26.9871,
            }
        },
        "country_centroid_outliers": [],
    }
    df = _sample_raw_df(
        [
            {
                "birthplace_qid": "Q999999",
                "birthplace_en": "Proskuriv",
                "birthplace_uk": "Проскурів",
                "lat": 49.4000,
                "lon": 26.9500,
            }
        ]
    )
    audited = clean_mod._apply_gazetteer_and_coord_audit(df, gazetteer)
    assert audited.height == 1
    assert audited["birthplace_qid"][0] == "Q171332"
    assert audited["birthplace_en"][0] == "Khmelnytskyi"
    assert audited["lat"][0] == pytest.approx(49.4229)


def test_swapped_ukrainian_coordinates_are_auto_repaired():
    gazetteer = {"historical_settlements": {}, "country_centroid_outliers": []}
    # Kyiv coordinates accidentally stored as (lat=30.5234, lon=50.4501)
    df = _sample_raw_df(
        [
            {
                "birthplace_qid": "Q1899",
                "birthplace_en": "Kyiv",
                "lat": 30.5234,
                "lon": 50.4501,
                "source_query": "born",
            }
        ]
    )
    audited = clean_mod._apply_gazetteer_and_coord_audit(df, gazetteer)
    assert audited.height == 1
    assert audited["lat"][0] == pytest.approx(50.4501)
    assert audited["lon"][0] == pytest.approx(30.5234)


def test_country_centroid_q212_outlier_is_rejected():
    gazetteer = {
        "historical_settlements": {},
        "country_centroid_outliers": [
            {"qid": "Q212", "lat": 49.0, "lon": 32.0, "radius_deg": 0.05}
        ],
    }
    df = _sample_raw_df(
        [
            {
                "birthplace_qid": "Q212",
                "birthplace_en": "Ukraine",
                "lat": 49.0,
                "lon": 32.0,
            }
        ]
    )
    audited = clean_mod._apply_gazetteer_and_coord_audit(df, gazetteer)
    assert audited.height == 0


def test_run_clean_geocode_prioritizes_village_over_oblast_and_sets_provenance(
    tmp_path, monkeypatch
):
    raw_in = tmp_path / "01_raw_people.parquet"
    clean_out = tmp_path / "02_clean_people.parquet"
    cache_json = tmp_path / "places_hierarchy.json"
    gaz_json = tmp_path / "historical_gazetteer.json"

    gaz_json.write_text(
        json.dumps({"historical_settlements": {}, "country_centroid_outliers": []}),
        encoding="utf-8",
    )
    cache_json.write_text(
        json.dumps(
            {
                "Q161808": {
                    "place_en": "Cherkasy Oblast",
                    "place_uk": "Черкаська область",
                    "place_ru": "Черкасская область",
                    "is_ukraine": True,
                    "country_qid": "Q212",
                    "ua_region_qid": "Q161808",
                },
                "Q2446750": {
                    "place_en": "Moryntsi",
                    "place_uk": "Моринці",
                    "place_ru": "Моринцы",
                    "is_ukraine": True,
                    "country_qid": "Q212",
                    "ua_region_qid": "Q161808",
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
                    "admin1_en": "New York",
                    "admin1_uk": "Нью-Йорк",
                    "admin1_ru": "Нью-Йорк",
                },
            }
        ),
        encoding="utf-8",
    )

    # Person 1 has both Oblast-level P19 and specific village P19 -> village must win
    # Person 2 is born abroad with Ukrainian heritage -> origin_type == ukrainian_nationality_or_heritage
    df = _sample_raw_df(
        [
            {
                "qid": "Q134958",
                "name_en": "Taras Shevchenko",
                "birthplace_qid": "Q161808",
                "birthplace_en": "Cherkasy Oblast",
                "lat": 49.4444,
                "lon": 32.0598,
                "source_query": "born",
            },
            {
                "qid": "Q134958",
                "name_en": "Taras Shevchenko",
                "birthplace_qid": "Q2446750",
                "birthplace_en": "Moryntsi",
                "lat": 49.2478,
                "lon": 31.0014,
                "source_query": "born",
            },
            {
                "qid": "Q55555",
                "name_en": "Diaspora Scholar",
                "birthplace_qid": "Q60",
                "birthplace_en": "New York City",
                "lat": 40.7128,
                "lon": -74.0060,
                "source_query": "heritage",
            },
        ]
    )
    df.write_parquet(raw_in)

    monkeypatch.setattr(clean_mod, "PARQUET_IN", raw_in)
    monkeypatch.setattr(clean_mod, "PARQUET_OUT", clean_out)
    monkeypatch.setattr(clean_mod, "PLACES_CACHE", cache_json)
    monkeypatch.setattr(clean_mod, "GAZETTEER_PATH", gaz_json)

    clean_mod.run_clean_geocode(refresh_places=False)

    out = pl.read_parquet(clean_out)
    shevchenko = out.filter(pl.col("qid") == "Q134958").to_dicts()[0]
    diaspora = out.filter(pl.col("qid") == "Q55555").to_dicts()[0]

    assert shevchenko["birthplace_qid"] == "Q2446750"
    assert shevchenko["birthplace_en"] == "Moryntsi"
    assert "Q2446750" in shevchenko["loc_key"]
    assert shevchenko["born_in_ukraine"] is True
    assert shevchenko["origin_type"] == "born_in_ukraine"
    assert shevchenko["birth_year"] == 1895
    assert shevchenko["esu_id"] == "12345"

    assert diaspora["born_in_ukraine"] is False
    assert diaspora["ukrainian_nationality_or_heritage"] is True
    assert diaspora["origin_type"] == "ukrainian_nationality_or_heritage"
    assert "ethnic_group_p172" in diaspora["provenance_tags"]
