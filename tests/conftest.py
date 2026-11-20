"""
===============================================================================
conftest.py — Pytest Fixtures for Web JSON Data Contract Validation
-------------------------------------------------------------------------------
Provides two dataset targets to `test_web_contracts.py`:
  1. `synthetic_5_people`: An in-memory 5-person dataset covering edge cases
     (multi-category people, shared city coordinates, diaspora birthplaces,
     trilingual names, optional portraits, and nullable wiki URLs) so CI and
     fresh clones can always verify contract assertions.
  2. `web_data_production`: Loads the real ETL artifacts from `web/data/`
     whenever all 4 JSON files exist on disk, skipping cleanly beforehand.
===============================================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import orjson
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WEB_DATA_DIR = PROJECT_ROOT / "web" / "data"

REQUIRED_FILES = (
    "map_lite.json",
    "topic_map.json",
    "people_details.json",
    "search_index.json",
)


@dataclass(frozen=True)
class WebDataBundle:
    """Container holding all 4 parsed JSON payloads for a single test run."""

    source_name: str
    map_lite: list[Any]
    topic_map: dict[str, Any]
    people_details: dict[str, Any]
    search_index: dict[str, Any]


def _build_synthetic_bundle() -> WebDataBundle:
    """Constructs a valid 5-person dataset exercising all schema invariants."""
    map_lite = [
        [
            "Q217198",
            44.9484,
            34.1,
            [["Sports & Recreation", "Racquet & Precision Sports"]],
            "Simferopol_44.9484_34.1",
            ["Ukraine", "Autonomous Republic of Crimea", "Simferopol"],
        ],
        [
            "Q219976",
            50.45,
            30.5236,
            [
                ["Sports & Recreation", "Coaches & Officials"],
                ["Sports & Recreation", "Team Sports"],
            ],
            "Kyiv_50.45_30.5236",
            ["Ukraine", "Kyiv"],
        ],
        [
            "Q134958",
            50.45,
            30.5236,
            [["Culture & Arts", "Literature"]],
            "Kyiv_50.45_30.5236",
            ["Ukraine", "Kyiv"],
        ],
        [
            "Q43499",
            49.842,
            24.0316,
            [
                ["Culture & Arts", "Literature"],
                ["Science & Academia", "Humanities & History"],
            ],
            "Lviv_49.842_24.0316",
            ["Ukraine", "Lviv Oblast", "Lviv"],
        ],
        [
            "Q312456",
            52.2297,
            21.0122,
            [["Politics & Society", "Diplomacy & Law"]],
            "Warsaw_52.2297_21.0122",
            ["Poland", "Masovian Voivodeship", "Warsaw"],
        ],
    ]

    topic_map = {
        "Culture & Arts": {
            "Literature": 2,
        },
        "Politics & Society": {
            "Diplomacy & Law": 1,
        },
        "Science & Academia": {
            "Humanities & History": 1,
        },
        "Sports & Recreation": {
            "Coaches & Officials": 1,
            "Racquet & Precision Sports": 1,
            "Team Sports": 1,
        },
    }

    people_details = {
        "Q217198": {
            "name_en": "Sergey Karjakin",
            "name_uk": "Сергій Карякін",
            "name_ru": "Сергей Карякин",
            "image": "Sergey_Karjakin_2018.jpg",
            "birthplace": "Simferopol",
            "occupations": ["chess player"],
            "wiki_en": "https://en.wikipedia.org/wiki/Sergey_Karjakin",
            "wiki_uk": "https://uk.wikipedia.org/wiki/%D0%9A%D0%B0%D1%80%D1%8F%D0%BA%D1%96%D0%BD_%D0%A1%D0%B5%D1%80%D0%B3%D1%96%D0%B9",
            "wiki_ru": "https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%80%D1%8F%D0%BA%D0%B8%D0%BD,_%D0%A1%D0%B5%D1%80%D0%B3%D0%B5%D0%B9",
            "remarkability": 64,
        },
        "Q219976": {
            "name_en": "Oleksandr Shovkovskyi",
            "name_uk": "Шовковський Олександр Володимирович",
            "name_ru": "Александр Владимирович Шовковский",
            "image": "Oleksandr_Shovkovskyi.jpg",
            "birthplace": "Kyiv",
            "occupations": [
                "association football player",
                "association football coach",
            ],
            "wiki_en": "https://en.wikipedia.org/wiki/Oleksandr_Shovkovskyi",
            "wiki_uk": "https://uk.wikipedia.org/wiki/%D0%A8%D0%BE%D0%B2%D0%BA%D0%BE%D0%B2%D1%81%D1%8C%D0%BA%D0%B8%D0%B9_%D0%9E%D0%BB%D0%B5%D0%BA%D1%81%D0%B0%D0%BD%D0%B4%D1%80",
            "wiki_ru": None,
            "remarkability": 50,
        },
        "Q134958": {
            "name_en": "Lesya Ukrainka",
            "name_uk": "Леся Українка",
            "name_ru": "Леся Украинка",
            "image": "Lesya_Ukrainka_portrait.jpg",
            "birthplace": "Kyiv",
            "occupations": ["poet", "playwright", "translator"],
            "wiki_en": "https://en.wikipedia.org/wiki/Lesya_Ukrainka",
            "wiki_uk": "https://uk.wikipedia.org/wiki/%D0%9B%D0%B5%D1%81%D1%8F_%D0%A3%D0%BA%D1%80%D0%B0%D1%97%D0%BD%D0%BA%D0%B0",
            "wiki_ru": "https://ru.wikipedia.org/wiki/%D0%9B%D0%B5%D1%81%D1%8F_%D0%A3%D0%BA%D1%80%D0%B0%D0%B8%D0%BD%D0%BA%D0%B0",
            "remarkability": 95,
        },
        "Q43499": {
            "name_en": "Ivan Franko",
            "name_uk": "Іван Франко",
            "name_ru": "Иван Франко",
            "image": None,
            "birthplace": "Lviv",
            "occupations": ["writer", "scholar", "journalist"],
            "wiki_en": "https://en.wikipedia.org/wiki/Ivan_Franko",
            "wiki_uk": "https://uk.wikipedia.org/wiki/%D0%86%D0%B2%D0%B0%D0%BD_%D0%A4%D1%80%D0%B0%D0%BD%D0%BA%D0%BE",
            "wiki_ru": None,
            "remarkability": 92,
        },
        "Q312456": {
            "name_en": None,
            "name_uk": "Олександр Ковальов",
            "name_ru": None,
            "image": None,
            "birthplace": "Warsaw",
            "occupations": ["diplomat"],
            "wiki_en": None,
            "wiki_uk": "https://uk.wikipedia.org/wiki/Example_Diplomat",
            "wiki_ru": None,
            "remarkability": 18.5,
        },
    }

    search_index = {
        "locations": {
            "Simferopol_44.9484_34.1": [
                44.9484,
                34.1,
                "Autonomous Republic of Crimea",
                "Simferopol",
            ],
            "Kyiv_50.45_30.5236": [50.45, 30.5236, "Kyiv", "Kyiv"],
            "Lviv_49.842_24.0316": [49.842, 24.0316, "Lviv Oblast", "Lviv"],
            "Warsaw_52.2297_21.0122": [
                52.2297,
                21.0122,
                "Masovian Voivodeship",
                "Warsaw",
            ],
            "zones": [
                "Autonomous Republic of Crimea",
                "Kyiv",
                "Lviv Oblast",
                "Masovian Voivodeship",
                "Poland",
                "Ukraine",
            ],
        },
        "people": [
            [
                "Q134958",
                "lesya ukrainka леся українка леся украинка",
                "Lesya Ukrainka",
                "Kyiv_50.45_30.5236",
                "Kyiv",
                "Ukraine",
            ],
            [
                "Q43499",
                "ivan franko іван франко иван франко",
                "Ivan Franko",
                "Lviv_49.842_24.0316",
                "Lviv",
                "Ukraine",
            ],
            [
                "Q217198",
                "sergey karjakin сергій карякін сергей карякин",
                "Sergey Karjakin",
                "Simferopol_44.9484_34.1",
                "Simferopol",
                "Ukraine",
            ],
            [
                "Q219976",
                "oleksandr shovkovskyi шовковський олександр володимирович александр владимирович шовковский",
                "Oleksandr Shovkovskyi",
                "Kyiv_50.45_30.5236",
                "Kyiv",
                "Ukraine",
            ],
            [
                "Q312456",
                "олександр ковальов",
                "Олександр Ковальов",
                "Warsaw_52.2297_21.0122",
                "Warsaw",
                "Poland",
            ],
        ],
    }

    return WebDataBundle(
        source_name="synthetic_5_people",
        map_lite=map_lite,
        topic_map=topic_map,
        people_details=people_details,
        search_index=search_index,
    )


def _load_production_bundle() -> WebDataBundle:
    """Loads the 4 production JSON files from `web/data/` using orjson."""
    return WebDataBundle(
        source_name="web_data_production",
        map_lite=orjson.loads((WEB_DATA_DIR / "map_lite.json").read_bytes()),
        topic_map=orjson.loads((WEB_DATA_DIR / "topic_map.json").read_bytes()),
        people_details=orjson.loads(
            (WEB_DATA_DIR / "people_details.json").read_bytes()
        ),
        search_index=orjson.loads((WEB_DATA_DIR / "search_index.json").read_bytes()),
    )


@pytest.fixture(
    params=[
        pytest.param("synthetic", id="synthetic_5_people"),
        pytest.param("production", id="web_data_production"),
    ]
)
def dataset(request: pytest.FixtureRequest) -> WebDataBundle:
    """Yields each available dataset bundle for contract verification."""
    if request.param == "synthetic":
        return _build_synthetic_bundle()

    missing = [name for name in REQUIRED_FILES if not (WEB_DATA_DIR / name).is_file()]
    if missing:
        pytest.skip(
            f"Production artifacts not yet present in web/data/ (missing: {', '.join(missing)})"
        )

    return _load_production_bundle()
