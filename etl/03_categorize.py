"""
===============================================================================
03_categorize.py — Occupation Taxonomy Mapping & Remarkability Scoring
-------------------------------------------------------------------------------
Transforms `etl/processed/02_clean_people.parquet` into
`etl/processed/03_categorized.parquet`.

Pipeline Steps:
  1. Taxonomy Join (`etl/config/category_map.json`):
     - Flattens the version-controlled `{MainCategory: {SubCategory: {qid: label}}}`
       config into a lookup DataFrame and validates zero duplicate Q-ID mappings.
     - Explodes each person's `occupation_qids`, joins against the taxonomy, and
       aggregates deduplicated `[MainCategory, SubCategory]` pairs per person.
     - Assigns `[["Other", "Unknown"]]` to any person with zero mapped
       occupations so the frontend's non-empty category contract is guaranteed.
  2. Unmapped Occupation Diagnostics:
     - Computes and logs the top 50 unmapped Wikidata `P106` occupation Q-IDs
       (with English labels and person counts) so coverage gaps can be closed
       iteratively in `category_map.json`.
  3. Deterministic `remarkability` Score (`Float64`):
     - Combines `sitelinks_count` with weighted language-coverage bonuses
       (`wiki_uk`: +5.0, `wiki_en`: +3.0, `wiki_ru`: +2.0, `image`: +1.0) so
       figures central to Ukrainian historiography rank prominently in city and
       region lists even when they have fewer international stubs.
===============================================================================
"""

from __future__ import annotations

from pathlib import Path

import orjson
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "etl" / "config" / "category_map.json"
PROCESSED_DIR = PROJECT_ROOT / "etl" / "processed"

PARQUET_IN = PROCESSED_DIR / "02_clean_people.parquet"
PARQUET_OUT = PROCESSED_DIR / "03_categorized.parquet"


def load_taxonomy_df(config_path: Path) -> pl.DataFrame:
    """Flattens `category_map.json` into a validated Polars lookup table."""
    if not config_path.is_file():
        raise FileNotFoundError(f"Missing taxonomy config: {config_path}")

    raw_config: dict[str, dict[str, dict[str, str]]] = orjson.loads(
        config_path.read_bytes()
    )

    rows: list[dict[str, str]] = []
    seen_qids: dict[str, tuple[str, str]] = {}

    for main_cat, sub_map in raw_config.items():
        if not isinstance(sub_map, dict):
            raise TypeError(f"Expected dict under main category {main_cat!r}")
        for sub_cat, qid_map in sub_map.items():
            if not isinstance(qid_map, dict):
                raise TypeError(
                    f"Expected dict of Q-IDs under [{main_cat} > {sub_cat}]"
                )
            for occ_qid, readable_label in qid_map.items():
                if occ_qid in seen_qids:
                    prev = seen_qids[occ_qid]
                    raise TypeError(
                        f"Duplicate occupation Q-ID {occ_qid} ({readable_label!r}): "
                        f"mapped to both {prev} and {(main_cat, sub_cat)}"
                    )
                seen_qids[occ_qid] = (main_cat, sub_cat)
                rows.append(
                    {
                        "occupation_qid": occ_qid,
                        "main_category": main_cat.strip(),
                        "sub_category": sub_cat.strip(),
                    }
                )

    return pl.DataFrame(
        rows,
        schema={
            "occupation_qid": pl.String,
            "main_category": pl.String,
            "sub_category": pl.String,
        },
    )


def log_unmapped_diagnostics(
    people_df: pl.DataFrame,
    taxonomy_df: pl.DataFrame,
    top_n: int = 50,
) -> None:
    """Logs the most frequent unmapped occupation Q-IDs with English labels."""
    # Pair `occupation_qids` with `occupation_labels` only where list lengths match,
    # and also count Q-IDs directly across all people so counts are exact.
    exploded_qids = (
        people_df.select(["qid", "occupation_qids"])
        .explode("occupation_qids")
        .rename({"occupation_qids": "occupation_qid"})
        .filter(pl.col("occupation_qid").is_not_null())
    )

    unmapped = exploded_qids.join(taxonomy_df, on="occupation_qid", how="anti")
    top_unmapped = (
        unmapped.group_by("occupation_qid")
        .agg(pl.len().alias("person_count"))
        .sort(["person_count", "occupation_qid"], descending=[True, False])
        .head(top_n)
    )

    if top_unmapped.height == 0:
        print("\n=== Unmapped Occupations Diagnostic ===")
        print("Zero unmapped occupations found!")
        return

    # Recover best available English label for each unmapped Q-ID from rows where
    # occupation_qids and occupation_labels align in length
    aligned_labels = (
        people_df.filter(
            pl.col("occupation_qids").list.len()
            == pl.col("occupation_labels").list.len()
        )
        .select(["occupation_qids", "occupation_labels"])
        .explode(["occupation_qids", "occupation_labels"])
        .rename(
            {
                "occupation_qids": "occupation_qid",
                "occupation_labels": "occupation_label",
            }
        )
        .filter(
            pl.col("occupation_qid").is_not_null()
            & pl.col("occupation_label").is_not_null()
        )
        .group_by("occupation_qid")
        .agg(pl.col("occupation_label").first().alias("occupation_label"))
    )

    report = top_unmapped.join(aligned_labels, on="occupation_qid", how="left").sort(
        ["person_count", "occupation_qid"], descending=[True, False]
    )

    print(f"\n=== Top {report.height} Unmapped Occupation Q-IDs ===")
    for row in report.iter_rows(named=True):
        label = row["occupation_label"] or "<no English label>"
        print(f"  {row['occupation_qid']:<12} | {row['person_count']:>5,} | {label}")


def run_categorize() -> None:
    """Assigns two-level categories and remarkability scores via Polars."""
    if not PARQUET_IN.is_file():
        raise FileNotFoundError(f"Missing input Parquet file: {PARQUET_IN}")

    people_df = pl.read_parquet(PARQUET_IN)
    taxonomy_df = load_taxonomy_df(CONFIG_PATH)

    # 1. Explode `occupation_qids` and inner-join against taxonomy
    mapped_pairs = (
        people_df.select(["qid", "occupation_qids"])
        .explode("occupation_qids")
        .rename({"occupation_qids": "occupation_qid"})
        .join(taxonomy_df, on="occupation_qid", how="inner")
        .select(["qid", "main_category", "sub_category"])
        .unique()
        .sort(["qid", "main_category", "sub_category"])
        .with_columns(
            pl.concat_list([pl.col("main_category"), pl.col("sub_category")]).alias(
                "category_pair"
            )
        )
        .group_by("qid", maintain_order=True)
        .agg(pl.col("category_pair").alias("categories"))
    )

    # 2. Left-join back onto all people and default unmapped records to [["Other", "Unknown"]]
    fallback_categories = pl.lit(
        [["Other", "Unknown"]], dtype=pl.List(pl.List(pl.String))
    )

    categorized_df = (
        people_df.join(mapped_pairs, on="qid", how="left")
        .with_columns(
            pl.when(
                pl.col("categories").is_null() | (pl.col("categories").list.len() == 0)
            )
            .then(fallback_categories)
            .otherwise(pl.col("categories"))
            .alias("categories")
        )
        .with_columns(
            (
                pl.col("sitelinks_count").cast(pl.Float64)
                + pl.when(pl.col("wiki_uk").is_not_null()).then(5.0).otherwise(0.0)
                + pl.when(pl.col("wiki_en").is_not_null()).then(3.0).otherwise(0.0)
                + pl.when(pl.col("wiki_ru").is_not_null()).then(2.0).otherwise(0.0)
                + pl.when(pl.col("image").is_not_null()).then(1.0).otherwise(0.0)
            ).alias("remarkability")
        )
        .sort(
            by=["remarkability", "sitelinks_count", "qid"],
            descending=[True, True, False],
        )
    )

    PARQUET_OUT.parent.mkdir(parents=True, exist_ok=True)
    categorized_df.write_parquet(PARQUET_OUT, compression="zstd")

    total_people = categorized_df.height
    other_count = categorized_df.filter(
        pl.col("categories") == fallback_categories
    ).height
    other_pct = (other_count / total_people * 100.0) if total_people else 0.0

    # Category breakdown summary
    cat_summary = (
        categorized_df.select(["qid", "categories"])
        .explode("categories")
        .with_columns(
            [
                pl.col("categories").list.get(0).alias("main_category"),
                pl.col("categories").list.get(1).alias("sub_category"),
            ]
        )
        .group_by(["main_category", "sub_category"])
        .agg(pl.len().alias("people_count"))
        .sort(["main_category", "people_count"], descending=[False, True])
    )

    print("\n=== T5 Categorization & Remarkability Summary ===")
    print(f"Total people processed   : {total_people:,}")
    print(
        f"Mapped to named category : {total_people - other_count:,} ({100.0 - other_pct:.1f}%)"
    )
    print(f"Fallback [Other/Unknown] : {other_count:,} ({other_pct:.1f}%)")
    print("\n=== Category Breakdown ===")
    for row in cat_summary.iter_rows(named=True):
        print(
            f"  {row['main_category']:<22} > {row['sub_category']:<26} : {row['people_count']:>6,}"
        )

    log_unmapped_diagnostics(people_df, taxonomy_df, top_n=50)
    print(f"\nOutput artifact -> {PARQUET_OUT.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    run_categorize()
