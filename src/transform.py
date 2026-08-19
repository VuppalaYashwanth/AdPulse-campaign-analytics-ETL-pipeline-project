"""
Transform stage.

Reads the two raw staging tables, cleans and unifies them into one common
schema, derives marketing metrics (CTR, CPC, CPA, ROAS), and writes the
result to data/processed/campaign_performance_clean.parquet for the
warehouse loader to pick up.

Cleaning steps applied:
  - type coercion (numeric fields to numeric, dates to date)
  - dedup on natural key (campaign, channel, date, geography)
  - null handling: numeric metric nulls -> 0 (an ad platform reporting a
    missing spend value for a given day means "no spend recorded", not
    "unknown" -- treating it as 0 is the correct business interpretation
    here, unlike e.g. a missing campaign_id which would be dropped instead)
  - campaign name casing standardized (title case)
  - derived metrics computed with divide-by-zero guarded
"""
import numpy as np
import pandas as pd
from sqlalchemy import text

from src.config import PROCESSED_DATA_DIR
from src.db import get_engine
from src.logging_config import setup_logging

logger = setup_logging("adpulse.transform")

REQUIRED_COLUMNS = [
    "campaign_id", "campaign_name", "channel", "report_date",
    "country", "region", "city",
    "impressions", "clicks", "spend", "conversions", "revenue",
]


def _read_csv_source(engine) -> pd.DataFrame:
    df = pd.read_sql(text("select * from staging.campaign_performance_csv"), engine)
    return df[[
        "campaign_id", "campaign_name", "channel", "report_date",
        "country", "region", "city",
        "impressions", "clicks", "spend", "conversions", "revenue",
    ]]


def _read_json_source(engine) -> pd.DataFrame:
    raw = pd.read_sql(text("select raw_payload from staging.campaign_performance_json"), engine)
    records = raw["raw_payload"].apply(pd.json_normalize).tolist()
    df = pd.concat(records, ignore_index=True) if records else pd.DataFrame()

    df = df.rename(columns={
        "campaign.id": "campaign_id",
        "campaign.name": "campaign_name",
        "channel.name": "channel",
        "date": "report_date",
        "location.country": "country",
        "location.region": "region",
        "location.city": "city",
        "metrics.impressions": "impressions",
        "metrics.clicks": "clicks",
        "metrics.spend_usd": "spend",
        "metrics.conversions": "conversions",
        "metrics.revenue_usd": "revenue",
    })
    return df[REQUIRED_COLUMNS]


def _coerce_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    numeric_cols = ["impressions", "clicks", "spend", "conversions", "revenue"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # missing metric values -> 0 (see module docstring for rationale)
    df[numeric_cols] = df[numeric_cols].fillna(0)

    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date

    # rows with no campaign_id or unparseable date are unusable -- drop, don't guess
    before = len(df)
    df = df.dropna(subset=["campaign_id", "report_date"])
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %d rows with missing campaign_id or unparseable date", dropped)

    df["campaign_id"] = df["campaign_id"].str.strip()
    df["campaign_name"] = df["campaign_name"].str.strip().str.title()
    df["channel"] = df["channel"].str.strip().str.lower()
    df["country"] = df["country"].str.strip()
    df["region"] = df["region"].str.strip()
    df["city"] = df["city"].str.strip()

    # dedup on natural key -- keep first occurrence
    key_cols = ["campaign_id", "channel", "report_date", "country", "region", "city"]
    before = len(df)
    df = df.drop_duplicates(subset=key_cols, keep="first")
    dupes = before - len(df)
    if dupes:
        logger.info("Removed %d duplicate rows on natural key", dupes)

    return df


def _derive_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ctr"] = np.where(df["impressions"] > 0, df["clicks"] / df["impressions"], 0.0)
    df["cpc"] = np.where(df["clicks"] > 0, df["spend"] / df["clicks"], 0.0)
    df["cpa"] = np.where(df["conversions"] > 0, df["spend"] / df["conversions"], 0.0)
    df["roas"] = np.where(df["spend"] > 0, df["revenue"] / df["spend"], 0.0)
    return df


def transform() -> pd.DataFrame:
    logger.info("Starting transform stage.")
    engine = get_engine()

    csv_df = _read_csv_source(engine)
    json_df = _read_json_source(engine)
    logger.info("Read %d rows from CSV staging, %d rows from JSON staging", len(csv_df), len(json_df))

    combined = pd.concat([csv_df, json_df], ignore_index=True)
    cleaned = _coerce_and_clean(combined)
    enriched = _derive_metrics(cleaned)

    out_path = PROCESSED_DATA_DIR / "campaign_performance_clean.parquet"
    enriched.to_parquet(out_path, index=False)
    logger.info("Transform stage complete: %d clean rows written to %s", len(enriched), out_path)

    return enriched


if __name__ == "__main__":
    transform()
