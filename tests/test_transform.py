"""
Unit tests for the pure/deterministic parts of transform.py:
_coerce_and_clean and _derive_metrics. These don't touch Postgres --
they operate on in-memory DataFrames so they're fast and can run in CI.

Run with: pytest tests/
"""
import pandas as pd

from src.transform import _coerce_and_clean, _derive_metrics


def _sample_df():
    return pd.DataFrame([
        {
            "campaign_id": " CMP-1 ", "campaign_name": "spring sale",
            "channel": "PAID_SEARCH", "report_date": "2026-01-01",
            "country": "US", "region": "CA", "city": "SF",
            "impressions": "1000", "clicks": "50", "spend": "100.0",
            "conversions": "5", "revenue": "500.0",
        },
        {
            # duplicate of the row above on the natural key -> should be dropped
            "campaign_id": "CMP-1", "campaign_name": "Spring Sale",
            "channel": "paid_search", "report_date": "2026-01-01",
            "country": "US", "region": "CA", "city": "SF",
            "impressions": "1000", "clicks": "50", "spend": "100.0",
            "conversions": "5", "revenue": "500.0",
        },
        {
            # missing campaign_id -> should be dropped
            "campaign_id": None, "campaign_name": "No Id",
            "channel": "email", "report_date": "2026-01-02",
            "country": "US", "region": "NY", "city": "NYC",
            "impressions": "100", "clicks": "1", "spend": None,
            "conversions": "0", "revenue": "0",
        },
        {
            # missing spend -> should become 0, row kept
            "campaign_id": "CMP-2", "campaign_name": "Newsletter",
            "channel": "email", "report_date": "2026-01-02",
            "country": "US", "region": "NY", "city": "NYC",
            "impressions": "2000", "clicks": "10", "spend": None,
            "conversions": "1", "revenue": "50.0",
        },
    ])


def test_coerce_and_clean_dedup_and_drops():
    df = _coerce_and_clean(_sample_df())
    # the exact duplicate row should be removed
    assert len(df) == 2
    # missing campaign_id row should be dropped
    assert "No Id" not in df["campaign_name"].values


def test_coerce_and_clean_missing_spend_becomes_zero():
    df = _coerce_and_clean(_sample_df())
    newsletter_row = df[df["campaign_id"] == "CMP-2"].iloc[0]
    assert newsletter_row["spend"] == 0


def test_coerce_and_clean_normalizes_text_fields():
    df = _coerce_and_clean(_sample_df())
    spring_row = df[df["campaign_id"] == "CMP-1"].iloc[0]
    assert spring_row["campaign_id"] == "CMP-1"  # whitespace stripped
    assert spring_row["campaign_name"] == "Spring Sale"  # title-cased
    assert spring_row["channel"] == "paid_search"  # lower-cased


def test_derive_metrics_basic():
    df = pd.DataFrame([
        {"impressions": 1000, "clicks": 50, "spend": 100.0, "conversions": 5, "revenue": 500.0},
    ])
    out = _derive_metrics(df)
    row = out.iloc[0]
    assert row["ctr"] == 50 / 1000
    assert row["cpc"] == 100.0 / 50
    assert row["cpa"] == 100.0 / 5
    assert row["roas"] == 500.0 / 100.0


def test_derive_metrics_handles_zero_denominators():
    df = pd.DataFrame([
        {"impressions": 0, "clicks": 0, "spend": 0.0, "conversions": 0, "revenue": 0.0},
    ])
    out = _derive_metrics(df)
    row = out.iloc[0]
    assert row["ctr"] == 0.0
    assert row["cpc"] == 0.0
    assert row["cpa"] == 0.0
    assert row["roas"] == 0.0
