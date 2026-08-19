"""
Generates realistic synthetic campaign-performance data from two "sources"
so the pipeline has something to run against without needing a real ad
account:

  1. data/raw/search_social_export.csv  — mimics a CSV export from a
     search/social ads platform.
  2. data/raw/display_email_feed.json   — mimics a JSON API response from
     a display/email platform.

The two sources deliberately have different shapes, some overlapping
messiness (nulls, duplicate rows, inconsistent casing, string numbers)
so extract/transform have real work to do.
"""
import json
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.config import RAW_DATA_DIR
from src.logging_config import setup_logging

logger = setup_logging("adpulse.generate_sample_data")

random.seed(42)
np.random.seed(42)

CAMPAIGNS = [
    ("CMP-1001", "Spring Sale Search", "paid_search"),
    ("CMP-1002", "Retargeting - Cart Abandoners", "paid_social"),
    ("CMP-1003", "Brand Awareness Q3", "display"),
    ("CMP-1004", "Newsletter Promo", "email"),
    ("CMP-1005", "Holiday Blowout", "paid_social"),
    ("CMP-1006", "New Customer Acquisition", "paid_search"),
]

GEOS = [
    ("US", "California", "San Francisco"),
    ("US", "New York", "New York"),
    ("US", "Texas", "Austin"),
    ("IN", "Karnataka", "Bengaluru"),
    ("UK", "England", "London"),
    ("DE", "Bavaria", "Munich"),
]

CHANNEL_TO_PLATFORM = {
    "paid_search": ["Google Ads", "Bing Ads"],
    "paid_social": ["Meta Ads", "LinkedIn Ads"],
    "display": ["Display Network"],
    "email": ["Email Platform"],
}


def _daterange(n_days: int):
    end = datetime.today().date()
    start = end - timedelta(days=n_days - 1)
    return [start + timedelta(days=i) for i in range(n_days)]


def generate_csv_source(n_days: int = 30) -> pd.DataFrame:
    """Search + social campaigns, exported as CSV."""
    rows = []
    for day in _daterange(n_days):
        for cid, cname, channel in CAMPAIGNS:
            if channel not in ("paid_search", "paid_social"):
                continue
            for country, region, city in GEOS:
                impressions = int(np.random.poisson(4000))
                if impressions == 0:
                    continue
                clicks = int(impressions * np.random.uniform(0.01, 0.08))
                spend = round(clicks * np.random.uniform(0.5, 3.5), 2)
                conversions = int(clicks * np.random.uniform(0.01, 0.12))
                revenue = round(conversions * np.random.uniform(20, 150), 2)

                rows.append({
                    "campaign_id": cid,
                    "campaign_name": cname if random.random() > 0.05 else cname.upper(),
                    "channel": channel,
                    "report_date": day.isoformat(),
                    "country": country,
                    "region": region,
                    "city": city,
                    "impressions": impressions,
                    "clicks": clicks,
                    "spend": spend,
                    "conversions": conversions,
                    "revenue": revenue,
                })

    df = pd.DataFrame(rows)

    # inject realistic messiness
    dupe_sample = df.sample(frac=0.01, random_state=1)
    df = pd.concat([df, dupe_sample], ignore_index=True)  # duplicate rows

    null_idx = df.sample(frac=0.01, random_state=2).index
    df.loc[null_idx, "spend"] = None  # missing values

    df["clicks"] = df["clicks"].astype(str)  # numbers-as-strings, like a real export

    return df


def generate_json_source(n_days: int = 30) -> list:
    """Display + email campaigns, shaped like a JSON API response."""
    records = []
    for day in _daterange(n_days):
        for cid, cname, channel in CAMPAIGNS:
            if channel not in ("display", "email"):
                continue
            platform = random.choice(CHANNEL_TO_PLATFORM[channel])
            for country, region, city in GEOS:
                impressions = int(np.random.poisson(6000))
                if impressions == 0:
                    continue
                clicks = int(impressions * np.random.uniform(0.002, 0.02))
                spend = round(clicks * np.random.uniform(0.3, 1.5), 2)
                conversions = int(clicks * np.random.uniform(0.005, 0.05))
                revenue = round(conversions * np.random.uniform(15, 100), 2)

                records.append({
                    "campaign": {"id": cid, "name": cname},
                    "channel": {"name": channel, "platform": platform},
                    "date": day.isoformat(),
                    "location": {"country": country, "region": region, "city": city},
                    "metrics": {
                        "impressions": impressions,
                        "clicks": clicks,
                        "spend_usd": spend if random.random() > 0.02 else None,
                        "conversions": conversions,
                        "revenue_usd": revenue,
                    },
                })
    return records


def main():
    logger.info("Generating synthetic source data...")

    csv_df = generate_csv_source()
    csv_path = RAW_DATA_DIR / "search_social_export.csv"
    csv_df.to_csv(csv_path, index=False)
    logger.info("Wrote %d rows to %s", len(csv_df), csv_path)

    json_records = generate_json_source()
    json_path = RAW_DATA_DIR / "display_email_feed.json"
    with open(json_path, "w") as f:
        json.dump(json_records, f, indent=2)
    logger.info("Wrote %d records to %s", len(json_records), json_path)

    logger.info("Sample data generation complete.")


if __name__ == "__main__":
    main()
