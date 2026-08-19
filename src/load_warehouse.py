"""
Load (clean data -> warehouse) stage.

Reads the cleaned Parquet file, builds/upserts dimension tables
(dim_date, dim_campaign, dim_channel, dim_geography), then loads
fact_campaign_performance using surrogate keys looked up from those
dimensions.

Dimension loads are upserts (insert-or-update on natural key) so re-running
the pipeline doesn't create duplicate dimension rows.
"""
import pandas as pd
from sqlalchemy import text

from src.config import PROCESSED_DATA_DIR
from src.db import get_engine
from src.logging_config import setup_logging

logger = setup_logging("adpulse.load_warehouse")

CHANNEL_TYPE_MAP = {
    "paid_search": "paid_search",
    "paid_social": "paid_social",
    "display": "display",
    "email": "email",
}


def build_dim_date(engine, dates: pd.Series):
    unique_dates = pd.to_datetime(pd.Series(dates.unique()))
    rows = []
    for d in unique_dates:
        rows.append({
            "date_key": int(d.strftime("%Y%m%d")),
            "full_date": d.date(),
            "day_of_week": d.strftime("%A"),
            "day_num": d.day,
            "month": d.month,
            "month_name": d.strftime("%B"),
            "quarter": (d.month - 1) // 3 + 1,
            "year": d.year,
            "is_weekend": d.dayofweek >= 5,
        })
    df = pd.DataFrame(rows)

    with engine.begin() as conn:
        for _, r in df.iterrows():
            conn.execute(text("""
                insert into warehouse.dim_date
                    (date_key, full_date, day_of_week, day_num, month, month_name, quarter, year, is_weekend)
                values
                    (:date_key, :full_date, :day_of_week, :day_num, :month, :month_name, :quarter, :year, :is_weekend)
                on conflict (date_key) do nothing
            """), r.to_dict())
    logger.info("dim_date: upserted %d dates", len(df))


def build_dim_campaign(engine, df: pd.DataFrame):
    campaigns = df[["campaign_id", "campaign_name"]].drop_duplicates()
    with engine.begin() as conn:
        for _, r in campaigns.iterrows():
            conn.execute(text("""
                insert into warehouse.dim_campaign (campaign_id, campaign_name)
                values (:campaign_id, :campaign_name)
                on conflict (campaign_id) do update
                    set campaign_name = excluded.campaign_name,
                        updated_at = now()
            """), r.to_dict())
    logger.info("dim_campaign: upserted %d campaigns", len(campaigns))


def build_dim_channel(engine, df: pd.DataFrame):
    channels = df[["channel"]].drop_duplicates()
    with engine.begin() as conn:
        for _, r in channels.iterrows():
            channel_name = r["channel"]
            conn.execute(text("""
                insert into warehouse.dim_channel (channel_name, channel_type)
                values (:channel_name, :channel_type)
                on conflict (channel_name) do nothing
            """), {
                "channel_name": channel_name,
                "channel_type": CHANNEL_TYPE_MAP.get(channel_name, "other"),
            })
    logger.info("dim_channel: upserted %d channels", len(channels))


def build_dim_geography(engine, df: pd.DataFrame):
    geos = df[["country", "region", "city"]].drop_duplicates()
    with engine.begin() as conn:
        for _, r in geos.iterrows():
            conn.execute(text("""
                insert into warehouse.dim_geography (country, region, city)
                values (:country, :region, :city)
                on conflict (country, region, city) do nothing
            """), r.to_dict())
    logger.info("dim_geography: upserted %d geographies", len(geos))


def load_fact(engine, df: pd.DataFrame) -> int:
    dims = {}
    with engine.connect() as conn:
        dims["campaign"] = pd.read_sql(text("select campaign_key, campaign_id from warehouse.dim_campaign"), conn)
        dims["channel"] = pd.read_sql(text("select channel_key, channel_name from warehouse.dim_channel"), conn)
        dims["geo"] = pd.read_sql(text("select geo_key, country, region, city from warehouse.dim_geography"), conn)

    fact = df.copy()
    fact["date_key"] = pd.to_datetime(fact["report_date"]).dt.strftime("%Y%m%d").astype(int)

    fact = fact.merge(dims["campaign"], on="campaign_id", how="left")
    fact = fact.merge(dims["channel"], left_on="channel", right_on="channel_name", how="left")
    fact = fact.merge(dims["geo"], on=["country", "region", "city"], how="left")

    missing_keys = fact[fact[["campaign_key", "channel_key", "geo_key"]].isna().any(axis=1)]
    if len(missing_keys):
        logger.warning("%d fact rows dropped: could not resolve a dimension key", len(missing_keys))
        fact = fact.dropna(subset=["campaign_key", "channel_key", "geo_key"])

    fact_cols = [
        "date_key", "campaign_key", "channel_key", "geo_key",
        "impressions", "clicks", "spend", "conversions", "revenue",
        "ctr", "cpc", "cpa", "roas",
    ]
    fact_to_load = fact[fact_cols]

    with engine.begin() as conn:
        conn.execute(text("truncate table warehouse.fact_campaign_performance"))
        fact_to_load.to_sql(
            "fact_campaign_performance",
            conn,
            schema="warehouse",
            if_exists="append",
            index=False,
        )

    logger.info("fact_campaign_performance: loaded %d rows", len(fact_to_load))
    return len(fact_to_load)


def load_warehouse() -> int:
    logger.info("Starting load-to-warehouse stage.")
    engine = get_engine()

    path = PROCESSED_DATA_DIR / "campaign_performance_clean.parquet"
    df = pd.read_parquet(path)

    build_dim_date(engine, df["report_date"])
    build_dim_campaign(engine, df)
    build_dim_channel(engine, df)
    build_dim_geography(engine, df)

    rows_loaded = load_fact(engine, df)
    logger.info("Load-to-warehouse stage complete.")
    return rows_loaded


if __name__ == "__main__":
    load_warehouse()
