"""
Load (raw -> staging) stage.

Loads the raw CSV and JSON source files into staging.* tables in Postgres
exactly as they are — text columns for the CSV source, a jsonb blob for
the JSON source. No cleaning or type coercion happens here; that's
transform.py's job. Keeping raw loads untouched means we always have an
auditable copy of exactly what the source sent us.
"""
import json

import pandas as pd
from sqlalchemy import text

from src.config import RAW_DATA_DIR
from src.db import get_engine
from src.logging_config import setup_logging

logger = setup_logging("adpulse.load_staging")


def load_csv_source(engine) -> int:
    path = RAW_DATA_DIR / "search_social_export.csv"
    df = pd.read_csv(path, dtype=str)  # load everything as text on purpose
    df["source_file"] = path.name

    df.to_sql(
        "campaign_performance_csv",
        engine,
        schema="staging",
        if_exists="append",
        index=False,
    )
    logger.info("Loaded %d rows from %s into staging.campaign_performance_csv", len(df), path.name)
    return len(df)


def load_json_source(engine) -> int:
    path = RAW_DATA_DIR / "display_email_feed.json"
    with open(path) as f:
        records = json.load(f)

    rows = [{"raw_payload": json.dumps(r), "source_file": path.name} for r in records]
    df = pd.DataFrame(rows)

    df.to_sql(
        "campaign_performance_json",
        engine,
        schema="staging",
        if_exists="append",
        index=False,
        dtype={"raw_payload": None},
    )
    logger.info("Loaded %d records from %s into staging.campaign_performance_json", len(df), path.name)
    return len(df)


def log_load(engine, source_name: str, file_name: str, rows_loaded: int, status: str, notes: str = ""):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                insert into staging.load_log (source_name, file_name, rows_loaded, status, notes)
                values (:source_name, :file_name, :rows_loaded, :status, :notes)
                """
            ),
            {
                "source_name": source_name,
                "file_name": file_name,
                "rows_loaded": rows_loaded,
                "status": status,
                "notes": notes,
            },
        )


def load_staging():
    logger.info("Starting load-to-staging stage.")
    engine = get_engine()

    try:
        csv_rows = load_csv_source(engine)
        log_load(engine, "csv_export", "search_social_export.csv", csv_rows, "success")
    except Exception as exc:
        logger.error("Failed loading CSV source into staging: %s", exc)
        log_load(engine, "csv_export", "search_social_export.csv", 0, "failed", str(exc))
        raise

    try:
        json_rows = load_json_source(engine)
        log_load(engine, "json_feed", "display_email_feed.json", json_rows, "success")
    except Exception as exc:
        logger.error("Failed loading JSON source into staging: %s", exc)
        log_load(engine, "json_feed", "display_email_feed.json", 0, "failed", str(exc))
        raise

    logger.info("Load-to-staging stage complete. csv_rows=%d json_rows=%d", csv_rows, json_rows)
    return csv_rows, json_rows


if __name__ == "__main__":
    load_staging()
