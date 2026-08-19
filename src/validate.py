"""
Validate stage — the data-quality gate.

Runs after the warehouse load and checks:
  1. Row-count sanity: warehouse fact row count vs. distinct natural keys
     in the cleaned Parquet file (should match, within tolerance).
  2. Null checks on required fact/dimension columns.
  3. Duplicate check on the fact table's natural key.
  4. Outlier/sanity checks on derived metrics (e.g. CTR should never
     exceed 1.0; negative spend/revenue is invalid).

Writes a timestamped JSON report to logs/ and exits non-zero if any
check marked as critical fails, so an orchestrator (e.g. Airflow) can
halt the pipeline rather than serve bad data downstream.
"""
import json
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from src.config import LOGS_DIR, PROCESSED_DATA_DIR, ROW_COUNT_DRIFT_TOLERANCE
from src.db import get_engine
from src.logging_config import setup_logging

logger = setup_logging("adpulse.validate")


def check_row_counts(engine, clean_df: pd.DataFrame) -> dict:
    expected = len(clean_df)
    with engine.connect() as conn:
        actual = conn.execute(text("select count(*) from warehouse.fact_campaign_performance")).scalar()

    drift = abs(expected - actual) / expected if expected else 0
    passed = drift <= ROW_COUNT_DRIFT_TOLERANCE
    return {
        "check": "row_count_match",
        "expected_rows": expected,
        "actual_rows": actual,
        "drift_pct": round(drift * 100, 3),
        "passed": passed,
        "critical": True,
    }


def check_nulls(engine) -> dict:
    required_cols = ["date_key", "campaign_key", "channel_key", "geo_key"]
    results = {}
    with engine.connect() as conn:
        total = conn.execute(text("select count(*) from warehouse.fact_campaign_performance")).scalar()
        for col in required_cols:
            null_count = conn.execute(
                text(f"select count(*) from warehouse.fact_campaign_performance where {col} is null")
            ).scalar()
            results[col] = null_count

    passed = all(v == 0 for v in results.values())
    return {
        "check": "required_column_nulls",
        "total_rows": total,
        "null_counts": results,
        "passed": passed,
        "critical": True,
    }


def check_duplicates(engine) -> dict:
    with engine.connect() as conn:
        dupe_count = conn.execute(text("""
            select count(*) from (
                select date_key, campaign_key, channel_key, geo_key, count(*) c
                from warehouse.fact_campaign_performance
                group by 1,2,3,4
                having count(*) > 1
            ) t
        """)).scalar()

    return {
        "check": "duplicate_natural_key",
        "duplicate_groups": dupe_count,
        "passed": dupe_count == 0,
        "critical": True,
    }


def check_metric_sanity(engine) -> dict:
    with engine.connect() as conn:
        bad_ctr = conn.execute(text(
            "select count(*) from warehouse.fact_campaign_performance where ctr > 1"
        )).scalar()
        negative_spend = conn.execute(text(
            "select count(*) from warehouse.fact_campaign_performance where spend < 0"
        )).scalar()
        negative_revenue = conn.execute(text(
            "select count(*) from warehouse.fact_campaign_performance where revenue < 0"
        )).scalar()

    issues = {
        "ctr_over_100pct": bad_ctr,
        "negative_spend": negative_spend,
        "negative_revenue": negative_revenue,
    }
    passed = all(v == 0 for v in issues.values())
    return {
        "check": "metric_sanity",
        "issues": issues,
        "passed": passed,
        "critical": False,  # flag but don't hard-fail the pipeline on this one
    }


def validate() -> bool:
    logger.info("Starting validate stage.")
    engine = get_engine()

    clean_path = PROCESSED_DATA_DIR / "campaign_performance_clean.parquet"
    clean_df = pd.read_parquet(clean_path)

    checks = [
        check_row_counts(engine, clean_df),
        check_nulls(engine),
        check_duplicates(engine),
        check_metric_sanity(engine),
    ]

    all_passed = all(c["passed"] for c in checks)
    critical_failed = any((not c["passed"]) and c["critical"] for c in checks)

    for c in checks:
        level = logger.info if c["passed"] else (logger.error if c["critical"] else logger.warning)
        level("Check '%s': passed=%s critical=%s", c["check"], c["passed"], c["critical"])

    report = {
        "run_at": datetime.utcnow().isoformat(),
        "all_passed": all_passed,
        "critical_failure": critical_failed,
        "checks": checks,
    }

    report_path = LOGS_DIR / f"validation_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Validation report written to %s", report_path)

    if critical_failed:
        logger.error("Validation FAILED on a critical check. See report for details.")
        return False

    logger.info("Validation complete. all_passed=%s", all_passed)
    return True


if __name__ == "__main__":
    ok = validate()
    sys.exit(0 if ok else 1)
