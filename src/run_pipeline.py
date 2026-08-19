"""
Runs the full AdPulse pipeline end-to-end, in order:
extract -> load_staging -> transform -> load_warehouse -> validate

This mirrors what the Airflow DAG does, but as a single local script for
development or for running inside the Docker container without Airflow.
Stops immediately (non-zero exit) if any stage raises or if validation
finds a critical data-quality failure.
"""
import sys

from src.logging_config import setup_logging

logger = setup_logging("adpulse.run_pipeline")


def main():
    from src import extract, load_staging, transform, load_warehouse, validate

    logger.info("=== AdPulse pipeline run starting ===")

    try:
        logger.info("[1/5] extract")
        extract.extract()

        logger.info("[2/5] load_staging")
        load_staging.load_staging()

        logger.info("[3/5] transform")
        transform.transform()

        logger.info("[4/5] load_warehouse")
        load_warehouse.load_warehouse()

        logger.info("[5/5] validate")
        ok = validate.validate()

        if not ok:
            logger.error("=== Pipeline run completed WITH data-quality failures ===")
            sys.exit(1)

        logger.info("=== AdPulse pipeline run completed successfully ===")

    except Exception:
        logger.exception("Pipeline run failed with an unhandled exception")
        sys.exit(1)


if __name__ == "__main__":
    main()
