"""
Extract stage.

Reads the raw source files from data/raw/ and, if USE_S3 is enabled,
uploads them to an S3 bucket first — simulating a real ingestion pipeline
where raw files land in object storage before being loaded into staging.

This stage does NOT parse/clean anything; it just confirms the source
files exist, are readable, and (optionally) archives them to S3.
"""
import sys
from pathlib import Path

from src.config import RAW_DATA_DIR, USE_S3, S3_BUCKET, AWS_REGION
from src.logging_config import setup_logging

logger = setup_logging("adpulse.extract")

EXPECTED_FILES = ["search_social_export.csv", "display_email_feed.json"]


def upload_to_s3(file_path: Path) -> bool:
    """Upload a single file to S3. Returns True on success, False otherwise.
    Failure here is non-fatal — the pipeline can still run against local
    files — but is logged clearly so it's not silently ignored.
    """
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        logger.warning("boto3 not installed; skipping S3 upload for %s", file_path.name)
        return False

    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"raw/{file_path.name}"
        s3.upload_file(str(file_path), S3_BUCKET, key)
        logger.info("Uploaded %s to s3://%s/%s", file_path.name, S3_BUCKET, key)
        return True
    except (BotoCoreError, ClientError) as exc:
        logger.error("Failed to upload %s to S3: %s", file_path.name, exc)
        return False


def extract() -> list[Path]:
    logger.info("Starting extract stage. USE_S3=%s", USE_S3)
    found_files = []

    for filename in EXPECTED_FILES:
        path = RAW_DATA_DIR / filename
        if not path.exists():
            logger.error("Expected source file missing: %s", path)
            continue
        if path.stat().st_size == 0:
            logger.error("Source file is empty: %s", path)
            continue

        logger.info("Found source file: %s (%d bytes)", path.name, path.stat().st_size)
        found_files.append(path)

        if USE_S3 and S3_BUCKET:
            upload_to_s3(path)

    if len(found_files) < len(EXPECTED_FILES):
        logger.error(
            "Extract incomplete: expected %d source files, found %d. "
            "Run `python src/generate_sample_data.py` or place real files in data/raw/.",
            len(EXPECTED_FILES), len(found_files),
        )
        sys.exit(1)

    logger.info("Extract stage complete: %d files ready for staging load.", len(found_files))
    return found_files


if __name__ == "__main__":
    extract()
