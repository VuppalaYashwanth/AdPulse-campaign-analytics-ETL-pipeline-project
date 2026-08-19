"""
Central configuration for AdPulse, driven entirely by environment
variables (see .env.example). Keeping config in one place means no
script hardcodes credentials or paths.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# --- Paths -------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR = BASE_DIR / "logs"

for _dir in (RAW_DATA_DIR, PROCESSED_DATA_DIR, LOGS_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

# --- Postgres ------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "adpulse"),
    "user": os.getenv("POSTGRES_USER", "adpulse"),
    "password": os.getenv("POSTGRES_PASSWORD", "adpulse"),
}

# --- Optional S3 landing zone -------------------------------------------
USE_S3 = os.getenv("USE_S3", "false").lower() == "true"
S3_BUCKET = os.getenv("S3_BUCKET", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# --- Data quality thresholds ---------------------------------------------
MAX_ALLOWED_NULL_FRACTION = float(os.getenv("MAX_ALLOWED_NULL_FRACTION", "0.02"))
ROW_COUNT_DRIFT_TOLERANCE = float(os.getenv("ROW_COUNT_DRIFT_TOLERANCE", "0.0"))
