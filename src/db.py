"""
Shared Postgres connection helper. All pipeline stages import get_connection()
and get_engine() from here instead of opening their own connections, so
credentials and error handling stay in one place.
"""
import logging

import psycopg2
from sqlalchemy import create_engine

from src.config import DB_CONFIG

logger = logging.getLogger("adpulse.db")


def get_connection():
    """Return a raw psycopg2 connection (used for DDL / row-by-row work)."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.OperationalError as exc:
        logger.error("Could not connect to Postgres at %s:%s: %s",
                      DB_CONFIG["host"], DB_CONFIG["port"], exc)
        raise


def get_engine():
    """Return a SQLAlchemy engine (used by pandas .to_sql()/.read_sql())."""
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(url)
