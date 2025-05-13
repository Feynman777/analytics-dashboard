from datetime import datetime
from psycopg2.extras import RealDictCursor
from helpers.connection import get_cache_db_connection
import json
from pathlib import Path
import pandas as pd


def get_last_sync(section: str) -> datetime:
    """
    Fetch the last sync time for a given section.
    Returns None if no sync has been recorded.
    """
    query = 'SELECT last_sync FROM sync_state WHERE section = %s'
    with get_cache_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (section,))
            row = cur.fetchone()
            if row and row.get("last_sync"):
                return row["last_sync"]

    # Fallback: default to Jan 1st, 2024 if not found
    print(f"⚠️ No previous sync timestamp found for section `{section}`. Defaulting to 2024-01-01.")
    return datetime(2024, 1, 1)


def update_last_sync(section: str, timestamp: datetime):
    """
    Upsert the last sync time for a given section.
    """
    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sync_state (section, last_sync)
                VALUES (%s, %s)
                ON CONFLICT (section)
                DO UPDATE SET last_sync = EXCLUDED.last_sync
            """, (section, timestamp))
        conn.commit()
        print(f"🕒 Updated last_sync for `{section}` to {timestamp.isoformat()}")

def get_last_sync_all() -> dict:
    path = Path("last_sync.json")
    if not path.exists():
        return {}
    with open(path, "r") as f:
        data = json.load(f)
    return {k: pd.to_datetime(v) for k, v in data.items() if v}