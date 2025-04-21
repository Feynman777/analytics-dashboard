# helpers/utils/sync_state.py

from datetime import datetime
from psycopg2.extras import RealDictCursor
from helpers.connection import get_cache_db_connection

def get_last_sync(section: str) -> datetime:
    query = 'SELECT last_sync FROM sync_state WHERE section = %s'
    with get_cache_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (section,))
            row = cur.fetchone()
            if row and row.get("last_sync"):
                return row["last_sync"]
    return None

def update_last_sync(section: str, timestamp: datetime):
    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sync_state (section, last_sync)
                VALUES (%s, %s)
                ON CONFLICT (section) DO UPDATE SET last_sync = EXCLUDED.last_sync
            """, (section, timestamp))
        conn.commit()
