# === helpers/fetch/weekly_data.py ===

from datetime import datetime
from typing import List, Dict, Optional
from helpers.connection import get_cache_db_connection
import pandas as pd

def fetch_swap_series(start: Optional[datetime] = None, end: Optional[datetime] = None) -> List[Dict]:
    base_query = """
        SELECT
            DATE(created_at) AS date,
            from_chain AS chain,
            COUNT(*) AS quantity,
            SUM(amount_usd) AS value
        FROM transactions_cache
        WHERE type = 'SWAP' AND status = 'SUCCESS'
    """
    filters = []
    params = []

    if start:
        filters.append("created_at >= %s")
        params.append(start)
    if end:
        filters.append("created_at <= %s")
        params.append(end)

    if filters:
        base_query += " AND " + " AND ".join(filters)

    base_query += """
        GROUP BY DATE(created_at), from_chain
        ORDER BY DATE(created_at) ASC
    """

    with get_cache_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(base_query, tuple(params))
            rows = cursor.fetchall()

    return [
        {
            "date": row[0],
            "chain": row[1],
            "quantity": int(row[2]),
            "value": float(row[3] or 0)
        }
        for row in rows
    ]

def fetch_weekly_stats(metric: str) -> pd.DataFrame:
    with get_cache_db_connection() as conn:
        df = pd.read_sql("""
            SELECT week_start_date AS week, value, quantity
            FROM weekly_stats
            WHERE metric = %s
            ORDER BY week
        """, conn, params=(metric,))
    return df


def fetch_weekly_swap_revenue(conn) -> pd.DataFrame:
    query = """
        SELECT
            DATE_TRUNC('week', date) AS week_start_date,
            SUM(swap_revenue) AS value
        FROM daily_stats
        GROUP BY week_start_date
        ORDER BY week_start_date
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        return pd.DataFrame([
            {
                "week_start_date": row[0],
                "metric": "swap_revenue",
                "value": float(row[1] or 0),
                "quantity": 0,
            }
            for row in rows
        ])

