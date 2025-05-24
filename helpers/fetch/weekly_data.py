# === helpers/fetch/weekly_data.py ===

from datetime import datetime, timedelta
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

def fetch_weekly_avg_revenue_metrics(start):
    """
    Aggregates revenue and active user metrics from `avg_revenue_metrics` table
    for the full week starting on `start` (a Monday).
    Returns a single-row DataFrame for upsert.
    """
    if isinstance(start, pd.Timestamp):
        start = start.to_pydatetime().date()

    if hasattr(start, "date"):  # In case it's accidentally a datetime
        start = start.date()

    end = start + timedelta(days=7)

    with get_cache_db_connection() as conn:
        df = pd.read_sql("""
            SELECT date, total_fees, active_users
            FROM avg_revenue_metrics
            WHERE date >= %s AND date < %s
        """, conn, params=(start, end))

    if df.empty:
        return pd.DataFrame()

    total_fees = df["total_fees"].sum()
    total_active_users = df["active_users"].sum()
    avg_rev_per_active_user = total_fees / total_active_users if total_active_users else 0

    return pd.DataFrame([{
        "week": start,
        "total_fees": total_fees,
        "active_users": total_active_users,
        "avg_rev_per_active_user": avg_rev_per_active_user
    }])