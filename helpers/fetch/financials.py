from datetime import date, timedelta
import pandas as pd
from helpers.upsert.revenue import upsert_avg_revenue_metrics
from helpers.connection import get_main_db_connection, get_cache_db_connection

def fetch_avg_revenue_metrics(days: int = 30, snapshot_date: date = None) -> dict:
    snapshot_date = snapshot_date or date.today()
    start_date = snapshot_date - timedelta(days=days)

    with get_main_db_connection() as conn_main, get_cache_db_connection() as conn_cache:
        cur_main = conn_main.cursor()
        cur_cache = conn_cache.cursor()

        # Load cached metrics if available
        cur_cache.execute("SELECT * FROM avg_revenue_metrics WHERE date = %s", (snapshot_date,))
        row = cur_cache.fetchone()
        if row:
            return {
                "date": row[0],
                "total_fees": float(row[1] or 0),
                "total_users": row[2],
                "active_users": row[3],
                "avg_rev_per_user": float(row[4] or 0),
                "avg_rev_per_active_user": float(row[5] or 0)
            }

        # Compute metrics
        cur_cache.execute("SELECT SUM(value) FROM timeseries_fees WHERE date >= %s", (start_date,))
        total_fees = cur_cache.fetchone()[0] or 0

        cur_main.execute('SELECT COUNT(*) FROM "User" WHERE "createdAt" >= %s', (start_date,))
        total_users = cur_main.fetchone()[0] or 0

        cur_cache.execute('''
            SELECT COUNT(DISTINCT from_user)
            FROM transactions_cache
            WHERE type = 'SWAP' AND status = 'SUCCESS' AND created_at >= %s
        ''', (start_date,))
        active_users = cur_cache.fetchone()[0] or 0

        result = {
            "date": snapshot_date,
            "total_fees": total_fees,
            "total_users": total_users,
            "active_users": active_users,
            "avg_rev_per_user": total_fees / total_users if total_users else 0,
            "avg_rev_per_active_user": total_fees / active_users if active_users else 0,
        }

        upsert_avg_revenue_metrics(pd.DataFrame([result]))
        return result

def fetch_avg_revenue_metrics_for_range(start_date: date, days: int = 7) -> pd.DataFrame:
    end_date = start_date + timedelta(days=days)
    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT SUM(value) FROM timeseries_fees
                WHERE date >= %s AND date < %s
            """, (start_date, end_date))
            total_fees = cur.fetchone()[0] or 0

            cur.execute("""
                SELECT COUNT(DISTINCT from_user) FROM transactions_cache
                WHERE type = 'SWAP' AND status = 'SUCCESS'
                  AND created_at >= %s AND created_at < %s
            """, (start_date, end_date))
            active_users = cur.fetchone()[0] or 0

            return pd.DataFrame([{
                "week": start_date,
                "total_fees": total_fees,
                "active_users": active_users,
                "avg_rev_per_active_user": total_fees / active_users if active_users else 0
            }])

def fetch_weekly_avg_revenue_metrics() -> pd.DataFrame:
    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT week, total_fees, active_users, avg_rev_per_active_user
                FROM weekly_avg_revenue_metrics
                ORDER BY week
            """)
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=[
                "week", "total_fees", "active_users", "avg_rev_per_active_user"
            ])
            df["week"] = pd.to_datetime(df["week"])
            return df
