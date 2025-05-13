from datetime import date, timedelta
import pandas as pd
from helpers.connection import get_main_db_connection, get_cache_db_connection
from helpers.upsert.avg_revenue import upsert_avg_revenue_metrics
from helpers.fetch.fee_data import fetch_fee_series


def fetch_avg_revenue_metrics(days: int = 30, snapshot_date: date = None) -> dict:
    snapshot_date = snapshot_date or date.today()
    start_date = snapshot_date - timedelta(days=days)

    # === Load fee data from transactions_cache ===
    fee_df = fetch_fee_series()
    fee_df = fee_df[fee_df["date"] >= start_date]
    total_fees = fee_df["value"].sum()

    with get_main_db_connection() as conn_main, get_cache_db_connection() as conn_cache:
        cur_main = conn_main.cursor()
        cur_cache = conn_cache.cursor()

        # Check for cached entry in avg_revenue_metrics
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

        # Compute from raw data
        cur_main.execute('SELECT COUNT(*) FROM "User" WHERE "createdAt" >= %s', (start_date,))
        total_users = cur_main.fetchone()[0] or 0

        cur_cache.execute("""
            SELECT COUNT(DISTINCT from_user)
            FROM transactions_cache
            WHERE type = 'SWAP' AND status = 'SUCCESS' AND created_at >= %s
        """, (start_date,))
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

    # Pull fees from transactions_cache via fetch_fee_series
    fee_df = fetch_fee_series()
    mask = (fee_df["date"] >= start_date) & (fee_df["date"] < end_date)
    total_fees = fee_df.loc[mask, "value"].sum()

    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(DISTINCT from_user)
                FROM transactions_cache
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

def fetch_weekly_avg_revenue_per_user():
    query = """
        SELECT
            week AS date,
            avg_rev_per_active_user
        FROM weekly_avg_revenue_metrics
        WHERE week >= CURRENT_DATE - INTERVAL '90 days'
        ORDER BY week ASC
    """
    with get_cache_db_connection() as conn:
        df = pd.read_sql_query(query, conn)
    return df

