# helpers/fetch/daily_stats.py

import pandas as pd
from psycopg2.extras import RealDictCursor
from helpers.connection import get_cache_db_connection

def fetch_daily_stats(start=None, end=None):
    with get_cache_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = """
                SELECT
                    date,
                    SUM(swap_transactions) AS swap_transactions,
                    SUM(send_transactions) AS send_transactions,
                    SUM(cash_transactions) AS cash_transactions,
                    SUM(dapp_connections) AS dapp_connections,
                    MAX(referrals) AS referrals,
                    MAX(agents_deployed) AS agents_deployed,
                    SUM(swap_volume)::DOUBLE PRECISION AS swap_volume,
                    SUM(swap_revenue)::DOUBLE PRECISION AS swap_revenue,
                    SUM(send_volume)::DOUBLE PRECISION AS send_volume,
                    SUM(cash_volume)::DOUBLE PRECISION AS cash_volume,
                    SUM(cash_revenue)::DOUBLE PRECISION AS cash_revenue,
                    SUM(revenue)::DOUBLE PRECISION AS revenue,
                    MAX(active_users) AS active_users
                FROM daily_stats
                WHERE 1=1
            """
            params = []
            if start:
                query += " AND date >= %s"
                params.append(start)
            if end:
                query += " AND date <= %s"
                params.append(end)

            query += " GROUP BY date ORDER BY date ASC"
            cursor.execute(query, tuple(params))
            df = pd.DataFrame(cursor.fetchall())
            df["date"] = pd.to_datetime(df["date"])
            return df

def fetch_daily_user_stats(start=None, end=None):
    with get_cache_db_connection() as conn:
        with conn.cursor() as cursor:
            query = """
                SELECT *
                FROM daily_user_stats
                WHERE 1=1
            """
            params = []
            if start:
                query += " AND date >= %s"
                params.append(start)
            if end:
                query += " AND date <= %s"
                params.append(end)

            query += " ORDER BY date ASC"
            cursor.execute(query, tuple(params))
            df = pd.DataFrame(cursor.fetchall(), columns=[
                "date", "active_swap", "active_send", "active_cash",
                "total_active", "new", "new_active"
            ])
            df["date"] = pd.to_datetime(df["date"])
            return df