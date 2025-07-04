import pandas as pd
from helpers.connection import get_cache_db_connection
from psycopg2.extras import execute_values

def upsert_avg_revenue_metrics(df: pd.DataFrame):
    """
    Upserts daily average revenue metrics into avg_revenue_metrics table.
    Expected columns: date, total_fees, total_users, active_users, avg_rev_per_user, avg_rev_per_active_user
    """
    if df.empty:
        print("⚠️ No avg revenue metrics to upsert.")
        return

    with get_cache_db_connection() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, """
                INSERT INTO avg_revenue_metrics (
                    date, total_fees, total_users, active_users,
                    avg_rev_per_user, avg_rev_per_active_user
                ) VALUES %s
                ON CONFLICT (date) DO UPDATE SET
                    total_fees = EXCLUDED.total_fees,
                    total_users = EXCLUDED.total_users,
                    active_users = EXCLUDED.active_users,
                    avg_rev_per_user = EXCLUDED.avg_rev_per_user,
                    avg_rev_per_active_user = EXCLUDED.avg_rev_per_active_user
            """, [
                (
                    row["date"],
                    row["total_fees"],
                    row["total_users"],
                    row["active_users"],
                    row["avg_rev_per_user"],
                    row["avg_rev_per_active_user"]
                ) for _, row in df.iterrows()
            ])
        conn.commit()
        print(f"✅ Upserted {len(df)} avg revenue metric rows.")

import pandas as pd
from psycopg2.extras import execute_values
from helpers.connection import get_cache_db_connection

def upsert_weekly_avg_revenue_metrics(df: pd.DataFrame):
    """
    Upserts weekly average revenue metrics into weekly_avg_revenue_metrics table.
    Expected columns: week, total_fees, active_users, avg_rev_per_active_user
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        print("⚠️ No weekly avg revenue metrics to upsert or invalid DataFrame.")
        return

    required_cols = {"week", "total_fees", "active_users", "avg_rev_per_active_user"}
    if not required_cols.issubset(df.columns):
        print(f"❌ DataFrame is missing required columns: {required_cols - set(df.columns)}")
        return

    with get_cache_db_connection() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, """
                INSERT INTO weekly_avg_revenue_metrics (
                    week, total_fees, active_users, avg_rev_per_active_user
                ) VALUES %s
                ON CONFLICT (week) DO UPDATE SET
                    total_fees = EXCLUDED.total_fees,
                    active_users = EXCLUDED.active_users,
                    avg_rev_per_active_user = EXCLUDED.avg_rev_per_active_user
            """, [
                (
                    row["week"],
                    float(row["total_fees"]),
                    int(row["active_users"]),
                    float(row["avg_rev_per_active_user"])
                ) for _, row in df.iterrows()
            ])
        conn.commit()

    print(f"✅ Upserted {len(df)} rows into weekly_avg_revenue_metrics.")
