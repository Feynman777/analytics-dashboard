
# helpers/upsert/weekly_stats.py
from psycopg2.extras import execute_values
import pandas as pd
from helpers.fetch.financials import fetch_avg_revenue_metrics_for_range
from helpers.fetch.weekly_data import fetch_weekly_swap_revenue



def upsert_weekly_stats(df: pd.DataFrame, conn):
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO weekly_stats (week_start_date, metric, value, quantity)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (week_start_date, metric)
                DO UPDATE SET value = EXCLUDED.value, quantity = EXCLUDED.quantity
            """, (row["week_start_date"], row["metric"], row.get("value", 0), row.get("quantity", 0)))
        conn.commit()

def upsert_weekly_swap_revenue(conn):
    df = fetch_weekly_swap_revenue(conn)
    if df.empty:
        return

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO weekly_stats (week_start_date, metric, value, quantity)
            VALUES %s
            ON CONFLICT (week_start_date, metric)
            DO UPDATE SET value = EXCLUDED.value, quantity = EXCLUDED.quantity
        """, [
            (r["week_start_date"], r["metric"], r["value"], r["quantity"])
            for _, r in df.iterrows()
        ])
        conn.commit()

def upsert_weekly_avg_revenue(conn):
    weekly_df = fetch_avg_revenue_metrics_for_range(weekly=True)  # adjust this to compute per week
    if weekly_df.empty:
        return

    with conn.cursor() as cur:
        for _, row in weekly_df.iterrows():
            cur.execute("""
                INSERT INTO weekly_stats (week_start_date, metric, value, quantity)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (week_start_date, metric)
                DO UPDATE SET value = EXCLUDED.value, quantity = EXCLUDED.quantity
            """, (
                row["week_start_date"],
                "avg_rev_per_active_user",
                row["avg_rev"],
                row["active_users"]
            ))
        conn.commit()