
# helpers/upsert/weekly_stats.py
from psycopg2.extras import execute_values
import pandas as pd
from helpers.fetch.financials import fetch_avg_revenue_metrics_for_range
from helpers.fetch.weekly_data import fetch_weekly_swap_revenue
from helpers.connection import get_cache_db_connection

def upsert_weekly_api_metrics(df):
    if df.empty:
        print("⚠️ No weekly API metrics to upsert.")
        return

    with get_cache_db_connection() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, """
                INSERT INTO weekly_stats (week_start_date, metric, value, quantity)
                VALUES %s
                ON CONFLICT (week_start_date, metric) DO UPDATE SET
                    value = EXCLUDED.value,
                    quantity = EXCLUDED.quantity
            """, [
                (
                    row["week_start_date"],
                    row["metric"],
                    float(row["value"]),
                    int(row.get("quantity", 0))
                )
                for _, row in df.iterrows()
            ])
        conn.commit()

    print(f"✅ Upserted {len(df)} rows into weekly_stats (API metrics).")

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
    # === Fetch swap_revenue ===
    df_revenue = fetch_weekly_swap_revenue(conn)

    # === Fetch swap_volume and swap_transactions from daily_stats ===
    query = """
        SELECT DATE_TRUNC('week', date) AS week_start_date,
               SUM(swap_volume) AS value,
               SUM(swap_transactions) AS quantity
        FROM daily_stats
        GROUP BY week_start_date
        ORDER BY week_start_date
    """
    with conn.cursor() as cur:
        cur.execute(query)
        volume_rows = cur.fetchall()

    df_volume = pd.DataFrame([{
        "week_start_date": row[0],
        "metric": "swap_volume",
        "value": float(row[1] or 0),
        "quantity": int(row[2] or 0),
    } for row in volume_rows])

    # === Combine both revenue and volume ===
    combined = pd.concat([df_revenue, df_volume], ignore_index=True)

    if combined.empty:
        print("⚠️ No weekly swap revenue or volume to upsert.")
        return

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO weekly_stats (week_start_date, metric, value, quantity)
            VALUES %s
            ON CONFLICT (week_start_date, metric)
            DO UPDATE SET value = EXCLUDED.value, quantity = EXCLUDED.quantity
        """, [
            (r["week_start_date"], r["metric"], r["value"], r["quantity"])
            for _, r in combined.iterrows()
        ])
        conn.commit()

    print(f"✅ Upserted {len(combined)} rows into weekly_stats (swap revenue + volume).")
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