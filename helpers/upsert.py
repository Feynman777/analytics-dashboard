# upsert.py
from helpers.connection import get_cache_db_connection
import psycopg2
import time
import pandas as pd



def upsert_chain_timeseries(df):
    """
    Upserts a list of dictionaries into the timeseries_chain_volume table.

    Each dictionary should contain the following keys:
    - date (str or datetime.date)
    - chain (str)
    - metric (str)
    - status (str)
    - value (float)
    - quantity (int)
    """
    if df.empty:
        return

    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO timeseries_chain_volume (date, chain, metric, status, value, quantity)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date, chain, metric, status)
                        DO UPDATE SET value = EXCLUDED.value, quantity = EXCLUDED.quantity
                    """, (
                        row['date'],
                        row['chain'],
                        row['metric'],
                        row['status'],
                        row['value'],
                        row['quantity']
                    ))
                conn.commit()
    except Exception as e:
        print(f"Error in upsert_chain_timeseries: {e}")
        raise


def upsert_timeseries(metric, df):
    """
    Upserts a pandas DataFrame into the timeseries_cache table.

    Parameters:
    - metric (str): the name of the metric
    - df (pd.DataFrame): must contain columns ['date', 'value']
    """
    if df.empty:
        return

    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                batch_size = 50
                for start in range(0, len(df), batch_size):
                    batch = df.iloc[start:start + batch_size]
                    for attempt in range(3):
                        try:
                            for _, row in batch.iterrows():
                                date = row['date'].strftime('%Y-%m-%d')
                                value = float(row['value'])
                                cursor.execute("""
                                    INSERT INTO timeseries_cache (metric, date, value)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (metric, date)
                                    DO UPDATE SET value = EXCLUDED.value
                                """, (metric, date, value))
                            conn.commit()
                            break
                        except psycopg2.errors.DeadlockDetected:
                            conn.rollback()
                            time.sleep(1)
                            if attempt == 2:
                                raise
                        except Exception as e:
                            conn.rollback()
                            raise
    except Exception as e:
        print(f"Error in upsert_timeseries: {e}")


def upsert_fee_series(df):
    """Insert or update fee data into timeseries_fees table."""
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO timeseries_fees (date, chain, value)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (date, chain)
                        DO UPDATE SET value = EXCLUDED.value
                    """, (row["date"], row["chain"], row["value"]))
                conn.commit()
    except Exception as e:
        print(f"[ERROR] Failed to upsert fees: {e}")
        raise

def upsert_avg_revenue_metrics(df):
    """Insert or update average revenue metrics into avg_revenue_metrics table."""
    from helpers.connection import get_cache_db_connection

    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO avg_revenue_metrics (
                            date, total_fees, total_users, active_users,
                            avg_rev_per_user, avg_rev_per_active_user
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date)
                        DO UPDATE SET
                            total_fees = EXCLUDED.total_fees,
                            total_users = EXCLUDED.total_users,
                            active_users = EXCLUDED.active_users,
                            avg_rev_per_user = EXCLUDED.avg_rev_per_user,
                            avg_rev_per_active_user = EXCLUDED.avg_rev_per_active_user
                    """, (
                        row["date"], row["total_fees"], row["total_users"],
                        row["active_users"], row["avg_rev_per_user"], row["avg_rev_per_active_user"]
                    ))
            conn.commit()
    except Exception as e:
        print(f"[ERROR] Failed to upsert avg revenue metrics: {e}")
        raise

def upsert_weekly_avg_revenue_metrics(df: pd.DataFrame):
    from helpers.connection import get_cache_db_connection
    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO weekly_avg_revenue_metrics (week, total_fees, active_users, avg_rev_per_active_user)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (week) DO UPDATE
                    SET total_fees = EXCLUDED.total_fees,
                        active_users = EXCLUDED.active_users,
                        avg_rev_per_active_user = EXCLUDED.avg_rev_per_active_user
                """, (row["week"], row["total_fees"], row["active_users"], row["avg_rev_per_active_user"]))
        conn.commit()
