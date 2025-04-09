# upsert.py
from helpers.connection import get_cache_db_connection
import psycopg2
import time


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
