import pandas as pd
from psycopg2 import errors
import time
from helpers.connection import get_cache_db_connection

def upsert_timeseries(metric: str, df: pd.DataFrame):
    """
    Upserts a pandas DataFrame into the timeseries_cache table.

    Parameters:
    - metric (str): the name of the metric
    - df (pd.DataFrame): must contain columns ['date', 'value']
    """
    if df.empty:
        print(f"⚠️ Skipping upsert_timeseries: No data for metric '{metric}'")
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
                        except errors.DeadlockDetected:
                            conn.rollback()
                            time.sleep(1)
                            if attempt == 2:
                                raise
                        except Exception as e:
                            conn.rollback()
                            print(f"❌ Failed batch upsert for metric '{metric}': {e}")
                            raise
    except Exception as e:
        print(f"❌ Error in upsert_timeseries for '{metric}': {e}")