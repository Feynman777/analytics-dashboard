#helpers/cache_db.py
import psycopg2
import pandas as pd
import time
from streamlit import secrets

# === DB CONNECTION ===
DB_HOST_cache = secrets["cache_db"]["DB_HOST"]
DB_PORT_cache = secrets["cache_db"]["DB_PORT"]
DB_NAME_cache = secrets["cache_db"]["DB_NAME"]
DB_USER_cache = secrets["cache_db"]["DB_USER"]
DB_PASS_cache = secrets["cache_db"]["DB_PASS"]

def get_cache_db_connection():
    return psycopg2.connect(
        host=DB_HOST_cache,
        port=int(DB_PORT_cache),
        dbname=DB_NAME_cache,
        user=DB_USER_cache,
        password=DB_PASS_cache
    )

# === UPSERT TO timeseries_cache ===
def upsert_timeseries(metric, df):
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
                                print(f"[DEBUG] Upserting {metric}: date={date}, value={value}")
                                cursor.execute("""
                                    INSERT INTO timeseries_cache (metric, date, value)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (metric, date)
                                    DO UPDATE SET value = EXCLUDED.value
                                """, (metric, date, value))
                            conn.commit()
                            print(f"[DEBUG] Committed {len(batch)} rows for {metric} in batch {start//batch_size + 1}")
                            break
                        except psycopg2.errors.DeadlockDetected as e:
                            print(f"[DEBUG] Deadlock detected, retrying {attempt + 1}/3: {e}")
                            conn.rollback()
                            time.sleep(1)
                            if attempt == 2:
                                raise
                        except Exception as e:
                            print(f"[DEBUG] Unexpected error in batch {start//batch_size + 1}: {e}")
                            conn.rollback()
                            raise
    except Exception as e:
        print(f"❌ Error during upsert: {e}")

# === FETCH FROM timeseries_cache ===
def fetch_timeseries(metric, start_date=None, end_date=None):
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT date, value FROM timeseries_cache WHERE metric = %s"
                params = [metric]
                if start_date:
                    query += " AND date >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND date <= %s"
                    params.append(end_date)
                cursor.execute(query, tuple(params))
                data = cursor.fetchall()
                print(f"[DEBUG] Fetched {len(data)} rows for {metric}")
                df = pd.DataFrame(data, columns=["date", "value"])
                df["date"] = pd.to_datetime(df["date"])
                return df
    except Exception as e:
        print(f"❌ Error during fetch: {e}")
        return pd.DataFrame()

# === FETCH FROM timeseries_chain_volume ===
def fetch_timeseries_chain_volume(metric="swap_volume", chains=None, status="success"):
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT date, SUM(value) as value, SUM(quantity) as quantity
                    FROM timeseries_chain_volume
                    WHERE metric = %s AND status = %s
                """
                params = [metric, status]

                if chains:
                    placeholders = ','.join(['%s'] * len(chains))
                    query += f" AND chain IN ({placeholders})"
                    params.extend(chains)
                    print(f"[DEBUG] Filtering for chains: {chains}")

                query += " GROUP BY date ORDER BY date ASC"
                cursor.execute(query, tuple(params))
                rows = cursor.fetchall()

                df = pd.DataFrame(rows, columns=["date", "value", "quantity"])
                df["date"] = pd.to_datetime(df["date"])
                return df
    except Exception as e:
        print(f"❌ Error in fetch_timeseries_chain_volume: {e}")
        return pd.DataFrame()
