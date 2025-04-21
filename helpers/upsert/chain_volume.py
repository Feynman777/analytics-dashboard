import pandas as pd
from helpers.connection import get_cache_db_connection


def upsert_chain_volume(df: pd.DataFrame):
    """
    Upserts a DataFrame into the `timeseries_chain_volume` table.
    Expects columns: date, chain, metric, status, value, quantity
    """
    if df.empty:
        print("[INFO] No data to upsert into timeseries_chain_volume.")
        return

    query = """
        INSERT INTO timeseries_chain_volume (date, chain, metric, status, value, quantity)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, chain, metric, status)
        DO UPDATE SET value = EXCLUDED.value, quantity = EXCLUDED.quantity
    """

    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    cur.execute(query, (
                        row["date"],
                        row["chain"],
                        row["metric"],
                        row["status"],
                        row["value"],
                        row["quantity"]
                    ))
                conn.commit()
        print(f"✅ Upserted {len(df)} rows into timeseries_chain_volume.")

    except Exception as e:
        print(f"[ERROR] Failed to upsert chain volume: {e}")
        raise