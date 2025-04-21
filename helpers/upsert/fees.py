# helpers/upsert/fees.py
import pandas as pd
from helpers.connection import get_cache_db_connection

def upsert_fee_series(df: pd.DataFrame):
    if df.empty:
        return

    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO timeseries_fees (date, chain, value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (date, chain) DO UPDATE
                    SET value = EXCLUDED.value
                """, (row["date"], row["chain"], row["value"]))
        conn.commit()