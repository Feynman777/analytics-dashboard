from datetime import datetime, date
import pandas as pd
from collections import defaultdict
from helpers.connection import get_cache_db_connection
from helpers.utils.constants import CHAIN_ID_MAP
from utils.safe_math import safe_float


def fetch_fee_series():
    """
    Loads fee data from transactions_cache for SWAPs with SUCCESS status
    and returns a flattened list by date and chain.
    """
    with get_cache_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DATE(created_at) AS date, fee_usd, from_chain
                FROM transactions_cache
                WHERE type = 'SWAP' AND status = 'SUCCESS'
            """)
            rows = cursor.fetchall()

    fee_data = defaultdict(lambda: defaultdict(float))
    for created_date, fee, from_chain_id in rows:
        chain = CHAIN_ID_MAP.get(from_chain_id, str(from_chain_id))
        fee_data[created_date][chain] += safe_float(fee)

    flattened = []
    for dt, chain_map in fee_data.items():
        for chain, value in chain_map.items():
            flattened.append({
                "date": dt,
                "chain": chain,
                "value": round(value, 6)
            })

    return pd.DataFrame(flattened)


def fetch_cached_fees():
    """
    Loads already-cached fees from timeseries_fees table (date, chain, value).
    """
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT date, chain, value
                    FROM timeseries_fees
                    ORDER BY date ASC
                """)
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=["date", "chain", "value"])
                df["date"] = pd.to_datetime(df["date"])
                return df
    except Exception as e:
        print(f"[ERROR] Failed to fetch cached fees: {e}")
        return pd.DataFrame()
