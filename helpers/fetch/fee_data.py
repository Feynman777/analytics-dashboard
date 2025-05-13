from datetime import datetime
import pandas as pd
from collections import defaultdict
from helpers.connection import get_cache_db_connection
from helpers.utils.constants import CHAIN_ID_MAP
from helpers.utils.safe_math import safe_float


def fetch_fee_series(start: datetime = None, end: datetime = None):
    """
    Loads fee data from transactions_cache for SWAPs with SUCCESS status,
    optionally within a date range, and returns a flattened DataFrame
    grouped by date and chain.
    """
    with get_cache_db_connection() as conn:
        with conn.cursor() as cursor:
            sql = """
                SELECT DATE(created_at) AS date, fee_usd, from_chain
                FROM transactions_cache
                WHERE type = 'SWAP' AND status = 'SUCCESS'
            """
            params = []

            if start:
                sql += " AND created_at >= %s"
                params.append(start)
            if end:
                sql += " AND created_at < %s"
                params.append(end)

            cursor.execute(sql, tuple(params))
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