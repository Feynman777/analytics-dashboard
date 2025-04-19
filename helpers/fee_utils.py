from collections import defaultdict
import pandas as pd
from helpers.connection import get_cache_db_connection
from helpers.constants import CHAIN_ID_MAP

def safe_float(val, default=0.0):
    try:
        return float(val or default)
    except (ValueError, TypeError):
        return default

def fetch_fee_series(start=None):
    from collections import defaultdict
    from helpers.connection import get_cache_db_connection
    from helpers.constants import CHAIN_ID_MAP
    from utils.safe_math import safe_float  # Adjust import if needed

    query = """
        SELECT DATE(created_at) AS date, fee_usd, from_chain
        FROM transactions_cache
        WHERE type = 'SWAP' AND status = 'SUCCESS'
    """
    params = []

    if start:
        query += " AND created_at >= %s"
        params.append(start)

    with get_cache_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()

    def normalize_chain_id(chain_id):
        if chain_id in (101, 1151111081099710):
            return "solana"
        return CHAIN_ID_MAP.get(chain_id, str(chain_id))

    fee_data = defaultdict(lambda: defaultdict(float))
    for created_date, amount_usd, from_chain_id in rows:
        chain_name = normalize_chain_id(from_chain_id)
        fee_data[created_date][chain_name] += safe_float(amount_usd)

    flattened = []
    for date, chains in fee_data.items():
        for chain, value in chains.items():
            flattened.append({
                "date": date,
                "chain": chain,
                "value": value
            })

    df = pd.DataFrame(flattened)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df
