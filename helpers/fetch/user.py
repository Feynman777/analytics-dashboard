import pandas as pd
from psycopg2.extras import RealDictCursor
from helpers.connection import get_cache_db_connection


def fetch_daily_user_stats(start=None, end=None) -> pd.DataFrame:
    query = """
        SELECT * FROM daily_user_stats
        WHERE 1=1
    """
    params = []
    if start:
        query += " AND date >= %s"
        params.append(start)
    if end:
        query += " AND date <= %s"
        params.append(end)

    query += " ORDER BY date ASC"

    with get_cache_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


def fetch_user_volume_by_day(username: str) -> pd.DataFrame:
    query = """
        SELECT created_at, amount_usd
        FROM transactions_cache
        WHERE type = 'SWAP' AND status = 'SUCCESS' AND from_user = %s
    """
    with get_cache_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (username,))
            rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=["created_at", "amount_usd"])
    if df.empty:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["created_at"]).dt.date
    df = df.groupby("date")["amount_usd"].sum().reset_index()
    df.columns = ["date", "daily_volume_usd"]
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


def fetch_user_txn_timeseries(username: str) -> pd.DataFrame:
    query = """
        SELECT created_at, amount_usd
        FROM transactions_cache
        WHERE type = 'SWAP' AND status = 'SUCCESS' AND from_user = %s
    """
    with get_cache_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (username,))
            rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=["datetime", "volume_usd"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime")