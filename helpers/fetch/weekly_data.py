import pandas as pd
from helpers.connection import get_cache_db_connection


def fetch_timeseries(metric: str, start_date=None, end_date=None) -> pd.DataFrame:
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT date, value
                    FROM timeseries_cache
                    WHERE metric = %s
                """
                params = [metric]

                if start_date:
                    query += " AND date >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND date <= %s"
                    params.append(end_date)

                query += " ORDER BY date ASC"
                cursor.execute(query, tuple(params))
                df = pd.DataFrame(cursor.fetchall(), columns=["date", "value"])
                df["date"] = pd.to_datetime(df["date"])
                return df
    except Exception as e:
        print(f"[ERROR] fetch_timeseries failed: {e}")
        return pd.DataFrame()


def fetch_timeseries_chain_volume(metric: str = "swap_volume", chains=None, status="success") -> pd.DataFrame:
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

                query += " GROUP BY date ORDER BY date ASC"
                cursor.execute(query, tuple(params))
                df = pd.DataFrame(cursor.fetchall(), columns=["date", "value", "quantity"])
                df["date"] = pd.to_datetime(df["date"])
                return df
    except Exception as e:
        print(f"[ERROR] fetch_timeseries_chain_volume failed: {e}")
        return pd.DataFrame()
