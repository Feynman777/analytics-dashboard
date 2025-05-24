from helpers.connection import get_cache_db_connection
import pandas as pd

def fetch_daily_app_downloads():
    with get_cache_db_connection() as conn:
        df = pd.read_sql("SELECT * FROM daily_app_downloads ORDER BY date", conn)
    return df
