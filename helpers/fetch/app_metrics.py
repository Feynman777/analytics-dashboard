#helpers/fetch/app_metrics.py
from datetime import date
from helpers.connection import get_cache_db_connection
import pandas as pd

def fetch_app_metrics_data(start: date, end: date) -> pd.DataFrame:
    query = """
        SELECT event_date AS date,
               event_name,
               operating_system,
               country,
               COUNT(*) AS count
        FROM daily_app_metrics
        WHERE event_date BETWEEN %s AND %s
        GROUP BY event_date, event_name, operating_system, country
        ORDER BY event_date;
    """

    with get_cache_db_connection() as conn:
        df = pd.read_sql(query, conn, params=(start, end))

    return df

def reshape_event_counts(df: pd.DataFrame, event_type: str) -> pd.DataFrame:
    df_event = df[df.event_name == event_type]
    pivoted = df_event.groupby(['date', 'operating_system'])['count'].sum().reset_index()
    pivoted = pivoted.pivot(index='date', columns='operating_system', values='count').fillna(0).reset_index()
    pivoted['total'] = pivoted.drop(columns='date').sum(axis=1)
    return pivoted

def fetch_country_installs(start: date, end: date) -> pd.DataFrame:
    query = """
        SELECT
            COALESCE(NULLIF(TRIM(country::text), ''), 'Unknown') AS country,
            COUNT(*) AS installs
        FROM daily_app_metrics
        WHERE event_name = 'first_open'
          AND event_date BETWEEN %s AND %s
        GROUP BY COALESCE(NULLIF(TRIM(country::text), ''), 'Unknown')
        ORDER BY installs DESC;
    """

    with get_cache_db_connection() as conn:
        return pd.read_sql(query, conn, params=(start, end))

def fetch_total_installs(start: date, end: date) -> int:
    query = """
        SELECT COUNT(*) AS total
        FROM daily_app_metrics
        WHERE event_name = 'first_open' AND event_date BETWEEN %s AND %s;
    """

    with get_cache_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, (start, end))
        result = cur.fetchone()
        return result[0] if result else 0
