from google.cloud import bigquery
from datetime import date, datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from helpers.connection import get_cache_db_connection

def fetch_daily_installs_from_bigquery(start: datetime = None) -> pd.DataFrame:
    client = bigquery.Client()
    if not start:
        start = datetime.utcnow() - timedelta(days=1)

    query = f"""
        SELECT
            DATE(TIMESTAMP_MICROS(event_timestamp)) AS date,
            COUNT(*) AS installs,
            ARRAY_AGG(DISTINCT platform) AS os_types,
            ARRAY_AGG(DISTINCT geo.country) AS countries,
            ARRAY_AGG(DISTINCT traffic_source.source) AS sources
        FROM `mobile-app-df5c3.analytics_489350194.events_*`
        WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
                                AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
          AND event_name = 'first_open'
          AND TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP('{start.strftime('%Y-%m-%d %H:%M:%S')}')
        GROUP BY date
        ORDER BY date DESC
    """

    return client.query(query).to_dataframe()

def upsert_daily_app_downloads(df):
    records = [
    (
        row["date"],
        int(row["installs"]),
        list(row["os_types"]),
        list(row["countries"]),
        row["source"]
    )
    for _, row in df.iterrows()
]
    insert_query = """
        INSERT INTO daily_app_downloads (date, installs, os_types, countries, source)
        VALUES %s
        ON CONFLICT (date) DO UPDATE
        SET installs = EXCLUDED.installs,
            os_types = EXCLUDED.os_types,
            countries = EXCLUDED.countries,
            source = EXCLUDED.source;
    """
    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, records)
        conn.commit()

def sync_daily_app_downloads(start="2025-05-20"):
    df = fetch_daily_installs_from_bigquery(start)
    if not df.empty:
        upsert_daily_app_downloads(df)
        print(f"✅ Synced {len(df)} rows into daily_app_downloads")
    else:
        print("ℹ️ No new data to sync.")
