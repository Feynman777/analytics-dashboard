from google.cloud import bigquery
from datetime import date, datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from helpers.connection import get_cache_db_connection

# Set default start date to 3 days ago
DEFAULT_START_DATE = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%d")

def fetch_daily_installs_from_bigquery(start_date=DEFAULT_START_DATE) -> pd.DataFrame:
    client = bigquery.Client()

    query = f"""
        SELECT
            event_date,
            COUNT(DISTINCT user_pseudo_id) AS installs,
            ARRAY_AGG(DISTINCT device.operating_system IGNORE NULLS) AS os_types,
            ARRAY_AGG(DISTINCT geo.country IGNORE NULLS) AS countries
        FROM
            `mobile-app-df5c3.analytics_489350194.events_*`
        WHERE
            _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE("{start_date}"))
                             AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
            AND event_name = 'first_open'
        GROUP BY event_date
        ORDER BY event_date
    """

    result = client.query(query).to_dataframe()
    result["source"] = "firebase"
    result["event_date"] = pd.to_datetime(result["event_date"]).dt.date
    return result.rename(columns={"event_date": "date"})

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
