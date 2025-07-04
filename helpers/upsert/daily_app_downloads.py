from google.cloud import bigquery
from datetime import date, datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from helpers.connection import get_cache_db_connection

# Set default start date to 3 days ago
DEFAULT_START_DATE = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%d")

def fetch_daily_installs_from_bigquery(days: int = 3) -> pd.DataFrame:
    client = bigquery.Client()
    start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

    query = f"""
        SELECT
            DATE(TIMESTAMP_MICROS(event_timestamp)) AS date,
            COUNT(DISTINCT user_pseudo_id) AS installs,
            ARRAY_AGG(DISTINCT device.operating_system IGNORE NULLS) AS os_types,
            ARRAY_AGG(DISTINCT geo.country IGNORE NULLS) AS countries
        FROM
            `mobile-app-df5c3.analytics_489350194.events_*`
        WHERE
            _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE("{start_date}"))
                             AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
            AND event_name = 'first_open'
            AND DATE(TIMESTAMP_MICROS(event_timestamp)) >= DATE("{start_date}")
        GROUP BY date
        ORDER BY date
    """

    result = client.query(query).to_dataframe()
    result["source"] = "firebase"
    result["date"] = pd.to_datetime(result["date"]).dt.date
    return result

def upsert_daily_app_downloads(df, conn=None):
    if df.empty:
        print("⚠️ No installs to upsert.")
        return

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
    print("📝 Attempting to upsert the following rows:")
    for row in records:
        print(row)

    insert_query = """
        INSERT INTO daily_app_downloads (date, installs, os_types, countries, source)
        VALUES %s
        ON CONFLICT ("date") DO UPDATE
        SET installs = EXCLUDED.installs,
            os_types = EXCLUDED.os_types,
            countries = EXCLUDED.countries,
            source = EXCLUDED.source;
    """

    use_conn = conn or get_cache_db_connection()

    if conn:
        # Assume external caller manages commit + close
        with use_conn.cursor() as cur:
            execute_values(cur, insert_query, records)
    else:
        with use_conn:
            with use_conn.cursor() as cur:
                execute_values(cur, insert_query, records)
            use_conn.commit()

    print(f"✅ Upserted {len(records)} rows into daily_app_downloads.")