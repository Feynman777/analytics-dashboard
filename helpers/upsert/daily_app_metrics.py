from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from helpers.connection import get_cache_db_connection

DEFAULT_DAYS = 2

def fetch_app_event_data_from_bigquery(days: int = DEFAULT_DAYS) -> pd.DataFrame:
    client = bigquery.Client()
    start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y%m%d")

    query = f"""
        SELECT
            event_date,
            event_name,
            device.operating_system,
            device.mobile_brand_name,
            geo.country,
            user_pseudo_id
        FROM
            `mobile-app-df5c3.analytics_489350194.events_*`
        WHERE
            _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE("{start_date}"))
                             AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
            AND event_name IN (
                'app_remove', 'firebase_campaign', 'first_open',
                'location', 'screen_view', 'session_start',
                'user_engagement', 'user_signup'
            )
    """

    df = client.query(query).to_dataframe()
    df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d").dt.date
    return df


def upsert_daily_app_metrics(df: pd.DataFrame, conn=None):
    if df.empty:
        print("⚠️ No app event metrics to upsert.")
        return

    now = datetime.utcnow()

    records = [
        (
            row["event_date"],
            row["event_name"],
            row.get("operating_system"),
            row.get("mobile_brand_name"),
            row.get("country"),
            row["user_pseudo_id"]
        )
        for _, row in df.iterrows()
    ]

    insert_query = """
        INSERT INTO daily_app_metrics (
            event_date, event_name, operating_system,
            mobile_brand_name, country, user_pseudo_id
        )
        VALUES %s
        ON CONFLICT (event_date, user_pseudo_id, event_name)
        DO NOTHING;
    """

    use_conn = conn or get_cache_db_connection()

    if conn:
        with use_conn.cursor() as cur:
            execute_values(cur, insert_query, records)
    else:
        with use_conn:
            with use_conn.cursor() as cur:
                execute_values(cur, insert_query, records)
            use_conn.commit()

    print(f"✅ Upserted {len(records)} rows into daily_app_metrics.")


if __name__ == "__main__":
    df = fetch_app_event_data_from_bigquery()
    upsert_daily_app_metrics(df)
