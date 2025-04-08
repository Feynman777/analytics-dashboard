#helpers/chain_upsert.py
import psycopg2
import pandas as pd
import time
from psycopg2.extras import execute_values
import logging
from streamlit import secrets
import datetime

# Setup logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# === DB CONNECTION ===
DB_HOST_cache = secrets["cache_db"]["DB_HOST"]
DB_PORT_cache = secrets["cache_db"]["DB_PORT"]
DB_NAME_cache = secrets["cache_db"]["DB_NAME"]
DB_USER_cache = secrets["cache_db"]["DB_USER"]
DB_PASS_cache = secrets["cache_db"]["DB_PASS"]


def upsert_chain_timeseries(data):
    try:
        with psycopg2.connect(
            host=DB_HOST_cache, port=int(DB_PORT_cache), dbname=DB_NAME_cache, user=DB_USER_cache, password=DB_PASS_cache
        ) as conn:
            with conn.cursor() as cursor:
                for row in data:
                    cursor.execute("""
                        INSERT INTO timeseries_chain_volume (date, chain, metric, status, value, quantity)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date, chain, metric, status)
                        DO UPDATE SET value = EXCLUDED.value, quantity = EXCLUDED.quantity
                    """, (
                        row['date'],
                        row['chain'],
                        row['metric'],
                        row['status'],
                        row['value'],
                        row['quantity']
                    ))
                conn.commit()
    except Exception as e:
        print(f"[DEBUG] Error in upsert_chain_timeseries: {e}")
        raise