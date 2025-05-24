import os
import psycopg2

def get_direct_cache_connection():
    return psycopg2.connect(
        host=os.environ["CACHE_DB_HOST"],
        port=os.environ["CACHE_DB_PORT"],
        dbname=os.environ["CACHE_DB_NAME"],
        user=os.environ["CACHE_DB_USER"],
        password=os.environ["CACHE_DB_PASS"],
    )
