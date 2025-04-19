import os
import psycopg2

# === MAIN DB credentials ===
DB_HOST_db = os.getenv("DB_HOST")
DB_PORT_db = os.getenv("DB_PORT")
DB_NAME_db = os.getenv("DB_NAME")
DB_USER_db = os.getenv("DB_USER")
DB_PASS_db = os.getenv("DB_PASS")

# === CACHE DB credentials ===
DB_HOST_cache = os.getenv("CACHE_DB_HOST")
DB_PORT_cache = os.getenv("CACHE_DB_PORT")
DB_NAME_cache = os.getenv("CACHE_DB_NAME")
DB_USER_cache = os.getenv("CACHE_DB_USER")
DB_PASS_cache = os.getenv("CACHE_DB_PASS")

def get_cache_db_connection():
    return psycopg2.connect(
        host=DB_HOST_cache,
        port=int(DB_PORT_cache),
        database=DB_NAME_cache,
        user=DB_USER_cache,
        password=DB_PASS_cache
    )

def get_main_db_connection():
    return psycopg2.connect(
        host=DB_HOST_db,
        port=int(DB_PORT_db),
        database=DB_NAME_db,
        user=DB_USER_db,
        password=DB_PASS_db
    )
