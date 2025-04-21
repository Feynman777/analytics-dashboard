import os
import psycopg2
from helpers.utils.env_utils import get_env_or_secret

# === MAIN DB ===
DB_HOST_db = get_env_or_secret("DB_HOST", section="database")
DB_PORT_db = int(get_env_or_secret("DB_PORT", section="database", default="29661"))
DB_NAME_db = get_env_or_secret("DB_NAME", section="database")
DB_USER_db = get_env_or_secret("DB_USER", section="database")
DB_PASS_db = get_env_or_secret("DB_PASS", section="database")


# === CACHE DB ===
DB_HOST_cache = os.getenv("CACHE_DB_HOST") or get_env_or_secret("DB_HOST", section="cache_db")
DB_PORT_cache = int(os.getenv("CACHE_DB_PORT") or get_env_or_secret("DB_PORT", section="cache_db", default="49400"))
DB_NAME_cache = os.getenv("CACHE_DB_NAME") or get_env_or_secret("DB_NAME", section="cache_db")
DB_USER_cache = os.getenv("CACHE_DB_USER") or get_env_or_secret("DB_USER", section="cache_db")
DB_PASS_cache = os.getenv("CACHE_DB_PASS") or get_env_or_secret("DB_PASS", section="cache_db")


def get_cache_db_connection():
    return psycopg2.connect(
        host=DB_HOST_cache,
        port=DB_PORT_cache,
        database=DB_NAME_cache,
        user=DB_USER_cache,
        password=DB_PASS_cache
    )


def get_main_db_connection():
    return psycopg2.connect(
        host=DB_HOST_db,
        port=DB_PORT_db,
        database=DB_NAME_db,
        user=DB_USER_db,
        password=DB_PASS_db
    )
