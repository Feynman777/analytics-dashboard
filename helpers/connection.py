from streamlit import secrets
import psycopg2

# Fetch credentials from secrets.toml
DB_HOST_db = secrets["database"]["DB_HOST"]
DB_PORT_db = secrets["database"]["DB_PORT"]
DB_NAME_db = secrets["database"]["DB_NAME"]
DB_USER_db = secrets["database"]["DB_USER"]
DB_PASS_db = secrets["database"]["DB_PASS"]

DB_HOST_cache = secrets["cache_db"]["DB_HOST"]
DB_PORT_cache = secrets["cache_db"]["DB_PORT"]
DB_NAME_cache = secrets["cache_db"]["DB_NAME"]
DB_USER_cache = secrets["cache_db"]["DB_USER"]
DB_PASS_cache = secrets["cache_db"]["DB_PASS"]


def get_cache_db_connection():
    """Establish the connection to the cache database."""
    conn = psycopg2.connect(
        host=DB_HOST_cache,
        port=int(DB_PORT_cache),
        database=DB_NAME_cache,
        user=DB_USER_cache,
        password=DB_PASS_cache
    )
    return conn

def get_main_db_connection():
    """Establish the connection to the main production database."""
    conn = psycopg2.connect(
        host=DB_HOST_db,
        port=int(DB_PORT_db),
        database=DB_NAME_db,
        user=DB_USER_db,
        password=DB_PASS_db
    )
    return conn