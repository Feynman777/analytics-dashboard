import os
import psycopg2
from urllib.parse import urljoin


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
    """Establish the connection to the main database."""
    conn = psycopg2.connect(
        host=DB_HOST_db,
        port=int(DB_PORT_db),
        database=DB_NAME_db,
        user=DB_USER_db,
        password=DB_PASS_db
    )
    return conn

# === Base URL ===
API_BASE_URL = os.getenv("API_BASE_URL", "https://newmoney-ai-analytics-prod-production.up.railway.app/")

# === API Endpoints ===
cash_volume      = urljoin(API_BASE_URL, "user/cash/volume")
active_users     = urljoin(API_BASE_URL, "user/active")
new_users        = urljoin(API_BASE_URL, "user/new")
referrals        = urljoin(API_BASE_URL, "user/referrals")
total_users      = urljoin(API_BASE_URL, "user/total")
total_agents     = urljoin(API_BASE_URL, "agents/deployed")
user_full_metrics = urljoin(API_BASE_URL, "user/metrics/")
user_volume      = urljoin(API_BASE_URL, "user/metrics/volume/")
user_agents      = urljoin(API_BASE_URL, "agents/user/")

# === Auth Key ===
AUTH_KEY = os.getenv("AUTH_KEY", "dev-default-key")