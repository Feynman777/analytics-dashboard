from datetime import datetime
import os

from helpers.sync_utils import (
    sync_transaction_cache,
    sync_daily_stats,
    sync_fee_series,
    sync_financials,
    sync_weekly_data,
)
from helpers.connection import get_main_db_connection, get_cache_db_connection

# === Start log ===
print("🔁 Cron sync started at:", datetime.utcnow())
print("🌐 ENVIRONMENT:", os.getenv("RAILWAY_ENVIRONMENT", "unknown"))

# === Test DB connections ===
try:
    conn = get_main_db_connection()
    print("✅ Connected to MAIN DB")
    conn.close()
except Exception as e:
    print("❌ Failed to connect to MAIN DB:", e)

try:
    conn = get_cache_db_connection()
    print("✅ Connected to CACHE DB")
    conn.close()
except Exception as e:
    print("❌ Failed to connect to CACHE DB:", e)

# === Run sync jobs ===
try:
    sync_transaction_cache()
    print("✅ Finished syncing transaction cache")
except Exception as e:
    print("❌ Error syncing transaction cache:", e)

try:
    sync_daily_stats()
    print("✅ Finished syncing daily stats")
except Exception as e:
    print("❌ Error syncing daily stats:", e)

try:
    sync_fee_series()
    print("✅ Finished syncing fee series")
except Exception as e:
    print("❌ Error syncing fee series:", e)

try:
    sync_financials()
    print("✅ Finished syncing financials")
except Exception as e:
    print("❌ Error syncing financials:", e)

try:
    sync_weekly_data()
    print("✅ Finished syncing weekly data")
except Exception as e:
    print("❌ Error syncing weekly data:", e)

# === End log ===
print("🎉 Cron sync completed at:", datetime.utcnow())
