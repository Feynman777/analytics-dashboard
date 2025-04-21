from datetime import datetime
import os

from helpers.sync.transactions import sync_transaction_cache
from helpers.sync.daily import sync_daily_stats
from helpers.sync.fees import sync_fee_series
from helpers.sync.weekly import sync_weekly_data
from helpers.connection import get_main_db_connection, get_cache_db_connection

# === Start log ===
print("\n🔁 Cron sync started at:", datetime.utcnow())
print("🌐 ENVIRONMENT:", os.getenv("RAILWAY_ENVIRONMENT", "unknown"))

# === Test DB connections ===
for label, conn_fn in [("MAIN", get_main_db_connection), ("CACHE", get_cache_db_connection)]:
    try:
        conn = conn_fn()
        print(f"✅ Connected to {label} DB")
        conn.close()
    except Exception as e:
        print(f"❌ Failed to connect to {label} DB:", e)

# === Run sync jobs ===
for label, fn in [
    ("transaction cache", sync_transaction_cache),
    ("daily stats", sync_daily_stats),
    ("fee series", sync_fee_series),
    ("weekly data", sync_weekly_data),
]:
    try:
        fn()
        print(f"✅ Finished syncing {label}")
    except Exception as e:
        print(f"❌ Error syncing {label}:", e)

print("🎉 Cron sync completed at:", datetime.utcnow())
