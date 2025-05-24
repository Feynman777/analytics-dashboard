# === cron_sync.py ===

from datetime import datetime
import os

from helpers.sync.transactions import sync_transaction_cache
from helpers.sync.daily_stats import sync_daily_stats
from helpers.sync.fees import sync_fee_series
from helpers.sync.weekly_data import sync_weekly_data, sync_weekly_avg_revenue_metrics
from helpers.upsert.weekly_stats import upsert_weekly_swap_revenue
from helpers.upsert.users import upsert_users
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

# === Sync weekly swap revenue ===
try:
    with get_cache_db_connection() as conn:
        upsert_weekly_swap_revenue(conn)
    print("✅ Finished syncing weekly swap revenue")
except Exception as e:
    print("❌ Error syncing weekly swap revenue:", e)

# === Sync weekly avg revenue per active user ===
try:
    sync_weekly_avg_revenue_metrics()
    print("✅ Finished syncing weekly avg revenue metrics")
except Exception as e:
    print("❌ Error syncing weekly avg revenue metrics:", e)

# === Sync users table ===
try:
    upsert_users()
    print("✅ Finished syncing users table")
except Exception as e:
    print("❌ Error syncing users table:", e)

print("🎉 Cron sync completed at:", datetime.utcnow())
