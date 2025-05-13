from datetime import datetime, timezone, timedelta
from helpers.upsert.daily_stats import upsert_daily_stats
from helpers.upsert.daily_user_stats import upsert_daily_user_stats
from helpers.connection import get_cache_db_connection
from helpers.utils.sync_state import get_last_sync, update_last_sync

SECTION_KEY = "Daily_Stats"

def sync_daily_stats() -> None:
    now = datetime.now(timezone.utc)

    # Get last sync or fallback to 24h ago
    last_sync = get_last_sync(SECTION_KEY)
    if not last_sync:
        print("⚠️ No last sync found. Defaulting to 24h ago.")
        last_sync = now - timedelta(days=1)

    last_sync = last_sync.replace(tzinfo=timezone.utc)
    start = last_sync.replace(hour=0, minute=0, second=0, microsecond=0)

    print(f"🔁 Starting sync for daily stats from {start.isoformat()}")

    try:
        with get_cache_db_connection() as conn:
            upsert_daily_stats(start=start, conn=conn)
            upsert_daily_user_stats(start=start, conn=conn)
        update_last_sync(SECTION_KEY, now)
        print(f"✅ Daily stats synced successfully. Last sync updated to {now.isoformat()}")
    except Exception as e:
        print(f"❌ Failed to sync daily stats: {e}")
        update_last_sync(SECTION_KEY, now)
