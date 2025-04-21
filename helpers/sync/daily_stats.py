from datetime import datetime, timezone
from helpers.upsert import upsert_daily_stats, upsert_daily_user_stats
from helpers.connection import get_cache_db_connection
from helpers.sync.state import get_last_sync, update_last_sync

SECTION_KEY = "Daily_Stats"


def sync_daily_stats():
    now = datetime.now(timezone.utc)
    last_sync = get_last_sync(SECTION_KEY).replace(tzinfo=timezone.utc)
    start = last_sync.replace(hour=0, minute=0, second=0, microsecond=0)

    print(f"🔁 Running sync_daily_stats from {start}")

    try:
        with get_cache_db_connection() as conn:
            upsert_daily_stats(start=start, conn=conn)
            upsert_daily_user_stats(start=start, conn=conn)
            update_last_sync(SECTION_KEY, now)
            print(f"✅ Daily stats synced and last sync updated to {now}")
    except Exception as e:
        print(f"❌ Error syncing daily stats: {e}")
        update_last_sync(SECTION_KEY, now)
