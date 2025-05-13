from datetime import datetime, timezone
from helpers.upsert.transactions import upsert_transactions_from_activity
from helpers.utils.sync_state import get_last_sync, update_last_sync
from helpers.connection import get_cache_db_connection

SECTION = "Transactions"

def sync_transaction_cache():
    now = datetime.now(timezone.utc)
    last_sync = get_last_sync(SECTION)
    last_sync = last_sync if last_sync.tzinfo else last_sync.replace(tzinfo=timezone.utc)

    print(f"🔁 Starting sync for `{SECTION}` from {last_sync.isoformat()} → {now.isoformat()}")

    # === Step 1: Upsert transactions from Activity
    upsert_transactions_from_activity(start=last_sync)

    # === Step 2: Update sync state using actual latest timestamp in DB
    latest_ts = now
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(created_at) FROM transactions_cache")
                result = cur.fetchone()
                if result and result[0]:
                    latest_ts = result[0].replace(tzinfo=timezone.utc)
    except Exception as e:
        print(f"⚠️ Warning: failed to retrieve latest created_at: {e}")

    update_last_sync(SECTION, latest_ts)
    print(f"✅ Finished syncing `{SECTION}`. Updated last sync to {latest_ts.isoformat()}")
