# helpers/sync/transactions.py

from datetime import datetime, timezone
from helpers.upsert.transactions import upsert_transactions_from_activity
from helpers.sync.utils import get_last_sync, update_last_sync

SECTION = "Transactions"


def sync_transaction_cache():
    now = datetime.now(timezone.utc)
    last_sync = get_last_sync(SECTION).replace(tzinfo=timezone.utc)

    print(f"🔁 Syncing transaction cache from {last_sync.isoformat()} to {now.isoformat()}")
    upsert_transactions_from_activity(start=last_sync)

    from helpers.connection import get_cache_db_connection
    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(created_at) FROM transactions_cache")
            latest_ts = cur.fetchone()[0] or now
            update_last_sync(SECTION, latest_ts)

    print(f"✅ Transaction cache synced. Last sync updated to {latest_ts}")