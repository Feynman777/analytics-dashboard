from datetime import datetime, timezone
from helpers.fetch.fee_data import fetch_fee_series
from helpers.utils.sync_state import get_last_sync, update_last_sync

SECTION_KEY = "Fee_Series"

def sync_fee_series():
    now = datetime.now(timezone.utc)
    start = get_last_sync(SECTION_KEY).replace(tzinfo=timezone.utc)

    print(f"🔁 Running sync_fee_series from {start.date()} to {now.date()}")

    try:
        df = fetch_fee_series(start=start)

        if df.empty or "date" not in df.columns:
            print("⚠️ No fee data found or missing 'date' column.")
            update_last_sync(SECTION_KEY, now)
            return

        print(f"📊 Found {len(df)} fee records (from {df['date'].min()} to {df['date'].max()})")
        # No upsert step — display summary only
        print(df.head())

        update_last_sync(SECTION_KEY, now)
        print(f"✅ Fee series sync complete. Last sync updated to {now.isoformat()}")

    except Exception as e:
        print(f"❌ Error syncing fee series: {e}")
