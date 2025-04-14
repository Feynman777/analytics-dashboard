import streamlit as st
from datetime import datetime, timedelta, timezone
import json
import os
from helpers.upsert import upsert_transactions_from_activity, get_latest_cached_timestamp
from helpers.connection import get_cache_db_connection

SYNC_FILE = "last_sync.json"

# === Sync Wrapper for Transactions Page ===
def sync_transaction_cache(force=False):
    from helpers.connection import get_cache_db_connection
    SECTION_KEY = "Transactions"
    now = datetime.now(timezone.utc)

    print("🔁 Running sync_transaction_cache")
    upsert_transactions_from_activity(force=force)

    # Get latest created_at from cache table
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(created_at) FROM transactions_cache")
                latest_ts = cursor.fetchone()[0]
                if latest_ts:
                    update_last_sync(SECTION_KEY, latest_ts)
                    print(f"✅ last_sync.json updated using latest from cache: {latest_ts}")
                else:
                    update_last_sync(SECTION_KEY, now)
                    print(f"⚠️ No data found in cache. Defaulting to now: {now}")
    except Exception as e:
        print(f"❌ Error updating last_sync.json from cache: {e}")
        update_last_sync(SECTION_KEY, now)

    print(f"✅ Sync complete at: {now}")


def get_last_sync(section: str) -> datetime:
    print(f"🕐 Fetching last sync for: {section}")
    try:
        if os.path.exists(SYNC_FILE):
            with open(SYNC_FILE, "r") as f:
                data = json.load(f)
                raw = data.get(section)
                if raw:
                    dt = datetime.fromisoformat(raw)
                    # Ensure it's timezone-aware
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
    except Exception as e:
        print(f"❌ Error reading last_sync.json: {e}")
    return datetime(2024, 1, 1, tzinfo=timezone.utc)

def update_last_sync(section: str, sync_datetime: datetime):
    print(f"💾 Updating last sync for: {section} → {sync_datetime.isoformat()}")
    print(f"🔍 Sync file path: {os.path.abspath(SYNC_FILE)}")

    try:
        data = {}
        if os.path.exists(SYNC_FILE):
            with open(SYNC_FILE, "r") as f:
                data = json.load(f)

        data[section] = sync_datetime.isoformat()

        with open(SYNC_FILE, "w") as f:
            json.dump(data, f, indent=2)

        print("✅ Sync file updated successfully.")

    except Exception as e:
        print(f"❌ Error writing to last_sync.json: {e}")

def sync_section(section_name: str, sync_callback):
    now = datetime.now(timezone.utc)
    last_sync = get_last_sync(section_name)

    force = st.button(f"🔁 Force Sync {section_name.replace('_', ' ')}")

    if force or (now - last_sync >= timedelta(hours=4)):
        with st.spinner(f"Syncing {section_name.replace('_', ' ')} from {last_sync.date()} to {now.date()}..."):
            try:
                sync_callback(last_sync, now)
                update_last_sync(section_name, now)
                st.success(f"✅ {section_name.replace('_', ' ')} synced successfully.")
            except Exception as e:
                st.error(f"❌ Sync failed: {e}")
    else:
        next_sync = last_sync + timedelta(hours=4)
        minutes_remaining = int((next_sync - now).total_seconds() / 60)

        st.info(f"""
        ✅ Last synced at: `{last_sync.strftime('%Y-%m-%d %H:%M')} UTC`  
        ⏳ Skipping update — next sync available at: `{next_sync.strftime('%Y-%m-%d %H:%M')} UTC`  
        🕒 Approximately **{minutes_remaining} minutes** from now.
        """)
