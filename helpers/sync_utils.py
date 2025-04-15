import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, timezone
import json
import os
from helpers.upsert import upsert_transactions_from_activity
from helpers.connection import get_cache_db_connection
from helpers.upsert import upsert_fee_series, upsert_weekly_avg_revenue_metrics

SYNC_FILE = "last_sync.json"

SECTION_DELTA_MAP = {
    "Transactions": timedelta(hours=2),
    "Financials": timedelta(hours=2),
    "Weekly_Data": timedelta(hours=2),
}

def patch_sui_failures_as_success(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE transactions_cache
            SET status = 'SUCCESS'
            WHERE from_chain = 'sui'
              AND status = 'FAIL'
              AND tx_hash IS NOT NULL
              AND LENGTH(tx_hash) > 10
        """)
        print(f"[PATCH] Corrected {cursor.rowcount} misclassified SUI transactions")
        conn.commit()

def sync_transaction_cache(force=False):
    SECTION_KEY = "Transactions"
    now = datetime.now(timezone.utc)

    if force:
        start = now - timedelta(hours=4)
        end = now
    else:
        start = get_last_sync(SECTION_KEY)
        end = now

    print("🔁 Running sync_transaction_cache")
    print(f"⏱️ Syncing from: {start} → {end} (force={force})")

    upsert_transactions_from_activity(force=force, start=start, end=end)

    print("🧾 Syncing fee series after transaction upsert")
    sync_fee_series()
    
    # Patch + update sync timestamp
    try:
        with get_cache_db_connection() as conn:
            patch_sui_failures_as_success(conn)
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
    from helpers.fetch import get_last_sync
    now = datetime.now(timezone.utc)
    last_sync = get_last_sync(section_name)
    delta = SECTION_DELTA_MAP.get(section_name, timedelta(hours=4))
    force = st.button(f"🔁 Force Sync {section_name.replace('_', ' ')}")
    if force or (now - last_sync >= delta):
        with st.spinner(f"Syncing {section_name.replace('_', ' ')} from {last_sync.date()} to {now.date()}..."):
            try:
                sync_callback(last_sync, now)
                update_last_sync(section_name, now)
                st.success(f"✅ {section_name.replace('_', ' ')} synced successfully.")
            except Exception as e:
                st.error(f"❌ Sync failed: {e}")
    else:
        next_sync = last_sync + delta
        minutes_remaining = int((next_sync - now).total_seconds() / 60)
        st.info(f"""
        ✅ Last synced at: `{last_sync.strftime('%Y-%m-%d %H:%M')} UTC`  
        ⏳ Skipping update — next sync available at: `{next_sync.strftime('%Y-%m-%d %H:%M')} UTC`  
        🕒 Approximately **{minutes_remaining} minutes** from now.
        """)

def sync_fee_series():
    from helpers.fee_utils import fetch_fee_series
    print("\n🔁 Running sync_fee_series")
    SECTION_KEY = "Transactions"
    now = datetime.now(timezone.utc)
    last_sync = get_last_sync(SECTION_KEY)
    last_sync_date = last_sync.date()
    df = fetch_fee_series()
    df = df[df["date"] >= pd.to_datetime(last_sync_date)]
    if df.empty:
        print("✅ No new fee data to sync.")
        return
    print(f"📊 Found {len(df)} fee records to upsert (from {df['date'].min().date()} to {df['date'].max().date()})")
    batch_size = 100
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i+batch_size]
        print(f"  ⏳ Upserting batch {i//batch_size + 1}: rows {i} → {i+len(batch_df)}")
        upsert_fee_series(batch_df)
    latest_ts = df["date"].max().to_pydatetime().replace(tzinfo=timezone.utc)
    update_last_sync(SECTION_KEY, latest_ts)
    print(f"✅ Fee series sync complete. Last sync updated to {latest_ts.isoformat()}")