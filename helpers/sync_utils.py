import streamlit as st
from datetime import datetime, timedelta, timezone
import json
import os

SYNC_FILE = "last_sync.json"

def get_last_sync(section: str) -> datetime:
    if os.path.exists(SYNC_FILE):
        with open(SYNC_FILE, "r") as f:
            data = json.load(f)
            raw = data.get(section)
            if raw:
                try:
                    return datetime.fromisoformat(raw)
                except Exception:
                    pass
    return datetime(2024, 1, 1, tzinfo=timezone.utc)

def update_last_sync(section: str, sync_datetime: datetime):
    data = {}
    if os.path.exists(SYNC_FILE):
        with open(SYNC_FILE, "r") as f:
            data = json.load(f)
    data[section] = sync_datetime.isoformat()
    with open(SYNC_FILE, "w") as f:
        json.dump(data, f, indent=2)

def sync_section(section_name: str, sync_callback):
    """
    Handles sync logic for a section.
    
    Parameters:
    - section_name: The key in last_sync.json (e.g., "Weekly_Data")
    - sync_callback: Function to call if sync is needed or forced
    """
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
