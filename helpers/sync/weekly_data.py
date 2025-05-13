# helpers/sync/weekly_data.py

from datetime import datetime, timedelta
import pandas as pd

from helpers.api_utils import fetch_api_metric
from helpers.fetch.weekly_data import fetch_swap_series, fetch_weekly_stats
from helpers.upsert.avg_revenue import upsert_weekly_avg_revenue_metrics
from helpers.utils.sync_state import get_last_sync, update_last_sync

SECTION_KEY = "Weekly_Data"

API_ENDPOINTS = {
    "cash_volume": "user/cash/volume",
    "new_users": "user/new",
    "referrals": "user/referrals",
    "total_agents": "agents/deployed",
}

def sync_weekly_data():
    now = datetime.now(tz=datetime.utcnow().astimezone().tzinfo)
    last_sync = get_last_sync(SECTION_KEY)
    start_date = (last_sync - timedelta(hours=2)).date()
    end_date = now.date()

    print(f"🔁 Syncing weekly data from {start_date} to {end_date}")

    # === Sync swap volume from cache ===
    try:
        raw_swaps = fetch_swap_series(start=start_date, end=end_date)
        df_swaps = pd.DataFrame(raw_swaps)
        if not df_swaps.empty:
            print(f"📊 Retrieved {len(df_swaps)} rows of swap volume (not stored)")
    except Exception as e:
        print(f"❌ Error fetching swap volume: {e}")

    # === Sync API-driven metrics (log only) ===
    for metric, endpoint in API_ENDPOINTS.items():
        rows = []
        for d in pd.date_range(start=start_date, end=end_date):
            date_str = d.strftime("%Y-%m-%d")
            df = fetch_api_metric(endpoint, start=date_str, end=date_str)
            if isinstance(df, pd.DataFrame) and not df.empty:
                if "date" not in df.columns:
                    # 🔧 Manually assign the date if not provided by API
                    df["date"] = pd.to_datetime(date_str).date()
                df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)
                rows.append(df)

        if rows:
            full_df = pd.concat(rows, ignore_index=True)
            print(f"📊 Retrieved {len(full_df)} rows for {metric} (not stored)")

    update_last_sync(SECTION_KEY, now)
    print(f"✅ Weekly data sync complete. Last sync updated to {now.isoformat()}")

def sync_weekly_avg_revenue_metrics():
    last_sync = get_last_sync(SECTION_KEY)
    now = datetime.utcnow()

    # Sync full weeks between last sync and now
    start_date = last_sync.date()
    today = now.date()
    start_of_week = start_date - timedelta(days=start_date.weekday())  # Monday

    while start_of_week <= today:
        upsert_weekly_avg_revenue_metrics(start_of_week)
        start_of_week += timedelta(days=7)

    update_last_sync(SECTION_KEY, now)
