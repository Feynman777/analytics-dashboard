from datetime import datetime, timedelta
import pandas as pd

from helpers.fetch.metrics.api_metrics import fetch_api_metric
from helpers.fetch.metrics.swap_metrics import fetch_swap_series
from helpers.upsert.timeseries import upsert_chain_timeseries, upsert_timeseries
from helpers.sync.utils import get_last_sync, update_last_sync

SECTION_KEY = "Weekly_Data"
API_ENDPOINTS = {
    "cash_volume": "user/cash/volume",
    "new_users": "user/new",
    "referrals": "user/referrals",
    "total_agents": "agents/deployed",
}

def sync_weekly_data():
    now = datetime.utcnow()
    last_sync = get_last_sync(SECTION_KEY)
    start_date = (last_sync - timedelta(hours=2)).date()
    end_date = now.date()

    print(f"🔁 Syncing weekly data from {start_date} to {end_date}")

    # === Sync swap volume ===
    raw_swaps = fetch_swap_series(start=start_date, end=end_date)
    df_swaps = pd.DataFrame(raw_swaps)
    if not df_swaps.empty:
        df_swaps["date"] = pd.to_datetime(df_swaps["date"]).dt.date
        df_swaps["metric"] = "swap_volume"
        df_swaps["status"] = "success"
        df_swaps["quantity"] = df_swaps["quantity"].astype(int)
        upsert_chain_timeseries(df_swaps)

    # === Sync API metrics ===
    for metric, endpoint in API_ENDPOINTS.items():
        rows = []
        for d in pd.date_range(start=start_date, end=end_date):
            date_str = d.strftime("%Y-%m-%d")
            df = fetch_api_metric(endpoint, start=date_str, end=date_str)
            if isinstance(df, pd.DataFrame) and not df.empty:
                df["date"] = pd.to_datetime(df["date"]).dt.date
                df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)
                rows.append(df)

        if rows:
            all_df = pd.concat(rows, ignore_index=True)
            upsert_timeseries(metric, all_df)

    update_last_sync(SECTION_KEY, now)
    print(f"✅ Weekly data sync complete. Last sync updated to {now.isoformat()}")
