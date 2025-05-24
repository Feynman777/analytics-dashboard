# helpers/sync/weekly_data.py

from datetime import datetime, timedelta, date
import pandas as pd

from helpers.api_utils import fetch_api_metric
from helpers.fetch.weekly_data import fetch_swap_series, fetch_weekly_avg_revenue_metrics
from helpers.upsert.avg_revenue import upsert_weekly_avg_revenue_metrics
from helpers.utils.sync_state import get_last_sync, update_last_sync
from helpers.upsert.weekly_stats import upsert_weekly_api_metrics

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
    #last_sync = datetime(2025, 5, 4)
    start_date = (last_sync - timedelta(hours=2)).date() if last_sync else datetime(2024, 1, 1).date()
    end_date = now.date()

    print(f"🔁 Syncing weekly data from {start_date} to {end_date}")

    all_api_metric_rows = []

    # === Sync swap volume from cache ===
    try:
        raw_swaps = fetch_swap_series(start=start_date, end=end_date)
        df_swaps = pd.DataFrame(raw_swaps)
        if not df_swaps.empty:
            print(f"📊 Retrieved {len(df_swaps)} rows of swap volume (not stored)")
    except Exception as e:
        print(f"❌ Error fetching swap volume: {e}")

    # === Sync API-driven metrics and collect results ===
    for metric, endpoint in API_ENDPOINTS.items():
        rows = []
        for d in pd.date_range(start=start_date, end=end_date):
            date_str = d.strftime("%Y-%m-%d")
            try:
                df = fetch_api_metric(endpoint, start=date_str, end=date_str)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    if "date" not in df.columns:
                        df["date"] = pd.to_datetime(date_str).date()
                    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)
                    df["metric"] = metric
                    rows.append(df)
            except Exception as e:
                print(f"❌ Error fetching {metric} for {date_str}: {e}")

        if rows:
            full_df = pd.concat(rows, ignore_index=True)
            print(f"📊 Retrieved {len(full_df)} rows for {metric} (not stored)")
            all_api_metric_rows.append(full_df)

    # === Aggregate and upsert API metrics weekly ===
    if all_api_metric_rows:
        api_df = pd.concat(all_api_metric_rows, ignore_index=True)
        api_df["week_start_date"] = pd.to_datetime(api_df["date"]).dt.to_period("W").apply(lambda r: r.start_time)
        weekly_api = api_df.groupby(["week_start_date", "metric"], as_index=False)["value"].sum()
        weekly_api["quantity"] = 0

        try:
            upsert_weekly_api_metrics(weekly_api)
        except Exception as e:
            print(f"❌ Error upserting weekly API metrics: {e}")

    update_last_sync(SECTION_KEY, now)
    print(f"✅ Weekly data sync complete. Last sync updated to {now.isoformat()}")

def sync_weekly_avg_revenue_metrics():
    """
    Syncs weekly average revenue metrics from the last sync date up to the current week.
    """
    now = datetime.utcnow()
    last_sync = get_last_sync(SECTION_KEY)

    if not last_sync:
        print("⚠️ No previous sync found for Weekly_Data. Defaulting to 2024-01-01.")
        last_sync = datetime(2024, 1, 1)

    if isinstance(last_sync, datetime):
        start_date = last_sync.date()
    elif isinstance(last_sync, pd.Timestamp):
        start_date = last_sync.to_pydatetime().date()
    else:
        start_date = last_sync  # assume it's already a datetime.date

    today = now.date()
    start_of_week = start_date - timedelta(days=start_date.weekday())  # align to Monday
    current_week = today - timedelta(days=today.weekday())

    print(f"🔁 Syncing weekly average revenue metrics from {start_of_week} to {current_week}")

    while start_of_week <= current_week:
        try:
            print(f"📅 Processing week: {start_of_week}")
            df = fetch_weekly_avg_revenue_metrics(start_of_week)
            if df is not None and not df.empty:
                print(f"📊 Upserting {len(df)} rows for week: {start_of_week}")
                upsert_weekly_avg_revenue_metrics(df)
            else:
                print(f"⚠️ No data returned for week: {start_of_week}")
        except Exception as e:
            print(f"❌ Error processing week {start_of_week}: {e}")
        start_of_week += timedelta(days=7)

    update_last_sync(SECTION_KEY, now)
    print(f"✅ Weekly average revenue metrics sync complete. Last sync updated to {now.isoformat()}")