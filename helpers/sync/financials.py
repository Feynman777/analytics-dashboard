from datetime import datetime, timedelta
import pandas as pd
from helpers.fetch.revenue import fetch_avg_revenue_metrics, fetch_avg_revenue_metrics_for_range
from helpers.fetch.fees import fetch_fee_series
from helpers.upsert.revenue import upsert_weekly_avg_revenue_metrics
from helpers.upsert.fees import upsert_fee_series

def sync_financials(last_sync: datetime, now: datetime):
    # === Sync Fee Series ===
    df_fees = fetch_fee_series(start=last_sync)
    if not df_fees.empty:
        df_fees["date"] = pd.to_datetime(df_fees["date"]).dt.date
        df_fees = df_fees.groupby(["date", "chain"])["value"].sum().reset_index()
        upsert_fee_series(df_fees)

    # === Daily Snapshot Revenue (30d rolling)
    fetch_avg_revenue_metrics(days=30)

    # === Weekly Revenue Metrics ===
    start = last_sync.date()
    today = now.date()
    start_of_week = start - timedelta(days=start.weekday())  # round to Monday

    while start_of_week <= today:
        weekly_df = fetch_avg_revenue_metrics_for_range(start_date=start_of_week, days=7)
        if not weekly_df.empty:
            upsert_weekly_avg_revenue_metrics(weekly_df)
        start_of_week += timedelta(days=7)

    print(f"✅ Financials synced from {last_sync.date()} to {now.date()}")