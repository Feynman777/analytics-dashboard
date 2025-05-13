from datetime import datetime, timedelta
import pandas as pd

from helpers.fetch.financials import fetch_avg_revenue_metrics, fetch_avg_revenue_metrics_for_range
from helpers.fetch.fee_data import fetch_fee_series
from helpers.upsert.avg_revenue import upsert_weekly_avg_revenue_metrics


def sync_financials(last_sync: datetime, now: datetime):
    # === Ensure UTC timezone
    last_sync = last_sync if last_sync.tzinfo else last_sync.replace(tzinfo=datetime.utcnow().astimezone().tzinfo)
    now = now if now.tzinfo else now.replace(tzinfo=datetime.utcnow().astimezone().tzinfo)

    print(f"🔁 Syncing financials from {last_sync.date()} to {now.date()}")

    # === 1. Fee Series Preview (No longer upserting to timeseries_fees)
    df_fees = fetch_fee_series(start=last_sync)
    if not df_fees.empty:
        df_fees["date"] = pd.to_datetime(df_fees["date"]).dt.date
        print(f"📊 Preview: {len(df_fees)} fee rows (not upserted)")
        print(df_fees.head())
    else:
        print("⚠️ No new fee data found.")

    # === 2. Daily Revenue Snapshot (rolling 30d)
    fetch_avg_revenue_metrics(days=30)
    print("✅ Daily average revenue metrics fetched")

    # === 3. Weekly Revenue Metrics
    start_date = last_sync.date()
    end_date = now.date()
    start_of_week = start_date - timedelta(days=start_date.weekday())  # align to Monday

    while start_of_week <= end_date:
        weekly_df = fetch_avg_revenue_metrics_for_range(start_date=start_of_week, days=7)
        if not weekly_df.empty:
            upsert_weekly_avg_revenue_metrics(weekly_df)
            print(f"📅 Weekly revenue upserted for week starting {start_of_week}")
        else:
            print(f"⚠️ No revenue data found for week of {start_of_week}")
        start_of_week += timedelta(days=7)

    print(f"🎉 Financials sync complete → {last_sync.date()} to {now.date()}")
