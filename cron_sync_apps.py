# === cron_sync_apps.py ===
import os
import base64
import pandas as pd
from datetime import datetime, timedelta
from helpers.upsert.daily_app_downloads import upsert_daily_app_downloads
from helpers.upsert.daily_app_downloads import fetch_daily_installs_from_bigquery
from helpers.connection_direct import get_direct_cache_connection  # 👈 use direct method

# Decode BQ service account key
if "BQ_KEY_BASE64" in os.environ:
    key_path = "/tmp/firebase-bq-key.json"
    with open(key_path, "wb") as f:
        f.write(base64.b64decode(os.environ["BQ_KEY_BASE64"]))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

if __name__ == "__main__":
    print("✅ Starting cron_sync_apps.py...")
    print("🔐 GOOGLE_APPLICATION_CREDENTIALS =", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

    df = fetch_daily_installs_from_bigquery()
    if df.empty:
        print("⚠️ No data returned from BigQuery.")
    else:
        print(f"📊 Retrieved {len(df)} rows from BigQuery.")
        with get_direct_cache_connection() as conn:
            upsert_daily_app_downloads(df, conn=conn)

