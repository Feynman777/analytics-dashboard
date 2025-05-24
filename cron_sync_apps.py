# === cron_sync_apps.py ===
import os
from helpers.upsert.daily_app_downloads import sync_daily_app_downloads

if __name__ == "__main__":
    print("🚀 Running App Download Sync script")
    print("🔍 GOOGLE_APPLICATION_CREDENTIALS =", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

    if not os.path.exists(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")):
        print("❌ Credential file not found. Exiting.")
        exit(1)

    sync_daily_app_downloads(start="2025-05-20")
