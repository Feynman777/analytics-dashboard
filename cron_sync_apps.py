import os
import base64
from helpers.upsert.daily_app_downloads import sync_daily_app_downloads
import streamlit as st

# Decode base64 key from secrets if running in Streamlit
if "google" in st.secrets:
    key_path = "/tmp/firebase-bq-key.json"
    with open(key_path, "wb") as f:
        f.write(base64.b64decode(st.secrets["google"]["bq_key_base64"]))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

if __name__ == "__main__":
    sync_daily_app_downloads(start="2025-05-20")