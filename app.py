#app.py
import streamlit as st
from datetime import datetime, timezone
from helpers.cache_db import fetch_timeseries

# === Streamlit Config ===
st.set_page_config(page_title="Newmoney.AI Analytics", layout="wide")
st.title("📊 Newmoney.AI Analytics Dashboard")

st.markdown("""
Welcome to the Newmoney.AI dashboard. Use the navigation sidebar to view:
- **Weekly Data** for high-level product analytics
- **User Data** for individual volume tracking
""")