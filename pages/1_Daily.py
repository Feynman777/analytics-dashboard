import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone

from helpers.fetch import fetch_daily_stats, fetch_daily_user_stats
from utils.charts import daily_metric_section

# === CONFIG ===
st.set_page_config(page_title="Daily Stats Dashboard", layout="wide")
st.title("📊 Newmoney.AI Daily Stats Dashboard")

# === DATE RANGE ===
st.subheader("📅 Select date range:")
today = datetime.now(timezone.utc).date()  # Use UTC now
default_start = today - timedelta(days=60)
start_date, end_date = st.date_input("Date range:", (default_start, today))

# === LOAD DATA ===
df = fetch_daily_stats(start=start_date, end=end_date)
if df.empty:
    st.warning("No daily stats available for selected range.")
    st.stop()

# === GROUP BY DATE (data is already summed across chains in SQL) ===
daily = df.groupby("date").sum(numeric_only=True).reset_index()

# === RENDER CHARTS ===
col1, col2 = st.columns(2)
with col1:
    st.altair_chart(daily_metric_section(daily, "Daily Transactions (SWAP)", "Count", col="swap_transactions"), use_container_width=True)
with col2:
    st.altair_chart(daily_metric_section(daily, "Daily Swap Volume", "USD", col="swap_volume"), use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    st.altair_chart(daily_metric_section(daily, "Daily Send Volume", "USD", col="send_volume"), use_container_width=True)
with col4:
    st.altair_chart(daily_metric_section(daily, "Daily Cash Volume", "USD", col="cash_volume"), use_container_width=True)

col5, col6 = st.columns(2)
with col5:
    st.altair_chart(daily_metric_section(daily, "Daily Active Users", "Users", col="active_users"), use_container_width=True)
with col6:
    st.altair_chart(daily_metric_section(daily, "Daily Revenue", "USD", col="revenue"), use_container_width=True)

col7, col8 = st.columns(2)
with col7:
    st.altair_chart(daily_metric_section(daily, "Daily Referrals", "Count", col="referrals"), use_container_width=True)
with col8:
    st.altair_chart(daily_metric_section(daily, "Agents Deployed", "Count", col="agents_deployed"), use_container_width=True)

'''col9, _ = st.columns(2)
with col9:
    st.altair_chart(
        daily_metric_section(daily, "Daily New Active Users", "Users", col="new_active_users"),
        use_container_width=True
    )'''
