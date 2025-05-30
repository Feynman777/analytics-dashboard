# pages/1_Daily.py

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone
from helpers.connection import get_cache_db_connection
from helpers.fetch.daily import fetch_daily_stats, fetch_total_balances
from helpers.utils.charts import daily_metric_section
from helpers.fetch.app_data import fetch_daily_app_downloads
from helpers.utils.charts import total_balance_chart
import altair as alt

# === CONFIG ===
st.set_page_config(page_title="Daily Stats Dashboard", layout="wide")
st.title("📊 Daily Stats")

# === DATE RANGE ===
st.subheader("📅 Select date range:")
today = datetime.now(timezone.utc).date()
default_start = today - timedelta(days=60)
start_date, end_date = st.date_input("Date range:", (default_start, today))

# === LOAD BASE DAILY STATS ===
df_apps = fetch_daily_app_downloads()
df_apps["date"] = pd.to_datetime(df_apps["date"])  # 🔧 Ensure datetime64 format

start_ts = pd.Timestamp(start_date)
end_ts = pd.Timestamp(end_date)

df_apps = df_apps[(df_apps["date"] >= start_ts) & (df_apps["date"] <= end_ts)]

df = fetch_daily_stats(start=start_date, end=end_date)
if df.empty:
    st.warning("No daily stats available for selected range.")
    st.stop()

# === LOAD daily_user_stats and JOIN ===
with get_cache_db_connection() as conn:
    user_df = pd.read_sql("""
        SELECT date, new_users, new_active_users
        FROM daily_user_stats
        WHERE date >= %s AND date <= %s
    """, conn, params=(start_date, end_date))

df["date"] = pd.to_datetime(df["date"])
user_df["date"] = pd.to_datetime(user_df["date"])
df = pd.merge(df, user_df, on="date", how="left")

# Fill any missing user stats with 0
df["new_users"] = df["new_users"].fillna(0).astype(int)
df["new_active_users"] = df["new_active_users"].fillna(0).astype(int)

# === Compute daily aggregate ===
expected_cols = [
    "swap_transactions", "swap_volume", "send_volume", "cash_volume",
    "active_users", "revenue", "referrals", "agents_deployed",
    "new_users", "new_active_users"
]

for col in expected_cols:
    if col not in df.columns:
        df[col] = 0

daily = df.groupby("date")[expected_cols].sum().reset_index()

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

col9, col10 = st.columns(2)
with col9:
    st.altair_chart(daily_metric_section(daily, "New Users", "Users", col="new_users"), use_container_width=True)
with col10:
    st.altair_chart(daily_metric_section(daily, "New Active Users", "Users", col="new_active_users"), use_container_width=True)

st.subheader("💰 Total Balance Over Time")

balance_df = pd.DataFrame(fetch_total_balances())
balance_df["date"] = pd.to_datetime(balance_df["date"])
balance_df["total_balance_usd"] = pd.to_numeric(balance_df["total_balance_usd"], errors="coerce")
filtered_df = balance_df[(balance_df["date"] >= pd.Timestamp(start_date)) & (balance_df["date"] <= pd.Timestamp(end_date))]

col_left, _ = st.columns(2)
with col_left:
    st.line_chart(filtered_df.set_index("date")["total_balance_usd"])