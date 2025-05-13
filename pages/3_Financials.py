import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from charts.financials.fee_distribution import render_fee_distribution
from helpers.connection import get_cache_db_connection
from helpers.fetch.financials import fetch_avg_revenue_metrics, fetch_weekly_avg_revenue_metrics
from helpers.fetch.weekly_data import fetch_weekly_stats
from charts.financials.weekly_fees import render_weekly_fees
from charts.financials.daily_fees import render_daily_fees
from charts.financials.weekly_avg_rev import render_weekly_avg_rev
from helpers.utils.constants import CHAIN_ID_MAP

# === PAGE SETUP ===
st.set_page_config(page_title="Financials", layout="wide")
st.title("💰 Financial Stats")

# === LOAD DAILY FEE DATA ===
def load_daily_fees(start_date=None, end_date=None):
    try:
        with get_cache_db_connection() as conn:
            query = """
                SELECT date, chain_name AS chain, swap_revenue AS value
                FROM daily_stats
                WHERE swap_revenue IS NOT NULL
            """
            if start_date and end_date:
                query += " AND date BETWEEN %s AND %s"
                df = pd.read_sql(query, conn, params=(start_date, end_date))
            else:
                df = pd.read_sql(query, conn)
            df["date"] = pd.to_datetime(df["date"])
            return df
    except Exception as e:
        st.error(f"Database fetch error: {e}")
        return pd.DataFrame()

# === DATE FILTER ===
st.subheader("📅 Filter by Date Range")
use_last_30 = st.checkbox("Use Last 30 Days", value=False)

if use_last_30:
    df = load_daily_fees()
    end_date = df["date"].max().date()
    start_date = end_date - timedelta(days=29)
else:
    df = load_daily_fees()
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    default_start = datetime(2025, 1, 1).date()
    start_date, end_date = st.date_input(
        "Select date range:",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date
    )

if df.empty:
    st.warning("No data available yet.")
    st.stop()

# === GROUPING ===
filtered_df = df[(df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)].copy()
filtered_df["week"] = filtered_df["date"].dt.to_period("W").apply(lambda r: r.start_time)
filtered_df["month"] = filtered_df["date"].dt.to_period("M").apply(lambda r: r.start_time)
weekly_fees = filtered_df.groupby("week", as_index=False)["value"].sum().sort_values("week")
daily_fees = filtered_df.groupby("date", as_index=False)["value"].sum()

# === METRICS ===
total_fees_range = filtered_df["value"].sum()
avg_fees_per_day = daily_fees["value"].mean()

st.subheader("📊 Summary Metrics")
col1, col2 = st.columns(2)
col1.metric("Total Fees in Date Range", f"${total_fees_range:,.2f}")
col2.metric("Average Fees per Day", f"${avg_fees_per_day:,.2f}")

# === WEEKLY + DAILY CHARTS ===
st.subheader("📈 Swap Fees")
col3, col4 = st.columns(2)
with col3:
    st.altair_chart(render_weekly_fees(weekly_fees), use_container_width=True)
with col4:
    st.altair_chart(render_daily_fees(daily_fees), use_container_width=True)

# === MONTHLY METRICS ===
df["month"] = df["date"].dt.to_period("M").apply(lambda r: r.start_time)
latest_month = df["month"].max()
prev_month = latest_month - pd.DateOffset(months=1)

last_month_total = df[df["month"] == prev_month]["value"].sum()
current_month_total = df[df["month"] == latest_month]["value"].sum()

st.subheader("📆 Monthly Fee Breakdown")
col5, col6 = st.columns(2)
col5.metric("Last Month Fees", f"${last_month_total:,.2f}")
col6.metric("Current Month Fees", f"${current_month_total:,.2f}")

# === CHAIN FEE DISTRIBUTION PIE CHART ===
# === CHAIN FEE DISTRIBUTION PIE CHART ===
chain_distro = filtered_df.groupby("chain", as_index=False)["value"].sum()
chain_distro["chain"] = chain_distro["chain"].fillna("unknown").astype(str)

# Normalize chain name using CHAIN_ID_MAP
def normalize_chain_name(chain):
    try:
        # First try integer key lookup (for numeric IDs)
        return CHAIN_ID_MAP.get(int(chain), chain)
    except (ValueError, TypeError):
        # Then try string key lookup
        return CHAIN_ID_MAP.get(chain, chain)

chain_distro["chain"] = chain_distro["chain"].apply(normalize_chain_name)
chain_distro = chain_distro.sort_values("value", ascending=False)

st.subheader("🌐 Fee Distribution by Chain")
col7, col8 = st.columns([1.5, 1])
with col7:
    st.plotly_chart(render_fee_distribution(chain_distro), use_container_width=True)

# === WEEKLY AVG REV PER ACTIVE USER CHART ===
weekly_df = fetch_weekly_avg_revenue_metrics()
weekly_df["week"] = pd.to_datetime(weekly_df["week"])
filtered_weekly_df = weekly_df[
    (weekly_df["week"].dt.date >= start_date) &
    (weekly_df["week"].dt.date <= end_date)
].copy()

if not filtered_weekly_df.empty and "avg_rev_per_active_user" in filtered_weekly_df.columns:
    st.subheader("📊 Weekly Avg Revenue Per Active User + 30-Day Metrics")
    col11, col12 = st.columns([2, 1])
    with col11:
        st.altair_chart(render_weekly_avg_rev(filtered_weekly_df), use_container_width=True)
    with col12:
        metrics_30d = fetch_avg_revenue_metrics()
        st.markdown("### 📈 30-Day Monetization")
        st.metric("Avg Rev / User", f"${metrics_30d['avg_rev_per_user']:.4f}", f"{metrics_30d['total_users']} users")
        st.metric("Avg Rev / Active User", f"${metrics_30d['avg_rev_per_active_user']:.4f}", f"{metrics_30d['active_users']} active")
else:
    st.info("No weekly average revenue data for the selected date range.")
