import streamlit as st
import pandas as pd
from datetime import timedelta, datetime, timezone

from helpers.fetch.weekly_data import (
    fetch_swap_series,
    fetch_timeseries_chain_volume,
    fetch_timeseries
)
from helpers.upsert.timeseries import upsert_chain_timeseries, upsert_timeseries
from helpers.fetch import fetch_api_metric
from charts.financials import metric_section

# === CONFIG ===
st.set_page_config(page_title="Weekly Data", layout="wide")
st.title("📊 Newmoney.AI Weekly Data Dashboard")

API_ENDPOINTS = {
    "cash_volume": "user/cash/volume",
    "new_users": "user/new",
    "referrals": "user/referrals",
    "total_agents": "agents/deployed"
}

available_chains = [
    "base", "arbitrum", "ethereum", "polygon", "avalanche",
    "mode", "bnb", "sui", "solana", "optimism"
]
selected_chains = st.multiselect("Select chains to include:", available_chains, default=available_chains)
exclude_current_week = st.toggle("🚫 Exclude current (incomplete) week from charts", value=True)

# === SYNC WRAPPER ===
def sync_weekly_data(last_sync, now):
    start_date = (last_sync - timedelta(hours=2)).date()
    end_date = now.date()

    raw_swaps = fetch_swap_series(start=start_date, end=end_date)
    df_swaps = pd.DataFrame(raw_swaps)
    if not df_swaps.empty:
        df_swaps["date"] = pd.to_datetime(df_swaps["date"]).dt.date
        df_swaps["metric"] = "swap_volume"
        df_swaps["status"] = "success"
        df_swaps["quantity"] = df_swaps["quantity"].astype(int)
        upsert_chain_timeseries(df_swaps)

    for metric, endpoint in API_ENDPOINTS.items():
        rows = []
        for d in pd.date_range(start=start_date, end=end_date):
            df = fetch_api_metric(endpoint, start=d.strftime("%Y-%m-%d"), end=d.strftime("%Y-%m-%d"))
            if isinstance(df, pd.DataFrame) and not df.empty:
                df["date"] = pd.to_datetime(df["date"]).dt.date
                df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)
                rows.append(df)

        if rows:
            all_df = pd.concat(rows)
            upsert_timeseries(metric, all_df)

# === WEEKLY HELPERS ===
def load_weekly_df(metric, chains=None, status="success"):
    df = fetch_timeseries_chain_volume(metric=metric, chains=chains, status=status)
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["week"] = df["date"].dt.to_period("W").apply(lambda r: r.start_time)
    return df.groupby("week")[["value", "quantity"]].sum().reset_index()

def load_weekly_api_df(metric):
    df = fetch_timeseries(metric)
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["week"] = df["date"].dt.to_period("W").apply(lambda r: r.start_time)
    return df.groupby("week")["value"].sum().reset_index()

def filter_weekly(df):
    if df.empty:
        return df
    filtered = df[(df["week"] >= start_range) & (df["week"] <= end_range)]
    if exclude_current_week:
        current_week_start = pd.Timestamp(datetime.now(timezone.utc).date()).to_period("W").start_time
        filtered = filtered[filtered["week"] < current_week_start]
    return filtered

# === DATE FILTER ===
st.subheader("📅 Filter chart date range:")
all_weeks = load_weekly_df("swap_volume", chains=selected_chains)
if all_weeks.empty:
    st.warning("No swap data available.")
    st.stop()

min_week = all_weeks["week"].min().date()
max_week = all_weeks["week"].max().date()
default_start = max(datetime(2025, 1, 1).date(), min_week)

date_range = st.date_input("Select start and end:", (default_start, max_week), min_value=min_week, max_value=max_week)
user_selected_start = pd.to_datetime(date_range[0]).date()
start_range = pd.to_datetime(max(user_selected_start, min_week))
end_range = pd.to_datetime(date_range[1])

# === LOAD + RENDER CHARTS ===
swap_df = filter_weekly(load_weekly_df("swap_volume", chains=selected_chains))
cash_df = filter_weekly(load_weekly_api_df("cash_volume"))
users_df = filter_weekly(load_weekly_api_df("new_users"))
refs_df = filter_weekly(load_weekly_api_df("referrals"))
agents_df = filter_weekly(load_weekly_api_df("total_agents"))

col1, col2 = st.columns(2)
with col1:
    st.altair_chart(metric_section(swap_df, "Weekly Swap Volume (USD)", "USD", col="value"), use_container_width=True)
with col2:
    st.altair_chart(metric_section(swap_df, "Weekly Swap Quantity", "# of TXNs", col="quantity"), use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    st.altair_chart(metric_section(cash_df, "Weekly Cash Volume (USD)", "USD"), use_container_width=True)
with col4:
    st.altair_chart(metric_section(users_df, "Weekly New Users", "Users"), use_container_width=True)

col5, col6 = st.columns(2)
with col5:
    st.altair_chart(metric_section(refs_df, "Weekly Referrals", "Referrals"), use_container_width=True)
with col6:
    st.altair_chart(metric_section(agents_df, "Weekly Agent Deployments", "Agents"), use_container_width=True)