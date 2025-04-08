import streamlit as st
import altair as alt
import pandas as pd
from datetime import datetime, timezone, timedelta
import os
import json

from helpers.database import fetch_swap_series
from helpers.chain_upsert import upsert_chain_timeseries
from helpers.cache_db import fetch_timeseries_chain_volume, fetch_timeseries

# === PAGE CONFIG ===
st.set_page_config(page_title="Weekly Data", layout="wide")
st.title("📊 Newmoney.AI Weekly Data Dashboard")

# === SYNC TRACKING ===
SYNC_FILE = "last_sync_swap_volume.json"
DEFAULT_START_DATE = datetime(2015, 1, 1).date()

def get_last_sync():
    if os.path.exists(SYNC_FILE):
        with open(SYNC_FILE, "r") as f:
            data = json.load(f)
            return datetime.strptime(data["last_sync"], "%Y-%m-%d").date()
    return datetime.strptime("2024-01-01", "%Y-%m-%d").date()

def update_last_sync(sync_date):
    with open(SYNC_FILE, "w") as f:
        json.dump({"last_sync": sync_date.strftime("%Y-%m-%d")}, f)
    print(f"[DEBUG] Updated last sync to {sync_date}")

# === CHAIN SELECTION ===
available_chains = ["base", "arbitrum", "ethereum", "polygon", "avalanche", "mode", "bnb", "sui", "solana", "optimism"]
selected_chains = st.multiselect("Select chains to include:", available_chains, default=available_chains)

# === TOGGLE TO EXCLUDE CURRENT WEEK ===
exclude_current_week = st.toggle("🚫 Exclude current (incomplete) week from charts", value=True)

# === SYNC NEW SWAP DATA IF AVAILABLE ===
today = datetime.now(timezone.utc).date()
last_sync = get_last_sync()
start_date = last_sync + timedelta(days=1)

if start_date <= today:
    with st.spinner(f"🔄 Syncing new swap volume from {start_date} to {today}..."):
        df_swaps = fetch_swap_series()
        if isinstance(df_swaps, list) and df_swaps:
            df_swaps = pd.DataFrame(df_swaps)
            df_swaps["date"] = pd.to_datetime(df_swaps["date"]).dt.date
            df_swaps["metric"] = "swap_volume"
            df_swaps["status"] = "success"
            df_swaps["quantity"] = df_swaps["quantity"].astype(int)
            try:
                upsert_chain_timeseries(df_swaps)
                update_last_sync(today)
                st.success(f"✅ Upserted {len(df_swaps)} swap rows.")
            except Exception as e:
                st.error(f"❌ Upsert failed: {e}")
        else:
            st.warning("⚠️ No swap data found to sync.")
else:
    st.info("✅ Fully synced!")

# === LOAD METRICS FROM CACHE ===

def load_weekly_df(metric, chains=None, status="success"):
    """
    Load and aggregate chain-based metrics (e.g., swap volume) by week.
    """
    df = fetch_timeseries_chain_volume(metric=metric, chains=chains, status=status)
    if df.empty:
        return pd.DataFrame()

    # Ensure date is timezone-naive before converting to weekly periods
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["week"] = df["date"].dt.to_period("W").apply(lambda r: r.start_time)

    # Group by week and sum both value and quantity
    return df.groupby("week")[["value", "quantity"]].sum().reset_index()


def load_weekly_api_df(metric):
    """
    Load and aggregate API-based metrics (e.g., cash volume) by week.
    """
    df = fetch_timeseries(metric)
    if df.empty:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["week"] = df["date"].dt.to_period("W").apply(lambda r: r.start_time)

    # Group by week and sum values (no quantity column in API metrics)
    return df.groupby("week")["value"].sum().reset_index()

# === FILTER RANGE UI ===
st.subheader("📅 Filter chart date range:")
all_weeks = load_weekly_df("swap_volume", chains=selected_chains)
if all_weeks.empty:
    st.warning("No swap data available.")
    st.stop()

# === Filter Range UI (Updated Logic) ===
min_week = all_weeks["week"].min().date()
max_week = all_weeks["week"].max().date()
hardcoded_default = datetime(2025, 1, 1).date()

# Use the later of hardcoded default and min_week
default_start = max(hardcoded_default, min_week)

# UI
date_range = st.date_input(
    "Select start and end:",
    (default_start, max_week),
    min_value=min_week,
    max_value=max_week
)

# Apply logic after user selection
user_selected_start = pd.to_datetime(date_range[0]).date()
start_range = pd.to_datetime(max(user_selected_start, min_week))
end_range = pd.to_datetime(date_range[1])

# === CHART + STATS RENDERING ===
def week_over_week_change(df, col="value"):
    if len(df) < 2:
        return None
    prev = df.iloc[-2][col]
    curr = df.iloc[-1][col]
    if prev == 0:
        return None
    return round(((curr - prev) / prev) * 100, 2)

def render_badge(change):
    if change is None:
        return ""
    color = "#28a745" if change > 0 else ("#dc3545" if change < 0 else "#6c757d")
    arrow = "▲" if change > 0 else ("▼" if change < 0 else "→")
    return f"""
        <div style="margin-top:4px;">
            <span style="
                background-color:{color};
                color:white;
                padding:5px 10px;
                border-radius:5px;
                font-size:0.9rem;
                font-weight:600;
            ">
                {arrow} {change:+.2f}%
            </span>
        </div>
    """

def metric_section(df, title, label, col="value", unit=""):
    if df.empty:
        st.warning(f"No data for {title}")
        return None
    df["week_str"] = df["week"].astype(str)
    total = df[col].sum()
    min_val = df[col].min()
    max_val = df[col].max()
    change = week_over_week_change(df, col)
    badge = render_badge(change)
    st.markdown(f"""
        <div style="font-size:1.1rem; font-weight:bold; margin-top:20px;">{title}</div>
        <div style="margin-bottom:6px;">
            <span>Total: <code>{total:,.2f}</code> | Min: <code>{min_val:,.2f}</code> | Max: <code>{max_val:,.2f}</code></span>
        </div>
        {badge}
    """, unsafe_allow_html=True)
    return alt.Chart(df).mark_bar().encode(
        x=alt.X("week_str:O", title="Week"),
        y=alt.Y(f"{col}:Q", title=label),
        tooltip=[alt.Tooltip("week_str:N", title="Week"), alt.Tooltip(f"{col}:Q", title=label, format=",.2f")]
    ).properties(height=300)

# === APPLY RANGE + TOGGLE FILTERING ===
def filter_weekly(df):
    if df.empty:
        return df
    filtered = df[(df["week"] >= start_range) & (df["week"] <= end_range)]
    if exclude_current_week:
        current_week_start = pd.to_datetime(datetime.now(timezone.utc)).to_period("W").start_time
        filtered = filtered[filtered["week"] < current_week_start]
    return filtered

# === LOAD & RENDER CHARTS ===
swap_df = filter_weekly(load_weekly_df("swap_volume", chains=selected_chains))
cash_df = filter_weekly(load_weekly_api_df("cash_volume"))
users_df = filter_weekly(load_weekly_api_df("new_users"))
refs_df = filter_weekly(load_weekly_api_df("referrals"))
agents_df = filter_weekly(load_weekly_api_df("total_agents"))

col1, col2 = st.columns(2)
with col1:
    chart = metric_section(swap_df, "Weekly Swap Volume (USD)", "USD", col="value")
    if chart:
        st.altair_chart(chart, use_container_width=True)
with col2:
    chart = metric_section(swap_df, "Weekly Swap Quantity", "# of TXNs", col="quantity")
    if chart:
        st.altair_chart(chart, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    chart = metric_section(cash_df, "Weekly Cash Volume (USD)", "USD")
    if chart:
        st.altair_chart(chart, use_container_width=True)
with col4:
    chart = metric_section(users_df, "Weekly New Users", "Users")
    if chart:
        st.altair_chart(chart, use_container_width=True)

col5, col6 = st.columns(2)
with col5:
    chart = metric_section(refs_df, "Weekly Referrals", "Referrals")
    if chart:
        st.altair_chart(chart, use_container_width=True)
with col6:
    chart = metric_section(agents_df, "Weekly Agent Deployments", "Agents")
    if chart:
        st.altair_chart(chart, use_container_width=True)
