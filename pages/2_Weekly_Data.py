import streamlit as st
import pandas as pd
from datetime import datetime, timezone

from helpers.connection import get_cache_db_connection
from helpers.utils.charts import metric_section

st.set_page_config(page_title="Weekly Data", layout="wide")
st.title("📊 Weekly Stats")

# === CONFIG ===
AVAILABLE_METRICS = [
    "swap_volume", "cash_volume", "new_users",
    "referrals", "total_agents", "active_users", "new_active_users"
]

METRIC_LABELS = {
    "swap_volume": "Swap Volume (USD)",
    "cash_volume": "Cash Volume (USD)",
    "new_users": "New Users",
    "referrals": "Referrals",
    "total_agents": "Agent Deployments",
    "active_users": "Active Users",
    "new_active_users": "New Active Users"
}

VALUE_SUFFIX = {
    "swap_volume": "USD",
    "cash_volume": "USD",
    "new_users": "Users",
    "referrals": "Referrals",
    "total_agents": "Agents",
    "active_users": "Users",
    "new_active_users": "Users"
}

# === Load weekly_stats table ===
with get_cache_db_connection() as conn:
    df = pd.read_sql("SELECT * FROM weekly_stats", conn)

if df.empty:
    st.warning("No data found in `weekly_stats`.")
    st.stop()

df["week_start_date"] = pd.to_datetime(df["week_start_date"])
df = df[df["metric"].isin(AVAILABLE_METRICS)]

# === Filter controls ===
exclude_current_week = st.toggle("🚫 Exclude current (incomplete) week", value=True)

min_week = df["week_start_date"].min().date()
max_week = datetime.now().date()
default_start = max(min_week, datetime(2025, 1, 1).date())

date_range = st.date_input("📅 Select date range:", (default_start, max_week), min_value=min_week, max_value=max_week)
start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])

if exclude_current_week:
    current_week = pd.Timestamp(datetime.now(timezone.utc).date()).to_period("W").start_time
    df = df[df["week_start_date"] < current_week]

df = df[(df["week_start_date"] >= start_date) & (df["week_start_date"] <= end_date)]

# === Show charts ===
chart_metrics = [
    ("swap_volume", "quantity"),
    ("cash_volume", "value"),
    ("new_users", "value"),
    ("referrals", "value"),
    ("total_agents", "value"),
    ("active_users", "value"),
    ("new_active_users", "value")
]

# First: swap volume + quantity
swap_df = df[df["metric"] == "swap_volume"].sort_values("week_start_date")
if not swap_df.empty:
    swap_df = swap_df.rename(columns={"week_start_date": "week"})
    col1, col2 = st.columns(2)
    with col1:
        st.altair_chart(
            metric_section(swap_df, "Weekly Swap Volume (USD)", "USD", col="value"),
            use_container_width=True
        )
    with col2:
        st.altair_chart(
            metric_section(swap_df, "Weekly Swap Quantity", "# of TXNs", col="quantity"),
            use_container_width=True
        )

# Other charts (2 per row)
non_swap_metrics = [m for m in AVAILABLE_METRICS if m != "swap_volume"]
for i in range(0, len(non_swap_metrics), 2):
    col1, col2 = st.columns(2)
    for col, metric in zip([col1, col2], non_swap_metrics[i:i+2]):
        metric_df = df[df["metric"] == metric].sort_values("week_start_date")
        if not metric_df.empty:
            metric_df = metric_df.rename(columns={"week_start_date": "week"})
            with col:
                st.altair_chart(
                    metric_section(metric_df, f"Weekly {METRIC_LABELS[metric]}", VALUE_SUFFIX[metric], col="value"),
                    use_container_width=True
                )
