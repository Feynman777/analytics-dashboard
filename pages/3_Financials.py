import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime, timezone, timedelta
import plotly.express as px
import altair as alt

from helpers.fetch import fetch_fee_series
from helpers.upsert import upsert_fee_series
from helpers.connection import get_cache_db_connection

# === PAGE SETUP ===
st.set_page_config(page_title="Financials", layout="wide")
st.title("💸 Financials - Fee Analytics")

SYNC_FILE = "last_sync_fees.json"

def get_last_sync():
    if os.path.exists(SYNC_FILE):
        with open(SYNC_FILE, "r") as f:
            data = json.load(f)
            raw = data.get("last_sync")
            try:
                # Handles full ISO format like '2025-04-10T01:51:35.883788+00:00'
                return datetime.fromisoformat(raw)
            except Exception:
                return datetime(2024, 1, 1, tzinfo=timezone.utc)
    return datetime(2024, 1, 1, tzinfo=timezone.utc)

def update_last_sync(sync_datetime):
    with open(SYNC_FILE, "w") as f:
        json.dump({"last_sync": sync_datetime.isoformat()}, f)

# === SYNC DATA (only if more than 4 hours since last sync) ===
now = datetime.now(timezone.utc)
last_sync = get_last_sync()

if (now - last_sync) >= timedelta(hours=4):
    with st.spinner(f"Syncing fees from {last_sync.date()} to {now.date()}..."):
        df_fees = fetch_fee_series()
        if not df_fees.empty:
            df_fees["date"] = pd.to_datetime(df_fees["date"]).dt.date
            try:
                df_fees = df_fees.groupby(["date", "chain"])["value"].sum().reset_index()
                upsert_fee_series(df_fees)
                update_last_sync(now)
                st.success(f"✅ Upserted {len(df_fees)} aggregated fee rows.")
            except Exception as e:
                st.error(f"❌ Fee upsert failed: {e}")
        else:
            st.warning("No fee data found.")
else:
    next_sync = last_sync + timedelta(hours=4)
    minutes_remaining = int((next_sync - now).total_seconds() / 60)

    st.info(f"""
    ✅ Last synced at: `{last_sync.strftime('%Y-%m-%d %H:%M')} UTC`  
    ⏳ Skipping update — next sync available at: `{next_sync.strftime('%Y-%m-%d %H:%M')} UTC`  
    🕒 Approximately **{minutes_remaining} minutes** from now.
    """)


# === FETCH CACHED DATA ===
def load_fees():
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT date, chain, value FROM timeseries_fees")
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=["date", "chain", "value"])
                df["date"] = pd.to_datetime(df["date"])
                return df
    except Exception as e:
        st.error(f"Database fetch error: {e}")
        return pd.DataFrame()

df = load_fees()
if df.empty:
    st.warning("No data available yet.")
    st.stop()

# === DATE FILTER ===
st.subheader("📅 Filter by Date Range")
use_last_30 = st.checkbox("Use Last 30 Days", value=False)

if use_last_30:
    end_date = df["date"].max().date()
    start_date = end_date - timedelta(days=29)
else:
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    default_start = datetime(2025, 1, 1).date()
    start_date, end_date = st.date_input(
        "Select date range:",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date
    )

filtered_df = df[(df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)]

# === AGGREGATE CHART DATA ===
filtered_df["week"] = filtered_df["date"].dt.to_period("W").apply(lambda r: r.start_time)
filtered_df["month"] = filtered_df["date"].dt.to_period("M").apply(lambda r: r.start_time)

weekly_fees = filtered_df.groupby("week")["value"].sum().reset_index().sort_values("week")
daily_fees = filtered_df.groupby("date")["value"].sum().reset_index()

# === METRICS ===
total_fees_range = filtered_df["value"].sum()
avg_fees_per_day = filtered_df.groupby("date")["value"].sum().mean()

st.subheader("📊 Summary Metrics")
col1, col2 = st.columns(2)
col1.metric("Total Fees in Date Range", f"${total_fees_range:,.2f}")
col2.metric("Average Fees per Day", f"${avg_fees_per_day:,.2f}")

# === CHARTS: WEEKLY + DAILY SIDE BY SIDE ===
st.subheader("📊 Weekly and Daily Fees")
col3, col4 = st.columns(2)

with col3:
    weekly_fees["week_label"] = weekly_fees["week"].dt.strftime("%b %d")
    weekly_chart = alt.Chart(weekly_fees).mark_bar(size=35).encode(
        x=alt.X("week_label:N", title="Week", sort=weekly_fees["week_label"].tolist()),
        y=alt.Y("value:Q", title="Total Fees"),
        tooltip=[
            alt.Tooltip("week:T", title="Week"),
            alt.Tooltip("value:Q", title="Fees", format=".2f")
        ]
    ).properties(
        width=500,
        height=500,
        title="Weekly Fees"
    )
    st.altair_chart(weekly_chart)

with col4:
    daily_chart = alt.Chart(daily_fees).mark_bar(size=8).encode(
        x=alt.X("date:T", title="Date", axis=alt.Axis(labelAngle=-45, format="%b %d")),
        y=alt.Y("value:Q", title="Total Fees"),
        tooltip=[
            alt.Tooltip("date:T", title="Date"),
            alt.Tooltip("value:Q", title="Fees", format=".2f")
        ]
    ).properties(
        width=500,
        height=500,
        title="Daily Fees"
    )
    st.altair_chart(daily_chart)

# === MONTHLY FEE TOTALS (based on full dataset, not filtered) ===
df["month"] = df["date"].dt.to_period("M").apply(lambda r: r.start_time)
latest_month = df["month"].max()
prev_month = latest_month - pd.DateOffset(months=1)

last_month_total = df[df["month"] == prev_month]["value"].sum()
current_month_total = df[df["month"] == latest_month]["value"].sum()

st.subheader("📆 Monthly Fee Breakdown")
col5, col6 = st.columns(2)
col5.metric("Last Month Fees", f"${last_month_total:,.2f}")
col6.metric("Current Month Fees", f"${current_month_total:,.2f}")

# === PIE CHART: CHAIN FEE DISTRIBUTION ===
chain_distro = filtered_df.groupby("chain")["value"].sum().reset_index()

CHAIN_ID_MAP = {
    8453: "base", 42161: "arbitrum", 137: "polygon", 1: "ethereum",
    101: "solana", 2: "sui", 43114: "avalanche", 34443: "mode",
    56: "bnb", 10: "optimism"
}
chain_distro["chain"] = chain_distro["chain"].apply(lambda cid: CHAIN_ID_MAP.get(int(cid), str(cid)))

st.subheader("📊 Fee Distribution by Chain")
col7, col8 = st.columns([1.5, 1])
with col7:
    pie = px.pie(
        chain_distro,
        values="value",
        names="chain",
        title="Fee Distribution by Chain",
        hole=0.4
    )
    pie.update_traces(textinfo="percent+label")
    pie.update_layout(title_x=0.5)
    st.plotly_chart(pie, use_container_width=True)
