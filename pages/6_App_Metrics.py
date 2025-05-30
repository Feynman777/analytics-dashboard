import streamlit as st
import pandas as pd
import altair as alt
from helpers.fetch.app_metrics import fetch_app_metrics_data

st.set_page_config(page_title="📊 App Metrics", layout="wide")
st.title("📲 App Metrics")

# === Select Date Range ===
st.subheader("Select Date Range")
col1, col2 = st.columns(2)

# Use a wide default range if you don't want to query twice
default_start = pd.to_datetime("2025-05-01").date()
default_end = pd.to_datetime("2025-05-30").date()

with col1:
    start_date = st.date_input("Start Date", value=default_start)
with col2:
    end_date = st.date_input("End Date", value=default_end)

# === Fetch Data Only Once Based on Selected Range ===
raw_data = pd.DataFrame(fetch_app_metrics_data(start=start_date, end=end_date))

if raw_data.empty:
    st.error("No app metrics data found for the selected date range.")
    st.stop()

# Clean and convert date
raw_data["date"] = pd.to_datetime(raw_data["date"]).dt.date


# === Total Downloads ===
filtered_data = raw_data.copy()
total_downloads = filtered_data[filtered_data.event_name == "first_open"]["count"].sum()
st.metric(label="📈 Total Downloads", value=f"{total_downloads:,}")

# === Chart: Daily Installs ===
st.subheader("📥 Daily Installs by OS")
col1, col2 = st.columns(2)
with col1:
    installs = filtered_data[filtered_data.event_name == "first_open"]
    chart = alt.Chart(installs).mark_bar().encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("count:Q", title="Installs"),
        color="operating_system:N",
        tooltip=["date:T", "operating_system:N", "count:Q"]
    ).properties(
        height=300,
        title="Daily Installs"
    )
    st.altair_chart(chart, use_container_width=True)

# === Chart: Removals ===
with col2:
    removals = filtered_data[filtered_data.event_name == "app_remove"]
    chart = alt.Chart(removals).mark_bar().encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("count:Q", title="Removals"),
        color="operating_system:N",
        tooltip=["date:T", "operating_system:N", "count:Q"]
    ).properties(
        height=300,
        title="Daily Removals by OS"
    )
    st.altair_chart(chart, use_container_width=True)

# === Session Starts ===
st.subheader("▶️ Session Starts and Signups")
col3, col4 = st.columns(2)

with col3:
    sessions = filtered_data[filtered_data.event_name == "session_start"]
    chart = alt.Chart(sessions).mark_bar(size=15).encode(
        x=alt.X("date:T", timeUnit="yearmonthdate", title="Date"),
        y=alt.Y("count():Q", title="Session Starts"),
        tooltip=["date:T", "count():Q"]
    ).properties(
        height=300,
        title="Session Starts"
    )
    st.altair_chart(chart, use_container_width=True)

with col4:
    signups = filtered_data[filtered_data.event_name == "user_signup"]
    chart = alt.Chart(signups).mark_bar(size=15).encode(
        x=alt.X("date:T", timeUnit="yearmonthdate", title="Date"),
        y=alt.Y("count():Q", title="User Signups"),
        tooltip=["date:T", "count():Q"]
    ).properties(
        height=300,
        title="User Signups"
    )
    st.altair_chart(chart, use_container_width=True)

# === Downloads by Country (Accurate from SQL) ===
st.subheader("🌍 Downloads by Country")

from helpers.fetch.app_metrics import fetch_country_installs

# Fetch from cache DB directly
country_counts = fetch_country_installs(start=start_date, end=end_date)

# Remove 'Unknown' from results
country_counts = country_counts[country_counts["country"] != "Unknown"]

# Optional: Rename for display
country_counts = country_counts.rename(columns={"installs": "count"})

# Display as half-width table without index and left-align count column
col_left, _ = st.columns(2)
with col_left:
    st.dataframe(
        country_counts.style.set_properties(subset=["count"], **{"text-align": "left"}),
        use_container_width=True,
        hide_index=True,
    )




