import streamlit as st
import pandas as pd
from datetime import datetime, timezone
from helpers.connection import get_main_db_connection
from helpers.fetch.user import fetch_top_users_last_7d, get_user_daily_volume
from charts.financials import user_volume_chart, user_txn_detail_chart

st.set_page_config(page_title="User Data", layout="wide")
st.title("👥 User Data Dashboard")

# === Top Users Table ===
conn = get_main_db_connection()
top_users = fetch_top_users_last_7d(conn)

top_users_df = pd.DataFrame([
    {"Username": username, "Swap Volume": f"${float(volume or 0):,.2f}"}
    for username, volume in top_users
])

st.subheader("🏅 Top Users (7d)")
st.dataframe(top_users_df, hide_index=True, use_container_width=True)

# === Custom User Input ===
st.subheader("🔍 Custom User Lookup")
usernames = st.text_input("Enter up to 5 usernames (comma separated):")

if usernames:
    username_list = [u.strip() for u in usernames.split(",")][:5]
    all_df_day = pd.DataFrame()
    df_ts_first = pd.DataFrame()
    min_transaction_date = None

    with st.spinner("Fetching data for all users..."):
        for i, uname in enumerate(username_list):
            df_day, df_ts = get_user_daily_volume(uname)
            if not df_day.empty:
                first_date = df_day["date"].min().date()
                if min_transaction_date is None or first_date < min_transaction_date:
                    min_transaction_date = first_date
                df_day["username"] = uname
                all_df_day = pd.concat([all_df_day, df_day], ignore_index=True)
            if i == 0:
                df_ts_first = df_ts

    if all_df_day.empty:
        st.warning("No data found for provided usernames.")
    else:
        min_date = min_transaction_date
        max_date = datetime.now(timezone.utc).date()
        date_range = st.date_input("Filter date range:", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        start_date, end_date = pd.to_datetime(date_range[0]).date(), pd.to_datetime(date_range[1]).date()

        all_df_day = all_df_day[
            (all_df_day["date"].dt.date >= start_date) & (all_df_day["date"].dt.date <= end_date)
        ]

        st.altair_chart(user_volume_chart(all_df_day), use_container_width=True)

        if not df_ts_first.empty and st.toggle("Show transaction-level detail for first user"):
            st.altair_chart(user_txn_detail_chart(df_ts_first, username_list[0]), use_container_width=True)
            st.dataframe(df_ts_first.style.format({"volume_usd": "{:.2f}"}), use_container_width=True)
