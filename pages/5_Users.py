#2_User_Data.py
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timezone
from decimal import Decimal
from io import StringIO
from helpers.connection import get_main_db_connection
from helpers.connection import get_cache_db_connection
from utils.charts import user_volume_chart, user_txn_detail_chart
from helpers.fetch import fetch_top_users_last_7d

conn = get_main_db_connection()
top_users = fetch_top_users_last_7d(conn)

def get_user_daily_volume(username):
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT created_at, amount_usd
                    FROM transactions_cache
                    WHERE type = 'SWAP' AND status = 'SUCCESS' AND from_user = %s
                """, (username,))
                rows = cursor.fetchall()

                if not rows:
                    return pd.DataFrame(), pd.DataFrame()

                df = pd.DataFrame(rows, columns=["created_at", "amount_usd"])
                df["date"] = pd.to_datetime(df["created_at"]).dt.date
                df_day = df.groupby("date")["amount_usd"].sum().reset_index()
                df_day.columns = ["date", "daily_volume_usd"]
                df_day["date"] = pd.to_datetime(df_day["date"])

                df_ts = df[["created_at", "amount_usd"]].copy()
                df_ts.columns = ["datetime", "volume_usd"]
                df_ts["datetime"] = pd.to_datetime(df_ts["datetime"])

                return df_day.sort_values("date"), df_ts.sort_values("datetime")

    except Exception as e:
        st.error(f"Error loading user volume: {e}")
        return pd.DataFrame(), pd.DataFrame()

# === Streamlit UI ===
st.title("User Data")

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
        chart = user_volume_chart(all_df_day)
        st.altair_chart(chart, use_container_width=True)

        if not df_ts_first.empty and st.toggle("Show transaction-level detail for first user"):
            st.altair_chart(user_txn_detail_chart(df_ts_first, username_list[0]), use_container_width=True)
            st.dataframe(df_ts_first.style.format({"volume_usd": "{:.2f}"}), use_container_width=True)

top_users_df = pd.DataFrame([
    {"Username": username, "Swap Volume": f"${float(volume or 0):,.2f}"}
    for username, volume in top_users
])

st.subheader("Top Users (7d)")
with st.container():
    col1, _ = st.columns([1, 1])
    with col1:
        st.dataframe(top_users_df, hide_index=True, use_container_width=True)
