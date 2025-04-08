#pages/2_User_Data.py
import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timezone
from decimal import Decimal
from io import StringIO
import altair as alt

DB_HOST = st.secrets["database"]["DB_HOST"]
DB_PORT = st.secrets["database"]["DB_PORT"]
DB_NAME = st.secrets["database"]["DB_NAME"]
DB_USER = st.secrets["database"]["DB_USER"]
DB_PASS = st.secrets["database"]["DB_PASS"]

def get_user_daily_volume(username):
    try:
        with psycopg2.connect(
            host=DB_HOST,
            port=int(DB_PORT),  # Ensure integer
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT A."createdAt", A.transaction
                    FROM public."Activity" A
                    JOIN public."User" U ON A."userId" = U."userId"
                    WHERE A.status = 'SUCCESS' AND A.type = 'SWAP' AND U."username" = %s
                """, (username,))
                rows = cursor.fetchall()
                volume_by_day = {}
                volume_by_timestamp = []

                for created_at, txn_raw in rows:
                    try:
                        txn = pd.read_json(StringIO(txn_raw), typ='series')
                        from_amount = Decimal(txn.get("fromAmount", 0))
                        decimals = int(txn.get("fromToken", {}).get("decimals", 18))
                        price_usd = Decimal(txn.get("fromToken", {}).get("tokenPrices", {}).get("usd", 0))

                        if from_amount and price_usd:
                            normalized = from_amount / Decimal(10 ** decimals)
                            volume = normalized * price_usd
                            volume = round(float(volume), 2)

                            date_only = created_at.date().isoformat()
                            volume_by_day[date_only] = float(volume_by_day.get(date_only, 0)) + volume

                            volume_by_timestamp.append({
                                "datetime": created_at.strftime("%Y-%m-%d %H:%M:%S"),
                                "volume_usd": float(volume)
                            })
                    except Exception as e:
                        st.warning(f"Skipping txn for {username} on {created_at}: {e}")

                df_day = pd.DataFrame(list(volume_by_day.items()), columns=["date", "daily_volume_usd"])
                df_day["date"] = pd.to_datetime(df_day["date"])
                df_day = df_day.sort_values("date")

                df_ts = pd.DataFrame(volume_by_timestamp)
                if not df_ts.empty:
                    df_ts["datetime"] = pd.to_datetime(df_ts["datetime"])
                    df_ts = df_ts.sort_values("datetime")

                return df_day, df_ts
    except Exception as e:
        st.error(f"Database error for {username}: {e}")
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
            (all_df_day["date"].dt.date >= start_date) &
            (all_df_day["date"].dt.date <= end_date)
        ]

        all_df_day["date"] = pd.to_datetime(all_df_day["date"])
        all_df_day["daily_volume_usd"] = all_df_day["daily_volume_usd"].astype(float)

        daily_chart = alt.Chart(all_df_day).mark_line(point=True).encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("daily_volume_usd:Q", title="Volume (USD)", scale=alt.Scale(zero=False)),
            color=alt.Color("username:N", title="User"),
            tooltip=["date:T", "username:N", alt.Tooltip("daily_volume_usd:Q", format=".2f")]
        ).properties(
            width=700,
            height=400,
            title="\ud83d\udcc8 Daily Volume (USD)"
        ).interactive()

        st.altair_chart(daily_chart, use_container_width=True)

        if not df_ts_first.empty:
            if st.toggle("Show transaction-level detail for first user"):
                df_ts_first["datetime"] = pd.to_datetime(df_ts_first["datetime"])
                df_ts_first["volume_usd"] = df_ts_first["volume_usd"].astype(float)

                detail_chart = alt.Chart(df_ts_first).mark_line(point=True).encode(
                    x=alt.X("datetime:T", title="Date & Time"),
                    y=alt.Y("volume_usd:Q", title="Volume (USD)", scale=alt.Scale(zero=False)),
                    tooltip=[alt.Tooltip("datetime:T"), alt.Tooltip("volume_usd:Q", format=".2f")]
                ).properties(
                    width=700,
                    height=300,
                    title=f"\ud83d\udcc8 Daily Volume (USD) for {username_list[0]}"
                ).interactive()

                st.altair_chart(detail_chart, use_container_width=True)
                st.dataframe(df_ts_first.style.format({"volume_usd": "{:.2f}"}), use_container_width=True)