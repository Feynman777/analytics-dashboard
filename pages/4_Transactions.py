import json
import os
import streamlit as st
from datetime import datetime, timedelta, timezone
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode
import pandas as pd

from helpers.fetch import (
    fetch_transactions_filtered,
    fetch_user_metrics_full,
    fetch_user_profile_summary
)
from helpers.connection import get_main_db_connection
from helpers.sync_utils import get_last_sync

SECTION_KEY = "Transactions"

st.set_page_config(page_title="🔁 Transactions", layout="wide")
st.title("🔁 Transactions")

def load_fresh_last_sync(section):
    sync_path = os.path.join(os.getcwd(), "last_sync.json")
    if os.path.exists(sync_path):
        with open(sync_path, "r") as f:
            data = json.load(f)
            if section in data:
                return datetime.fromisoformat(data[section])
    return None

# === Initialize state ===
if "search_filter" not in st.session_state:
    st.session_state.search_filter = ""

# === User Stats Input ===
st.markdown("### 🧑‍💻 User Stats Input")
stats_col1, stats_col2 = st.columns([2.5, 2.5])
with stats_col1:
    user_input = st.text_input("🔍 Search User (by Username, Email, or Wallet)", key="user_stats_input")
with stats_col2:
    start_date = st.date_input("📅 Start Date Filter", value=datetime(2025, 1, 1).date())

# === Load User Stats ===
if st.button("📊 Load User Stats"):
    with get_main_db_connection() as conn:
        profile = fetch_user_profile_summary(conn, user_input)

    if profile:
        resolved_identifier = profile.get("username") or profile.get("email")
        if not resolved_identifier:
            st.warning("⚠️ User found, but no valid username or email to query.")
        else:
            metrics = fetch_user_metrics_full(
                resolved_identifier,
                start=start_date.isoformat(),
                end=(datetime.today() + timedelta(days=1)).date().isoformat()
            )
            st.session_state.user_profile = profile
            st.session_state.user_stats = metrics
    else:
        st.warning("⚠️ No user found for that input.")

# === Display User Stats (if loaded) ===
if "user_profile" in st.session_state and "user_stats" in st.session_state:
    profile = st.session_state.user_profile
    user = st.session_state.user_stats

    if profile and user:
        st.subheader("📈 User Stats")
        username = profile.get("username", "N/A")
        email = profile.get("email", "N/A")
        created_at = profile.get("createdAt", "N/A")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("#### ℹ️ General Info")
            st.write("**👤 Username:**", username)
            st.write("**📧 Email:**", email)
            st.write("**🗓️ Joined:**", created_at)
            st.write("**🪙 EVM Address:**", user["profile"].get("evm", "N/A"))
            st.write("**🌞 Solana Address:**", user["profile"].get("solana", "N/A"))
            st.write("**₿ Bitcoin Address:**", user["profile"].get("btc", "N/A"))
            st.write("**🌊 Sui Address:**", user["profile"].get("sui", "N/A"))
            st.write("**💰 Crypto Balance (USD):**", f"${user['crypto'].get('totalBalanceUSD', 0):,.2f}")
            balance = user.get("cash", {}).get("balance") or 0
            st.write("**🏦 Cash Balance (USD):**", f"${float(balance):,.2f}")

        with col2:
            st.markdown("#### 📆 Lifetime Stats")
            st.write("**🔁 Swap Volume:**", f"${user['lifetime'].get('volume', {}).get('volume', 0):,.2f}")
            st.write("**🎯 Referrals:**", user["lifetime"].get("referrals", 0))

        with col3:
            st.markdown("#### ⏳ Date-Filtered Stats")
            st.write("**🔁 Swap Volume:**", f"${user['filtered'].get('volume', {}).get('volume', 0):,.2f}")
            st.write("**🎯 Referrals:**", user["filtered"].get("referrals", 0))

# === Show Last Sync Timestamp Only ===
SECTION_KEY = "Transactions"  # or "Daily_Stats", etc.
last_sync = get_last_sync(SECTION_KEY)

if last_sync:
    st.info(f"✅ Last synced at: `{last_sync.strftime('%Y-%m-%d %H:%M')} UTC`")
else:
    st.warning("⚠️ Last sync time not available.")

# === Table Filter Inputs ===
st.markdown("### 🔎 Filter Transactions Table")
tf1, tf2 = st.columns([3, 2])
with tf1:
    search_input = st.text_input("🔍 Filter by Username, Email, or Wallet", value=st.session_state.search_filter, key="txn_filter_input")
with tf2:
    selected_chains = st.multiselect("🪙 Chain Filter", [
        "base", "arbitrum", "ethereum", "polygon", "avalanche",
        "mode", "bnb", "sui", "solana", "optimism"
    ])

if st.button("🧹 Apply Table Filter"):
    st.session_state.search_filter = search_input
    st.rerun()

# === Fetch Filtered Transactions ===
txn_data = fetch_transactions_filtered(
    search_user_or_email=st.session_state.search_filter or None,
    since_date=start_date.isoformat(),
    from_chain=None,
    to_chain=None,
    limit=1000,
)

columns = [
    "Date", "Type", "Status", "From User", "To User", "From Token",
    "From Chain", "To Token", "To Chain", "Amount USD", "Tx Hash", "Tx Display"
]

df = pd.DataFrame(txn_data, columns=columns) if txn_data and len(txn_data[0]) == len(columns) else pd.DataFrame(txn_data)
if "Tx Display" not in df.columns:
    df["Tx Display"] = ""
else:
    df["Tx Display"] = df["Tx Display"].fillna("")

# === Display Transactions Table ===
st.subheader("📋 Transactions Table")
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_pagination(paginationAutoPageSize=True)
gb.configure_default_column(filter=True, sortable=True, resizable=True)
gb.configure_side_bar()

gb.configure_column("Type", filter="agSetColumnFilter")
gb.configure_column("Status", filter="agSetColumnFilter")
gb.configure_column("From User", filter="agTextColumnFilter")
gb.configure_column("To User", filter="agTextColumnFilter")
gb.configure_column("From Token", filter="agTextColumnFilter")
gb.configure_column("From Chain", filter="agSetColumnFilter")
gb.configure_column("To Token", filter="agTextColumnFilter")
gb.configure_column("To Chain", filter="agSetColumnFilter")
gb.configure_column("Amount USD", filter="agNumberColumnFilter")
gb.configure_selection(selection_mode="single", use_checkbox=True)
grid_options = gb.build()

AgGrid(
    df,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.MODEL_CHANGED,
    theme="balham",
    height=600,
    fit_columns_on_grid_load=True,
    enable_enterprise_modules=True,
    allow_unsafe_jscode=True
)

# === Total Amount USD Summary ===
if not df.empty and "Amount USD" in df.columns:
    total_usd = df["Amount USD"].sum()
    st.markdown(f"### 💸 Total Amount USD: **${total_usd:,.2f}**")

# === Summary Table ===
st.markdown("### 📊 Summary of Filtered Transactions")
if not df.empty:
    grouped = (
        df.groupby(["Type", "Status"])
          .agg(Quantity=("Amount USD", "count"), Amount=("Amount USD", "sum"))
          .reset_index()
    )

    pivot_qty = grouped.pivot(index="Type", columns="Status", values="Quantity").fillna(0).astype(int)
    pivot_amt = grouped.pivot(index="Type", columns="Status", values="Amount").fillna(0.0)

    summary = pd.DataFrame(index=pivot_qty.index)
    for status in sorted(set(grouped["Status"])):
        summary[f"{status} Qty"] = pivot_qty.get(status, 0)
        summary[f"{status} Amt"] = pivot_amt.get(status, 0.0)

    summary["Total Qty"] = summary.filter(like="Qty").sum(axis=1)
    amt_cols = [col for col in summary.columns if "Amt" in col]
    for col in amt_cols:
        summary[col] = summary[col].astype(float)
    summary["Total Amt"] = summary[amt_cols].sum(axis=1).round(2)

    total_row = summary.sum(numeric_only=True).to_frame().T
    total_row.index = ["TOTAL"]
    summary = pd.concat([summary, total_row])

    st.dataframe(summary.style.format({col: "${:,.2f}" for col in summary.columns if "Amt" in col}))
else:
    st.info("ℹ️ No transactions to summarize.")
