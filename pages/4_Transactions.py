import streamlit as st
from datetime import datetime, timedelta, timezone
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode
import pandas as pd
import time
import json
import os
from helpers.fetch import (
    fetch_transactions_filtered,
    fetch_user_profile_summary,
    fetch_user_metrics_full,
)
from helpers.connection import get_main_db_connection
from helpers.sync_utils import sync_transaction_cache
from helpers.sync_utils import get_last_sync, update_last_sync

SYNC_FILE = "last_sync.json"
SECTION_KEY = "Transactions"

st.set_page_config(page_title="Transactions", layout="wide")
st.title("🔄 Transactions")

if "is_syncing" in st.session_state and not st.session_state.is_syncing:
    del st.session_state["is_syncing"]

# Reset sync flag if stuck too long
if "sync_timestamp" in st.session_state:
    if datetime.now(timezone.utc) - st.session_state.sync_timestamp > timedelta(minutes=2):
        st.session_state.is_syncing = False
        del st.session_state.sync_timestamp

# === Initialize state ===
if "search_filter" not in st.session_state:
    st.session_state.search_filter = ""

# === User Stats Filters ===
st.markdown("### 📥 User Stats Input")
stats_col1, stats_col2 = st.columns([2.5, 2.5])
with stats_col1:
    user_input = st.text_input("Search User (for stats) by Username, Email, or Wallet", key="user_stats_input")
with stats_col2:
    start_date = st.date_input("Start Date Filter", value=datetime(2025, 1, 1).date())

# === Load User Stats ===
if st.button("Load User Stats"):
    with get_main_db_connection() as conn:
        profile = fetch_user_profile_summary(conn, user_input)

    if profile:
        resolved_identifier = profile.get("username") or profile.get("email")
        if not resolved_identifier:
            st.warning("User found, but no valid username or email to query.")
        else:
            metrics = fetch_user_metrics_full(
                resolved_identifier,
                start=start_date.isoformat(),
                end=(datetime.today() + timedelta(days=1)).date().isoformat()
            )
            st.session_state.user_profile = profile
            st.session_state.user_stats = metrics
    else:
        st.warning("No user found for that input.")

# === Display User Stats (only when loaded) ===
if "user_profile" in st.session_state and "user_stats" in st.session_state:
    profile = st.session_state.user_profile
    user = st.session_state.user_stats

    if profile and user:
        st.subheader("📊 User Stats")
        username = profile.get("username", "N/A")
        email = profile.get("email", "N/A")
        created_at = profile.get("createdAt", "N/A")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("#### General Info")
            st.write("**Username:**", username)
            st.write("**Email:**", email)
            st.write("**Joined:**", created_at)
            st.write("**EVM Address:**", user["profile"].get("evm", "N/A"))
            st.write("**Solana Address:**", user["profile"].get("solana", "N/A"))
            st.write("**Bitcoin Address:**", user["profile"].get("btc", "N/A"))
            st.write("**Sui Address:**", user["profile"].get("sui", "N/A"))
            st.write("**Crypto Balance (USD):**", f"${user['crypto'].get('totalBalanceUSD', 0):,.2f}")
            st.write("**Cash Balance (USD):**", f"${float(user['cash'].get('balance', 0)):,.2f}")

        with col2:
            st.markdown("#### Lifetime Stats")
            st.write("**Swap Volume:**", f"${user['lifetime'].get('volume', {}).get('volume', 0):,.2f}")
            st.write("**Referrals:**", user["lifetime"].get("referrals", 0))

        with col3:
            st.markdown("#### Date-Filtered Stats")
            st.write("**Swap Volume:**", f"${user['filtered'].get('volume', {}).get('volume', 0):,.2f}")
            st.write("**Referrals:**", user["filtered"].get("referrals", 0))

        # === Sync Display and Button below user stats ===
        now = datetime.now(timezone.utc)
        last_sync = get_last_sync(SECTION_KEY)

        if "is_syncing" not in st.session_state:
            st.session_state.is_syncing = False

        force = st.button("🔁 Force Sync Transactions Table", key="force_sync_button")

        print("⏱️ Checking sync logic...")
        print("Force pressed:", force, ", is_syncing:", st.session_state.is_syncing)
        print("Now:", now, ", Last sync:", last_sync)

        should_sync = (force or (now - last_sync >= timedelta(hours=1))) and not st.session_state.is_syncing

        if should_sync:
            st.session_state.is_syncing = True
            st.session_state.sync_timestamp = datetime.now(timezone.utc)
            sync_success = False

            with st.spinner("Syncing Transactions Cache..."):
                try:
                    sync_transaction_cache(force=True)
                    st.success("✅ Transactions cache synced!")
                    print("✅ Sync complete at:", now)
                    sync_success = True
                except Exception as e:
                    st.error(f"❌ Sync failed: {e}")
                finally:
                    st.session_state.is_syncing = False

            if sync_success:
                st.rerun()
        else:
            next_sync = last_sync + timedelta(hours=1)
            minutes_remaining = max(0, int((next_sync - now).total_seconds() / 60))
            st.info(f"""
            ✅ Last synced at: `{last_sync.strftime('%Y-%m-%d %H:%M')} UTC`  
            ⏳ Next sync in approximately **{minutes_remaining} minutes**.
            """)

# === Table Filter Inputs ===
st.markdown("### 🔍 Filter Transactions Table")
tf1, tf2 = st.columns([3, 2])
with tf1:
    search_input = st.text_input("Filter by Username, Email, or Wallet", value=st.session_state.search_filter, key="txn_filter_input")
with tf2:
    selected_chains = st.multiselect("Chain Filter", ["base", "arbitrum", "ethereum", "polygon", "avalanche", "mode", "bnb", "sui", "solana", "optimism"])

if st.button("Apply Table Filter"):
    st.session_state.search_filter = search_input
    st.rerun()

# === Fetch Filtered Transactions ===
chain_filters = selected_chains if selected_chains else None
txn_data = fetch_transactions_filtered(
    search_user_or_email=st.session_state.search_filter or None,
    since_date=start_date.isoformat(),
    from_chain=None,
    to_chain=None,
    limit=1000,
)

df = pd.DataFrame(txn_data, columns=[
    "Date", "Type", "Status", "From User", "To User", "From Token",
    "From Chain", "To Token", "To Chain", "Amount USD", "Tx Hash"
])

# === Display Transactions Table ===
st.subheader("📋 Transactions Table")
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_pagination(paginationAutoPageSize=True)
gb.configure_default_column(filter=True, sortable=True, resizable=True)
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
    fit_columns_on_grid_load=True
)

# === Total Amount USD Summary ===
if not df.empty and "Amount USD" in df.columns:
    total_usd = df["Amount USD"].sum()
    st.markdown(f"### 💰 Total Amount USD: **${total_usd:,.2f}**")

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
    st.info("No transactions to summarize.")
