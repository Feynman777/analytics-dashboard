import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode

from helpers.connection import get_main_db_connection
from helpers.fetch.transactions import fetch_transactions_filtered, fetch_user_profile_summary, fetch_user_metrics_full
from helpers.sync import get_last_sync

SECTION_KEY = "Transactions"

st.set_page_config(page_title="Transactions", layout="wide")
st.title("🔁 Transactions")

# === User Stats Input ===
st.markdown("### 🧑‍💻 User Stats Input")
user_col, date_col = st.columns([2, 2])
with user_col:
    user_input = st.text_input("🔍 Search User (by Username, Email, or Wallet)", key="user_stats_input")
with date_col:
    start_date = st.date_input("📅 Start Date Filter", value=datetime(2025, 1, 1).date())

# === Load User Stats ===
if st.button("📊 Load User Stats"):
    with get_main_db_connection() as conn:
        profile = fetch_user_profile_summary(conn, user_input)

    if profile:
        resolved_identifier = profile.get("username") or profile.get("email")
        if resolved_identifier:
            metrics = fetch_user_metrics_full(
                resolved_identifier,
                start=start_date.isoformat(),
                end=(datetime.today() + timedelta(days=1)).date().isoformat()
            )
            st.session_state.user_profile = profile
            st.session_state.user_stats = metrics
        else:
            st.warning("⚠️ User found, but no valid username or email to query.")
    else:
        st.warning("⚠️ No user found for that input.")

# === Display User Stats ===
if "user_profile" in st.session_state and "user_stats" in st.session_state:
    profile = st.session_state.user_profile
    stats = st.session_state.user_stats

    st.subheader("📈 User Stats")
    info_col1, info_col2, info_col3 = st.columns(3)

    with info_col1:
        st.markdown("#### ℹ️ General Info")
        st.write("**👤 Username:**", profile.get("username", "N/A"))
        st.write("**📧 Email:**", profile.get("email", "N/A"))
        st.write("**🗓️ Joined:**", profile.get("createdAt", "N/A"))
        st.write("**🪙 EVM Address:**", stats["profile"].get("evm", "N/A"))
        st.write("**🌞 Solana Address:**", stats["profile"].get("solana", "N/A"))
        st.write("**₿ Bitcoin Address:**", stats["profile"].get("btc", "N/A"))
        st.write("**🌊 Sui Address:**", stats["profile"].get("sui", "N/A"))
        st.write("**🏦 Cash Balance (USD):**", f"${float(stats.get('cash', {}).get('balance', 0)):.2f}")

    with info_col2:
        st.markdown("#### 📆 Lifetime Stats")
        st.write("**🔁 Swap Volume:**", f"${stats['lifetime'].get('volume', {}).get('volume', 0):,.2f}")
        st.write("**🎯 Referrals:**", stats["lifetime"].get("referrals", 0))

    with info_col3:
        st.markdown("#### ⏳ Date-Filtered Stats")
        st.write("**🔁 Swap Volume:**", f"${stats['filtered'].get('volume', {}).get('volume', 0):,.2f}")
        st.write("**🎯 Referrals:**", stats["filtered"].get("referrals", 0))

# === Last Sync Time ===
last_sync = get_last_sync(SECTION_KEY)
if last_sync:
    st.info(f"✅ Last synced at: `{last_sync.strftime('%Y-%m-%d %H:%M')} UTC`")
else:
    st.warning("⚠️ Last sync time not available.")

# === Table Filter ===
st.markdown("### 🔎 Filter Transactions Table")
tf_col1, tf_col2 = st.columns([3, 2])
with tf_col1:
    search_input = st.text_input("🔍 Filter by Username, Email, or Wallet", value=st.session_state.get("search_filter", ""))
with tf_col2:
    selected_chains = st.multiselect("🪙 Chain Filter", [
        "base", "arbitrum", "ethereum", "polygon", "avalanche",
        "mode", "bnb", "sui", "solana", "optimism"
    ])

if st.button("🧹 Apply Table Filter"):
    st.session_state.search_filter = search_input
    st.rerun()

# === Fetch and Display Transactions ===
txns = fetch_transactions_filtered(
    search_user_or_email=st.session_state.search_filter or None,
    since_date=start_date.isoformat(),
    from_chain=None,  # You can apply selected_chains filter here
    to_chain=None,
    limit=1000
)

columns = [
    "Date", "Type", "Status", "From User", "To User", "From Token",
    "From Chain", "To Token", "To Chain", "Amount USD", "Tx Hash", "Tx Display"
]

df = pd.DataFrame(txns, columns=columns) if txns else pd.DataFrame(columns=columns)
df["Tx Display"] = df["Tx Display"].fillna("")

st.subheader("📋 Transactions Table")
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_pagination(paginationAutoPageSize=True)
gb.configure_default_column(filter=True, sortable=True, resizable=True)
gb.configure_side_bar()

# Filters
for col in ["Type", "Status", "From User", "To User", "From Token", "From Chain", "To Token", "To Chain"]:
    gb.configure_column(col, filter="agTextColumnFilter")

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

# === Summary ===
if not df.empty and "Amount USD" in df.columns:
    st.markdown(f"### 💸 Total Amount USD: **${df['Amount USD'].sum():,.2f}**")

    st.markdown("### 📊 Summary of Filtered Transactions")
    grouped = (
        df.groupby(["Type", "Status"])
          .agg(Quantity=("Amount USD", "count"), Amount=("Amount USD", "sum"))
          .reset_index()
    )

    pivot_qty = grouped.pivot(index="Type", columns="Status", values="Quantity").fillna(0).astype(int)
    pivot_amt = grouped.pivot(index="Type", columns="Status", values="Amount").fillna(0.0)

    summary = pd.DataFrame(index=pivot_qty.index)
    for status in sorted(grouped["Status"].unique()):
        summary[f"{status} Qty"] = pivot_qty.get(status, 0)
        summary[f"{status} Amt"] = pivot_amt.get(status, 0.0)

    summary["Total Qty"] = summary.filter(like="Qty").sum(axis=1)
    amt_cols = [c for c in summary.columns if "Amt" in c]
    for c in amt_cols:
        summary[c] = summary[c].astype(float)
    summary["Total Amt"] = summary[amt_cols].sum(axis=1).round(2)

    st.dataframe(summary.style.format({col: "${:,.2f}" for col in amt_cols}))
else:
    st.info("ℹ️ No transactions to summarize.")
