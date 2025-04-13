import streamlit as st
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode
import pandas as pd
from datetime import date, datetime, timedelta
from helpers.fetch import (
    fetch_transactions_filtered,
    fetch_user_profile_summary,
    fetch_user_metrics_full,
)
from helpers.connection import get_main_db_connection

st.set_page_config(page_title="Transactions", layout="wide")
st.title("🔄 Transactions")

# === Initialize state ===
if "search_filter" not in st.session_state:
    st.session_state.search_filter = ""

# === Top Filters Layout ===
col1, col2 = st.columns([3, 2])
with col1:
    user_input = st.text_input(
        "Search User (for stats) by Username, Email, or any of their Wallet Addresses",
        key="user_stats_input"
    )

with col2:
    start_date = st.date_input("Start Date Filter", value=date(2025, 1, 1))

end_date_default = date.today() + timedelta(days=1)
apply_today = st.checkbox("Include today", value=True)
end_date = end_date_default if apply_today else st.date_input("End Date Filter", value=date.today())

# === Load User Stats Button ===
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
                end=end_date.isoformat()
            )
            st.session_state.user_profile = profile
            st.session_state.user_stats = metrics
    else:
        st.warning("No user found for that input.")

# === Use stored filter input before fetching data ===
search_filter = st.session_state.search_filter

# === Fetch Filtered Transactions ===
selected_chains = []  # initialize before filters
chain_filters = selected_chains if selected_chains else None
txn_data = fetch_transactions_filtered(
    search_user_or_email=search_filter if search_filter else None,
    since_date=start_date.isoformat(),
    from_chain=None,
    to_chain=None,
    limit=1000,
)

df = pd.DataFrame(txn_data, columns=[
    "Date", "Type", "Status", "From User", "To User", "From Token",
    "From Chain", "To Token", "To Chain", "Amount USD", "Tx Hash"
])

# === Display User Stats (only when button pressed) ===
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
            st.markdown("General Info")
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
            st.markdown("Lifetime Stats")
            st.write("**Swap Volume:**", f"${user['lifetime'].get('volume', {}).get('volume', 0):,.2f}")
            st.write("**Referrals:**", user["lifetime"].get("referrals", 0))

        with col3:
            st.markdown("Date-Filtered Stats")
            st.write("**Swap Volume:**", f"${user['filtered'].get('volume', {}).get('volume', 0):,.2f}")
            st.write("**Referrals:**", user["filtered"].get("referrals", 0))

        # === Table Filter Input UI + Chain Filter (moved here) ===
        st.markdown("🔍 Filter Transactions Table")
        ft_col1, ft_col2, ft_col3 = st.columns([2, 2, 2])
        with ft_col1:
            search_input = st.text_input("Filter by Username, Email, or Wallet", value=st.session_state.search_filter, key="txn_filter_input")
        with ft_col2:
            selected_chains = st.multiselect(
                "Chain Filter",
                ["base", "arbitrum", "ethereum", "polygon", "avalanche", "mode", "bnb", "sui", "solana", "optimism"]
            )
        with ft_col3:
            if st.button("Apply Table Filter"):
                st.session_state.search_filter = search_input
                st.rerun()

# === Display Table ===
st.subheader("🖥 Transactions Table")

gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_pagination(paginationAutoPageSize=True)
gb.configure_default_column(
    filter=True,
    sortable=True,
    resizable=True
)

# Enable Excel-style filters per column
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
