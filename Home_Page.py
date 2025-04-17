# === This is the full contents of Home_Page.py ===

import streamlit as st
import pandas as pd
from helpers.connection import get_cache_db_connection, get_main_db_connection
from helpers.fetch import fetch_home_stats, fetch_recent_transactions

# Load data
conn_cache = get_cache_db_connection()
conn_main = get_main_db_connection()
stats = fetch_home_stats(conn_main, conn_cache)
recent_txns = fetch_recent_transactions(conn_main)
conn_cache.close()
conn_main.close()

# === UI Layout ===
st.set_page_config(page_title="Home", layout="wide")
st.title("🏠 Home Dashboard")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Last 24h")
    st.metric("Transactions (24h)", f'{int(stats["24h"]["transactions"]):,}')
    st.metric("Swap Volume", f"${float(stats['24h']['swap_volume']):,.2f}")
    st.metric("Revenue", f"${float(stats['24h']['revenue']):,.2f}")
    st.metric("Active Users", int(stats["24h"]["active_users"]))

with col2:
    st.subheader("Lifetime")
    st.metric("Lifetime Transactions", f'{int(stats["lifetime"]["transactions"]):,}')
    st.metric("Lifetime Swap Volume", f"${float(stats['lifetime']['swap_volume']):,.2f}")
    st.metric("Lifetime Revenue", f"${float(stats['lifetime']['revenue']):,.2f}")
    st.metric("Total Users", int(stats["lifetime"]["total_users"]))

