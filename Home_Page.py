# === This is the full contents of Home_Page.py ===

import streamlit as st
import pandas as pd
from helpers.connection import get_main_db_connection
from helpers.fetch import fetch_home_stats, fetch_recent_transactions

# Load data
conn = get_main_db_connection()
stats = fetch_home_stats(conn)
recent_txns = fetch_recent_transactions(conn)
conn.close()

# === UI Layout ===
st.set_page_config(page_title="Home", layout="wide")
st.title("🏠 Home Dashboard")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Last 24h")
    st.metric("Active Users", stats["24h"]["active_users"])
    st.metric("Swap Volume", f"${stats['24h']['swap_volume']:.2f}")
    st.metric("Transactions", stats["24h"]["transactions"])

with col2:
    st.subheader("Lifetime")
    st.metric("Total Users", stats["lifetime"]["total_users"])
    st.metric("Revenue", f"${stats['lifetime']['revenue']:.2f}")

