import streamlit as st
from helpers.connection import get_main_db_connection, get_cache_db_connection
from helpers.fetch.home import fetch_home_stats

# === Load Data ===
conn_main = get_main_db_connection()
conn_cache = get_cache_db_connection()
stats = fetch_home_stats(conn_main, conn_cache)
conn_main.close()
conn_cache.close()

# === Metric Display Helper ===
def show_metric(col, label, value, prefix="", decimals=2):
    if isinstance(value, float):
        formatted = f"{prefix}{value:,.{decimals}f}"
    else:
        formatted = f"{int(value):,}"

    col.markdown(f"""
        <div style="line-height: 1.2; margin-bottom: 12px;">
            <div style="font-size: 1.5rem; font-weight: 600; color: inherit;">{label}</div>
            <div style="font-size: 1.6rem; font-weight: 700; margin-top: 2px; color: inherit;">{formatted}</div>
        </div>
    """, unsafe_allow_html=True)

# === Clean Section Card ===
def section_card_manual(title, rows, color="transparent"):
    st.markdown(f"""
        <div style="
            background-color:{color};
            padding:5px 20px;
            border-radius:12px;
            border:1px solid #ddd;
            margin-bottom:10px;
        ">
            <h3 style="
                text-align:center;
                font-size:1.7rem;
                font-weight:700;
                margin: 0;
                padding: 0;
                line-height: 1.2;
                color: inherit;
            ">{title}</h3>
    """, unsafe_allow_html=True)

    for metrics in rows:
        cols = st.columns(len(metrics))
        for col, (label, val, opts) in zip(cols, metrics):
            show_metric(col, label, val, **opts)

    st.markdown("</div>", unsafe_allow_html=True)

# === Page Setup ===
st.set_page_config(page_title="Home Dashboard", layout="wide")
st.title("Home Dashboard")

# === CRYPTO ROW ===
crypto_col1, spacer, crypto_col2 = st.columns([1, 0.1, 1])

with crypto_col1:
    section_card_manual("Crypto — Last 24h", [[
        ("Swap Volume", stats["24h"].get("swap_volume", 0), {"prefix": "$"}),
        ("Revenue", stats["24h"].get("revenue", 0), {"prefix": "$"}),
        ("Swap Txns", stats["24h"].get("swap_transactions", 0), {"decimals": 0}),
        ("Send Txns", stats["24h"].get("send_transactions", 0), {"decimals": 0}),
    ]])

with crypto_col2:
    section_card_manual("Crypto — Lifetime", [[
        ("Swap Volume", stats["lifetime"].get("swap_volume", 0), {"prefix": "$"}),
        ("Revenue", stats["lifetime"].get("swap_revenue", stats["lifetime"].get("revenue", 0)), {"prefix": "$"}),
        ("Swap Txns", stats["lifetime"].get("swap_transactions", 0), {"decimals": 0}),
        ("Send Txns", stats["lifetime"].get("send_transactions", 0), {"decimals": 0}),
    ]])

st.markdown("<div style='height: 40px'></div>", unsafe_allow_html=True)

# === CASH ROW ===
cash_col1, spacer, cash_col2 = st.columns([1, 0.1, 1])

with cash_col1:
    section_card_manual("Cash — Last 24h", [[
        ("Cash Txns", stats["24h"].get("cash_transactions", 0), {"decimals": 0}),
        ("Cash Volume", stats["24h"].get("cash_volume", 0), {"prefix": "$"}),
        ("Cash Yield", stats["24h"].get("cash_yield", 0), {"prefix": "$"}),
    ]])

with cash_col2:
    section_card_manual("Cash — Lifetime", [[
        ("Cash Txns", stats["lifetime"].get("cash_transactions", 0), {"decimals": 0}),
        ("Cash Volume", stats["lifetime"].get("cash_volume", 0), {"prefix": "$"}),
        ("Cash Yield", stats["lifetime"].get("cash_yield", 0), {"prefix": "$"}),
    ]])

st.markdown("<div style='height: 40px'></div>", unsafe_allow_html=True)

# === USERS ROW ===
user_col1, spacer, user_col2 = st.columns([1, 0.1, 1])

with user_col1:
    section_card_manual("Users — Last 24h", [[
        ("Active Users", stats["24h"].get("active_users", 0), {"decimals": 0}),
        ("New Active Users", stats["24h"].get("new_active_users", 0), {"decimals": 0}),
        ("New Users", stats["24h"].get("new_users", 0), {"decimals": 0}),
    ]])

with user_col2:
    section_card_manual("Users — Lifetime", [[
        ("Active Users", stats["lifetime"].get("active_users", 0), {"decimals": 0}),
        ("Total Users", stats["lifetime"].get("total_users", 0), {"decimals": 0}),
    ]])
