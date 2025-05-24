import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, time
from helpers.connection import get_cache_db_connection
from helpers.fetch.user import fetch_top_users_by_metric
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# === SETUP ===
st.set_page_config(page_title="User Data", layout="wide")

# Detect theme
theme_base = st.get_option("theme.base")
is_dark_mode = theme_base == "dark"
grid_theme = "material-dark" if is_dark_mode else "streamlit"

LEADERBOARD_TYPES = {
    "Swap Volume": "swap",
    "Cash Volume": "cash",
    "Referrals": "referrals"
}
DEFAULT_LEADERBOARD = "Swap Volume"

if "selected_leaderboard" not in st.session_state:
    st.session_state.selected_leaderboard = DEFAULT_LEADERBOARD
if "selected_time" not in st.session_state:
    st.session_state.selected_time = "Last 7 Days"
if "custom_date_range" not in st.session_state:
    st.session_state.custom_date_range = None
if "top_n" not in st.session_state:
    st.session_state.top_n = 10

CHAIN_OPTIONS = [
    "base", "arbitrum", "ethereum", "polygon", "avalanche",
    "mode", "bnb", "sui", "solana", "optimism"
]

# === LAYOUT ===
col_left, col_right = st.columns([1.2, 1.2])

with col_left:
    st.title("🏆 Leaderboard Filters")

    # Leaderboard type selector
    st.subheader("Metric")
    for option in LEADERBOARD_TYPES.keys():
        if st.button(option, use_container_width=True, key=f"type_{option}"):
            st.session_state.selected_leaderboard = option

    st.markdown(
        f"<div style='margin-top:-10px; color: gray;'>Current:</div>"
        f"<h5 style='color: royalblue'>{st.session_state.selected_leaderboard}</h5>",
        unsafe_allow_html=True
    )

    # Top N selector
    st.subheader("Top N Users")
    top_n_choice = st.radio(
        label="Show top:",
        options=[10, 25, 50, 100],
        index=[10, 25, 50, 100].index(st.session_state.top_n),
        horizontal=True
    )
    st.session_state.top_n = top_n_choice

    # Chain filter
    st.subheader("Chains")
    selected_chains = st.multiselect("Filter by chain:", CHAIN_OPTIONS, default=[])

    # === Date Range Filter ===
    st.subheader("Date Range")
    DATE_OPTIONS = {
        "Last 24 Hours": timedelta(days=1),
        "Last 7 Days": timedelta(days=7),
        "Last 30 Days": timedelta(days=30),
        "Lifetime": None,
        "Custom": "custom"
    }

    date_selection = st.radio("Select time range:", list(DATE_OPTIONS.keys()), index=1)

    custom_range = None
    if date_selection == "Custom":
        custom_range = st.date_input("Pick a custom range:", value=st.session_state.custom_date_range or [])
        if len(custom_range) == 2:
            st.session_state.custom_date_range = custom_range
    else:
        st.session_state.custom_date_range = None

# === COMPUTE DATE FILTER ===
now = datetime.utcnow()
start_date = end_date = None

if date_selection == "Custom" and st.session_state.custom_date_range:
    start, end = st.session_state.custom_date_range
    start_date = datetime.combine(start, time.min)
    end_date = datetime.combine(end, time.max)
elif date_selection != "Custom" and DATE_OPTIONS[date_selection] is not None:
    end_date = now
    start_date = now - DATE_OPTIONS[date_selection]

# === FETCH + DISPLAY LEADERBOARD ===
with col_right:
    st.title("👥 User Leaderboard")

    with st.spinner("Loading leaderboard..."):
        conn = get_cache_db_connection()
        metric_key = LEADERBOARD_TYPES[st.session_state.selected_leaderboard]

        results = fetch_top_users_by_metric(
            conn,
            metric=metric_key,
            start_date=start_date,
            end_date=end_date,
            chains=selected_chains if selected_chains else None,
            limit=st.session_state.top_n
        )

        if metric_key in ["swap", "cash"]:
            value_col = "Swap Volume ($)" if metric_key == "swap" else "Cash Volume ($)"
            df = pd.DataFrame([
                {"Place": i + 1, "Username": u, value_col: float(v or 0)}
                for i, (u, v) in enumerate(results)
            ])
        elif metric_key == "referrals":
            df = pd.DataFrame([
                {
                    "Place": i + 1,
                    "Username": u,
                    "Referral Count": int(r.get("count", 0)),
                    "Referral Volume ($)": float(r.get("volume", 0))
                }
                for i, (u, r) in enumerate(results)
            ])
        else:
            df = pd.DataFrame()

    if df.empty:
        st.warning("No leaderboard data available for the selected filters.")
    else:
        # Format numeric columns
        for col in df.columns:
            if "Volume" in col:
                df[col] = df[col].map("{:,.2f}".format)

        # Align header left
        st.markdown("""
            <style>
            .ag-header-cell-label {
                justify-content: flex-start !important;
                text-align: left !important;
            }
            </style>
        """, unsafe_allow_html=True)

        # Configure AgGrid
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(
            headerClass="left-align-header",
            cellStyle={"textAlign": "left"},
            sortable=True,
            resizable=True,
            filter=True
        )

        if "Username" in df.columns:
            gb.configure_column("Username", filter="agTextColumnFilter")
        for col in df.columns:
            if "Volume" in col:
                gb.configure_column(col, filter="agNumberColumnFilter")

        grid_options = gb.build()

        AgGrid(
            df,
            gridOptions=grid_options,
            height=950,
            theme=grid_theme,
            allow_unsafe_jscode=True,
            update_mode=GridUpdateMode.NO_UPDATE,
            fit_columns_on_grid_load=True
        )
