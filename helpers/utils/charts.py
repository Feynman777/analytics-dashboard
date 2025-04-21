import altair as alt
import streamlit as st

def render_badge(change):
    if change is None:
        return ""
    color = "#28a745" if change > 0 else ("#dc3545" if change < 0 else "#6c757d")
    arrow = "▲" if change > 0 else ("▼" if change < 0 else "→")
    return f"""
        <div style="margin-top:4px;">
            <span style="
                background-color:{color};
                color:white;
                padding:5px 10px;
                border-radius:5px;
                font-size:0.9rem;
                font-weight:600;
            ">
                {arrow} {change:+.2f}%
            </span>
        </div>
    """

def week_over_week_change(df, col="value"):
    if len(df) < 2:
        return None
    prev = df.iloc[-2][col]
    curr = df.iloc[-1][col]
    if prev == 0:
        return None
    return round(((curr - prev) / prev) * 100, 2)

def daily_metric_section(df, title, label, col="value"):
    if df.empty:
        st.warning(f"No data for {title}")
        return None
    df["date_str"] = df["date"].astype(str)
    total = df[col].sum()
    min_val = df[col].min()
    max_val = df[col].max()

    st.markdown(f"""
        <div style="font-size:1.1rem; font-weight:bold; margin-top:20px;">{title}</div>
        <div style="margin-bottom:6px;">
            <span>Total: <code>{total:,.2f}</code> | Min: <code>{min_val:,.2f}</code> | Max: <code>{max_val:,.2f}</code></span>
        </div>
    """, unsafe_allow_html=True)

    return alt.Chart(df).mark_bar().encode(
        x=alt.X("date_str:O", title="Date"),
        y=alt.Y(f"{col}:Q", title=label),
        tooltip=[
            alt.Tooltip("date_str:N", title="Date"),
            alt.Tooltip(f"{col}:Q", title=label, format=",.2f")
        ]
    ).properties(height=500)

def metric_section(df, title, label, col="value"):
    if df.empty:
        st.warning(f"No data for {title}")
        return None
    df["week_str"] = df["week"].astype(str)
    total = df[col].sum()
    min_val = df[col].min()
    max_val = df[col].max()
    change = week_over_week_change(df, col)
    badge = render_badge(change)

    st.markdown(f"""
        <div style="font-size:1.1rem; font-weight:bold; margin-top:20px;">{title}</div>
        <div style="margin-bottom:6px;">
            <span>Total: <code>{total:,.2f}</code> | Min: <code>{min_val:,.2f}</code> | Max: <code>{max_val:,.2f}</code></span>
        </div>
        {badge}
    """, unsafe_allow_html=True)

    return alt.Chart(df).mark_bar().encode(
        x=alt.X("week_str:O", title="Week"),
        y=alt.Y(f"{col}:Q", title=label),
        tooltip=[
            alt.Tooltip("week_str:N", title="Week"),
            alt.Tooltip(f"{col}:Q", title=label, format=",.2f")
        ]
    ).properties(height=500)

def user_volume_chart(df):
    return alt.Chart(df).mark_line(point=True).encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("daily_volume_usd:Q", title="Volume (USD)", scale=alt.Scale(zero=False)),
        color=alt.Color("username:N", title="User"),
        tooltip=[
            "date:T",
            "username:N",
            alt.Tooltip("daily_volume_usd:Q", format=".2f")
        ]
    ).properties(
        width=700,
        height=400,
        title="📈 Daily Volume (USD)"
    ).interactive()

def user_txn_detail_chart(df, username):
    return alt.Chart(df).mark_line(point=True).encode(
        x=alt.X("datetime:T", title="Date & Time"),
        y=alt.Y("volume_usd:Q", title="Volume (USD)", scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip("datetime:T"),
            alt.Tooltip("volume_usd:Q", format=".2f")
        ]
    ).properties(
        width=700,
        height=300,
        title=f"📈 Transaction Volume for {username}"
    ).interactive()

def user_line_chart(df, username_col="username"):
    return alt.Chart(df).mark_line(point=True).encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("daily_volume_usd:Q", title="Volume (USD)", scale=alt.Scale(zero=False)),
        color=alt.Color(f"{username_col}:N", title="User"),
        tooltip=["date:T", f"{username_col}:N", alt.Tooltip("daily_volume_usd:Q", format=".2f")]
    ).properties(
        width=700,
        height=400,
        title="📈 Daily Volume (USD)"
    ).interactive()

def simple_bar_chart(df, x_field, y_field, title="Chart"):
    return alt.Chart(df).mark_bar().encode(
        x=alt.X(f"{x_field}:T", title="Date"),
        y=alt.Y(f"{y_field}:Q", title="Value"),
        tooltip=[alt.Tooltip(f"{x_field}:T"), alt.Tooltip(f"{y_field}:Q", format=",.2f")]
    ).properties(
        title=title,
        height=350
    )