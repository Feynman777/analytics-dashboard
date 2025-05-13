import altair as alt
import pandas as pd

def render_weekly_avg_rev(df: pd.DataFrame):
    if df.empty:
        return None

    # Ensure correct types
    df = df.copy()
    df["week"] = pd.to_datetime(df["week"])
    df["avg_rev_per_active_user"] = pd.to_numeric(df["avg_rev_per_active_user"], errors="coerce")

    # Smooth line
    df = df.sort_values("week")
    df["avg_rolling"] = df["avg_rev_per_active_user"].rolling(window=3, min_periods=1).mean()

    base = alt.Chart(df).encode(
        x=alt.X("week:T", title="Week"),
        y=alt.Y("avg_rev_per_active_user:Q", title="USD"),
    )

    line = base.mark_line(strokeWidth=2).encode(
        tooltip=["week:T", "avg_rev_per_active_user:Q"]
    )

    points = base.mark_circle(size=50)

    smoothed = alt.Chart(df).mark_line(strokeDash=[4, 4], color="gray").encode(
        x="week:T",
        y="avg_rolling:Q",
        tooltip=["week:T", "avg_rolling:Q"]
    )

    return alt.layer(line, points, smoothed).properties(
        title="Weekly Avg Revenue per Active User",
        width="container",
        height=400
    )
