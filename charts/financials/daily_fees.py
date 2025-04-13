import altair as alt
import pandas as pd

def render_daily_fees(daily_df: pd.DataFrame) -> alt.Chart:
    return alt.Chart(daily_df).mark_bar(size=8).encode(
        x=alt.X("date:T", title="Date", axis=alt.Axis(labelAngle=-45, format="%b %d")),
        y=alt.Y("value:Q", title="Total Fees"),
        tooltip=[
            alt.Tooltip("date:T", title="Date"),
            alt.Tooltip("value:Q", title="Fees", format=".2f")
        ]
    ).properties(
        width=500,
        height=500,
        title="Daily Fees"
    )
