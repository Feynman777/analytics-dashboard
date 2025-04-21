import altair as alt
import pandas as pd


def render_weekly_fees(weekly_df: pd.DataFrame) -> alt.Chart:
    weekly_df["week_label"] = weekly_df["week"].dt.strftime("%b %d")
    return alt.Chart(weekly_df).mark_bar(size=35).encode(
        x=alt.X("week_label:N", title="Week", sort=weekly_df["week_label"].tolist()),
        y=alt.Y("value:Q", title="Total Fees"),
        tooltip=[
            alt.Tooltip("week:T", title="Week"),
            alt.Tooltip("value:Q", title="Fees", format=".2f")
        ]
    ).properties(
        width=500,
        height=500,
        title="Weekly Fees"
    )
