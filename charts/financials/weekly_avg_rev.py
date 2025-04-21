import altair as alt
import pandas as pd


def render_weekly_avg_rev(rev_df: pd.DataFrame) -> alt.Chart:
    rev_df["week_label"] = rev_df["week"].dt.strftime("%b %d")
    return alt.Chart(rev_df).mark_bar(size=35).encode(
        x=alt.X("week_label:N", title="Week", sort=rev_df["week_label"].tolist()),
        y=alt.Y("avg_rev_per_active_user:Q", title="Avg Rev / Active User", scale=alt.Scale(nice=True)),
        tooltip=[
            alt.Tooltip("week:T", title="Week"),
            alt.Tooltip("avg_rev_per_active_user:Q", title="Revenue", format=".4f")
        ]
    ).properties(
        width=500,
        height=500,
        title="Weekly Avg Revenue Per Active User"
    )
