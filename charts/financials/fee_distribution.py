import pandas as pd
import plotly.express as px


def render_fee_distribution(chain_df: pd.DataFrame):
    fig = px.pie(
        chain_df,
        values="value",
        names="chain",
        title="Fee Distribution by Chain",
        hole=0.4
    )
    fig.update_traces(textinfo="percent+label")
    fig.update_layout(title_text="Fee Distribution by Chain", title_x=0.5)
    return fig
