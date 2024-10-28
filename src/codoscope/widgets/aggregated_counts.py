import pandas
import plotly.graph_objects as go

from codoscope.common import NA_REPLACEMENT
from codoscope.reports.common import setup_default_layout
from codoscope.widgets.common import PlotlyFigureWidget


def aggregated_counts(
    activity_df: pandas.DataFrame,
    group_by: list[str],
    agg_period: str = "W",
    title: str | None = None,
) -> PlotlyFigureWidget:
    df = activity_df.set_index("timestamp")

    for column in group_by:
        df[column] = df[column].fillna(NA_REPLACEMENT)

    grouped = df.groupby(group_by)

    fig = go.Figure()
    for group_key, group_df in grouped:
        weekly_counts = group_df.resample(agg_period).size().reset_index(name="count")
        fig.add_trace(
            go.Bar(
                name=" ".join(group_key),
                x=weekly_counts["timestamp"],
                y=weekly_counts["count"],
            )
        )

    setup_default_layout(fig, title=title)

    fig.update_layout(
        barmode="stack",
        showlegend=True,  # ensure legend even for single series
    )

    return PlotlyFigureWidget(fig)
