import plotly.graph_objects as go
from pandas import DataFrame

from codoscope.common import WEEKDAY_ORDER
from codoscope.reports.common import setup_default_layout
from codoscope.widgets.common import PlotlyFigureWidget


def activity_heatmap(
    activity_df: DataFrame,
    title: str | None = None,
    height: int = 310,
    timezone_name: str = "utc",
) -> PlotlyFigureWidget | None:
    activity_df = activity_df.set_index("timestamp")

    aggregated_df = activity_df.resample("D").size().to_frame(name="count")
    aggregated_df["date"] = aggregated_df.index.date
    aggregated_df["day_of_week"] = aggregated_df.index.dayofweek  # Monday=0, Sunday=6
    aggregated_df["week_start"] = (
        aggregated_df.index.tz_convert(timezone_name).tz_localize(None).to_period("W").start_time
    )

    heatmap_data = aggregated_df.pivot(
        index=["day_of_week"],
        columns=["week_start"],
        values="count",
    )

    fig = go.Figure()

    colorscale = [
        [0.0, "#ffffff"],
        [0.1, "#c3e0d7"],
        [1.0, "#007d53"],
    ]

    fig.add_trace(
        go.Heatmap(
            z=heatmap_data.values,
            x=heatmap_data.columns,
            y=WEEKDAY_ORDER,
            colorscale=colorscale,
        )
    )

    setup_default_layout(fig, title=title)

    fig.update_layout(
        yaxis=dict(
            categoryorder="array",
            categoryarray=list(reversed(WEEKDAY_ORDER)),
        ),
    )

    # TODO: does not work for some reason with the following error:
    #   ignored yaxis.scaleanchor: "x" to avoid either an infinite loop and
    #   possibly inconsistent scaleratios

    # set scale so that blocks are square
    # fig.update_layout(
    #     yaxis=dict(
    #         scaleanchor="x",
    #         scaleratio=1,
    #     ),
    # )

    return PlotlyFigureWidget(fig, height=height)
