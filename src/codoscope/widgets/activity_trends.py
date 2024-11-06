
import plotly.graph_objects as go
from pandas import DataFrame

from codoscope.reports.common import setup_default_layout
from codoscope.widgets.common import PlotlyFigureWidget


class Metric:
    def __init__(
        self,
        name,
        report_period,
        report_aggregation="mean",
        line_width=2.0,
        opacity=1.0,
        marker_size=None,
    ):
        self.name = name
        self.report_period = report_period
        self.report_aggregation = report_aggregation
        self.line_width = line_width
        self.opacity = opacity
        self.marker_size = marker_size

        if marker_size is None:
            self.marker_size = line_width * 1.5


def activity_trend(
    activity_df: DataFrame,
    window_period: str = "h",
    aggregation_period: str = "D",
    metrics: list[Metric] = [Metric("monthly", "ME")],
    title: str | None = "Average daily active hours reported monthly",
    height: int = 600,
) -> PlotlyFigureWidget | None:

    activity_df = activity_df.set_index("timestamp")

    # phase 1: all activity is split into "window" chunks and each chuck is either active or not
    window_resampled = activity_df.resample(window_period).size().to_frame("count")
    is_active_window = (window_resampled["count"] > 0).astype("int")

    # phase 2. chunks are further resampled using aggregation periods and counts within each agg
    #  period is calcualted; at this state we have amount of active windows within aggregation
    #  period
    aggregation_resampled = is_active_window.resample(aggregation_period).sum().to_frame("count")

    fig = go.Figure()
    max_y = 0

    for metric in metrics:
        # phase 3. aggregation chunks are further resampled using report period
        report_resampled = aggregation_resampled.resample(metric.report_period)

        # compute required report metrick
        report_df = getattr(report_resampled, metric.report_aggregation)()

        x = report_df.index
        y = report_df["count"]
        max_y = max(max_y, y.max())

        fig.add_trace(
            go.Scatter(
                name=metric.name,
                x=x,
                y=y,
                mode="lines+markers",
                opacity=metric.opacity,
                line=dict(
                    width=metric.line_width,
                ),
                marker=dict(
                    symbol="circle-open",
                    size=metric.marker_size,
                ),
            )
        )

    fig.update_layout(
        yaxis=dict(range=[0, max_y]),
    )

    setup_default_layout(fig, title=title)

    return PlotlyFigureWidget(fig, height=height)
