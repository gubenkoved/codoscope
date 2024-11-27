from datetime import datetime

import pandas
import plotly.graph_objects as go
from pandas import DataFrame

from codoscope.common import WEEKDAY_ORDER
from codoscope.reports.common import setup_default_layout, time_axis_hours_based
from codoscope.widgets.common import PlotlyFigureWidget


def activity_by_weekday(
    activity_df: pandas.DataFrame,
    title: str | None = None,
    height: int = 600,
) -> PlotlyFigureWidget | None:

    if len(activity_df) == 0:
        return None

    activity_df["weekday"] = activity_df["timestamp"].dt.day_name()

    grouped_df: DataFrame = activity_df.groupby("weekday").size().reset_index(name="count")

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            name=f"activity count",
            x=grouped_df["weekday"],
            y=grouped_df["count"],
            marker_color="#2296bf",  # blue-ish
        ),
    )

    setup_default_layout(fig, title or "Activity by weekday")

    fig.update_layout(
        height=height,
        xaxis=dict(
            categoryorder="array",
            categoryarray=WEEKDAY_ORDER,
        ),
    )

    return PlotlyFigureWidget(fig)


def activity_offset_hisogram(
    activity_df: pandas.DataFrame,
    title: str | None = None,
    height: int = 600,
) -> PlotlyFigureWidget | None:

    if len(activity_df) == 0:
        return None

    def get_utc_offset_seconds(x: datetime) -> float:
        offset = x.utcoffset()
        if offset is None:
            return 0.0
        return offset.total_seconds()

    def format_offset(x: datetime) -> str:
        result: str = x.strftime("%z")
        return "%s:%s" % (result[:-2], result[-2:])

    # copy data frame before making changes
    activity_df = activity_df.copy()

    # can not use dt since timestamp here should not be normalized to utc
    activity_df["timezone_offset_numeric"] = activity_df["timestamp"].apply(get_utc_offset_seconds)
    activity_df["timezone_offset"] = activity_df["timestamp"].apply(format_offset)

    grouped_df: DataFrame = (
        activity_df.groupby(["timezone_offset_numeric", "timezone_offset"])
        .size()
        .reset_index(name="count")
    )

    grouped_df = grouped_df.sort_values(["timezone_offset_numeric"])

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            name=f"time offset count",
            x=grouped_df["timezone_offset"],
            y=grouped_df["count"],
        ),
    )

    setup_default_layout(fig, title or "Timezone offsets")

    fig.update_layout(
        height=height,
        xaxis=dict(
            type="category",
        ),
    )

    return PlotlyFigureWidget(fig)


def activity_by_weekday_2d(
    activity_df: pandas.DataFrame,
    title: str | None = None,
    height: int = 600,
    time_bin_size_hours: float = 0.5,
) -> PlotlyFigureWidget | None:

    if len(activity_df) == 0:
        return None

    # copy data frame before making changes
    activity_df = activity_df.copy()

    activity_df["weekday"] = activity_df["timestamp"].apply(lambda x: x.strftime("%A"))
    activity_df["day_offset_hours"] = activity_df["timestamp"].apply(
        lambda x: x.hour + x.minute / 60.0
    )

    fig = go.Figure()

    fig.add_trace(
        go.Histogram2d(
            name=f"activity count",
            x=activity_df["weekday"],
            y=activity_df["day_offset_hours"],
            ybins=dict(
                size=time_bin_size_hours,
            ),
            # better reflect small values
            colorscale=[
                [0, "rgb(0,20,60)"],
                [0.2, "rgb(10,136,186)"],
                [0.5, "rgb(242,211,56)"],
                [0.75, "rgb(242,143,56)"],
                [1, "rgb(217,30,30)"],
            ],
        ),
    )

    setup_default_layout(fig, title or "Activity by weekday")

    tickvals, ticktext = time_axis_hours_based()

    # TODO: fix the hovertemplate so that "Y" value is properly displayed
    #  using the time format instead of raw value
    fig.update_layout(
        height=height,
        xaxis=dict(
            categoryorder="array",
            categoryarray=WEEKDAY_ORDER,
        ),
        yaxis=dict(
            tickmode="array",
            tickvals=tickvals,
            ticktext=ticktext,
        ),
    )

    return PlotlyFigureWidget(fig)
