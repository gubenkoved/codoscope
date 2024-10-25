import pandas
import plotly.graph_objects as go
from pandas import DataFrame

from codoscope.reports.common import setup_default_layout
from codoscope.widgets.common import PlotlyFigureWidget

WEEKDAY_ORDER = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


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

    # can not use dt since timestamp here should not be normalized to utc
    activity_df["timezone_offset"] = activity_df["timestamp"].apply(lambda x: x.strftime("%z"))

    grouped_df: DataFrame = activity_df.groupby("timezone_offset").size().reset_index(name="count")

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
    )

    return PlotlyFigureWidget(fig)


def activity_by_weekday_2d(
    activity_df: pandas.DataFrame,
    title: str | None = None,
    height: int = 600,
) -> PlotlyFigureWidget | None:

    if len(activity_df) == 0:
        return None

    activity_df["weekday"] = activity_df["timestamp"].apply(lambda x: x.strftime("%A"))
    activity_df["hour"] = activity_df["timestamp"].apply(lambda x: x.hour)

    fig = go.Figure()

    fig.add_trace(
        go.Histogram2d(
            name=f"activity count",
            x=activity_df["weekday"],
            y=activity_df["hour"],
            nbinsy=24,
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
