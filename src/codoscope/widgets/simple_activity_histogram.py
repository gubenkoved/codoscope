import pandas
import plotly.graph_objects as go
from pandas import DataFrame

from codoscope.reports.common import setup_default_layout
from codoscope.widgets.common import PlotlyFigureWidget


def simple_activity_histogram(
    activity_df: pandas.DataFrame,
    group_by: str,
    agg_column: str,
    agg_type: str = "min",
    agg_period: str = "QE",
    title: str | None = None,
    height: int = 600,
) -> PlotlyFigureWidget | None:

    if len(activity_df) == 0:
        return None

    activity_df = activity_df.copy()
    activity_df["timestamp"] = pandas.to_datetime(activity_df["timestamp"], utc=True)

    grouped_df: DataFrame = (
        activity_df.groupby([group_by]).agg(target=(agg_column, agg_type)).reset_index()
    )

    # convert target into datetime column
    grouped_df["target"] = pandas.to_datetime(grouped_df["target"], utc=True)

    # index by target
    grouped_df = grouped_df.set_index("target")

    # aggregate using specified aggregation period
    counts_df: DataFrame = grouped_df.resample(agg_period).size().reset_index(name="count")

    # for some reason "Q" is being deprecated as "agg" type in favor of "QE",
    # however "to_period" does not understand "QE" at all
    dt_period_type: str = {
        "QE": "Q",
    }.get(agg_period, agg_period)

    # avoid warning "Converting to PeriodArray/Index representation will drop timezone information"
    # by explicitly droppping timezone information for the needs of period label calculation
    counts_df["period"] = (
        counts_df["target"].dt.tz_localize(None).dt.to_period(dt_period_type).astype(str)
    )

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=counts_df["period"],
            y=counts_df["count"],
        )
    )

    setup_default_layout(fig, title=title)

    return PlotlyFigureWidget(fig, height=height)
