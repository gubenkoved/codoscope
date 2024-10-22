import logging
import math
import os
import os.path

import pandas
import plotly.graph_objects as go

from codoscope.common import NA_REPLACEMENT, ensure_dir_for_path
from codoscope.config import read_mandatory, read_optional
from codoscope.datasets import Datasets
from codoscope.reports.common import (
    ReportBase,
    ReportType,
    render_widgets_report,
    setup_default_layout,
)
from codoscope.state import StateModel
from codoscope.widgets.activity_scatter import activity_scatter

LOGGER = logging.getLogger(__name__)


def apply_filter(df: pandas.DataFrame, expr: str) -> pandas.DataFrame:
    local_vars = {col: df[col] for col in df.columns}
    filtered_df = df.eval(expr, local_dict=local_vars)
    return df[filtered_df]


def people_timeline(df: pandas.DataFrame) -> go.Figure:
    df["author"] = df["author"].fillna(NA_REPLACEMENT)

    timestamp_range = [
        df["timestamp"].min(),
        df["timestamp"].max(),
    ]

    fig = go.Figure()

    # add line where start is equal to end timestamp
    fig.add_trace(
        go.Scatter(
            x=timestamp_range,
            y=timestamp_range,
            mode="lines",
            name="n/a",
            showlegend=False,
            opacity=0.4,
            line=dict(
                color="lightgray",
                width=1,
            ),
            hoverinfo="none",
        )
    )

    grouped_by_user = df.sort_values("timestamp", ascending=True).groupby(["author"])

    for (user,), user_df in grouped_by_user:
        first_timestamp = user_df["timestamp"].min()
        last_timestamp = user_df["timestamp"].max()

        text_atoms = [
            "<b>first:</b> %s<br>" % first_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "<b>last:</b> %s<br>" % last_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        ]

        # compute totals per activity
        user_grouped_by_activity = user_df.groupby(["activity_type"])
        for (activity_type,), activity_df in user_grouped_by_activity:
            text_atoms.append("<br><b>%s:</b> %d" % (activity_type, len(activity_df)))

        total_size_class = user_df["size_class"].sum()

        # single point per user
        fig.add_trace(
            go.Scatter(
                x=[first_timestamp],
                y=[last_timestamp],
                mode="markers",
                name=user,
                marker=dict(
                    symbol="x",
                    # this is arbitrary, really
                    size=4 + math.log(1 + (total_size_class / 16), 2),
                ),
                opacity=0.8,
                text=[
                    "".join(text_atoms),
                ],
                hovertemplate="%{text}",
                hoverinfo="text+name",
            )
        )

    setup_default_layout(fig, "People Timeline")

    fig.update_layout(
        xaxis_title="First contribution",
        yaxis_title="Last contribution",
        showlegend=True,
    )

    return fig


def convert_timestamp_timezone(df: pandas.DataFrame, timezone_name: str | None) -> pandas.DataFrame:
    if timezone_name:
        LOGGER.debug('converting timestamps to timezone "%s"', timezone_name)
        df["timestamp"] = pandas.to_datetime(df["timestamp"], utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert(timezone_name)
    return df


class OverviewReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.OVERVIEW

    def generate(self, config: dict, state: StateModel, datasets: Datasets):
        out_path = os.path.abspath(read_mandatory(config, "out-path"))
        ensure_dir_for_path(out_path)

        # TODO: include somewhere in the report filter and timezone used
        filter_expr = read_optional(config, "filter")

        activity_df = datasets.activity
        activity_df = convert_timestamp_timezone(activity_df, config.get("timezone"))

        # apply filters if applicable
        if filter_expr:
            count_before_filter = len(activity_df)
            activity_df = apply_filter(activity_df, filter_expr)
            count_after_filter = len(activity_df)
            LOGGER.info(
                'filter "%s" left %d of %d data points',
                filter_expr,
                count_after_filter,
                count_before_filter,
            )

        LOGGER.info("total data points: %d", len(activity_df))

        render_widgets_report(
            out_path,
            [
                activity_scatter(activity_df),
                people_timeline(activity_df),
            ],
            title="overview",
        )
