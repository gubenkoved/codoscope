import logging
import math
import os
import os.path

import pandas
import plotly.graph_objects as go

from codoscope.common import (
    NA_REPLACEMENT,
    convert_timezone,
    ensure_dir_for_path,
    apply_filter,
)
from codoscope.config import read_mandatory, read_optional
from codoscope.datasets import Datasets
from codoscope.reports.common import (
    ReportBase,
    ReportType,
    render_widgets_report,
    setup_default_layout,
)
from codoscope.state import StateModel
from codoscope.widgets.active_contributors_count import active_contributors_count
from codoscope.widgets.activity_scatter import activity_scatter
from codoscope.widgets.common import CompositeWidget, PlotlyFigureWidget
from codoscope.widgets.simple_activity_histogram import simple_activity_histogram
from codoscope.widgets.activity_heatmap import activity_heatmap

LOGGER = logging.getLogger(__name__)


def people_timeline(df: pandas.DataFrame) -> PlotlyFigureWidget:
    df["user"] = df["user"].fillna(NA_REPLACEMENT)

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

    grouped_by_user = df.sort_values("timestamp", ascending=True).groupby(["user"])

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

    return PlotlyFigureWidget(fig)


class OverviewReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.OVERVIEW

    def generate(self, config: dict, state: StateModel, datasets: Datasets):
        out_path = os.path.abspath(read_mandatory(config, "out-path"))
        ensure_dir_for_path(out_path)

        activity_df = convert_timezone(
            datasets.activity,
            timezone_name=config.get("timezone"),
            inplace=False,
        )

        # apply filters if applicable
        filter_expr = read_optional(config, "filter")
        activity_df = apply_filter(activity_df, filter_expr)

        LOGGER.info("total data points: %d", len(activity_df))

        render_widgets_report(
            out_path,
            [
                activity_scatter(activity_df),
                activity_heatmap(activity_df),
                people_timeline(activity_df),
                CompositeWidget(
                    [
                        [
                            simple_activity_histogram(
                                activity_df,
                                group_by="user",
                                agg_column="timestamp",
                                agg_type="min",
                                agg_period="QE",
                                title="First contribution by date (quaterly)",
                                height=600,
                            ),
                            simple_activity_histogram(
                                activity_df,
                                group_by="user",
                                agg_column="timestamp",
                                agg_type="max",
                                agg_period="QE",
                                title="Last contribution by date (quaterly)",
                                height=600,
                            ),
                            active_contributors_count(
                                activity_df,
                                title="Active contributors count",
                                height=600,
                            ),
                        ]
                    ]
                ),
            ],
            title="overview",
        )
