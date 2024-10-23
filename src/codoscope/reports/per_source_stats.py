import logging
import os.path

import pandas
import plotly.graph_objects as go

from codoscope.common import NA_REPLACEMENT, ensure_dir, sanitize_filename
from codoscope.config import read_mandatory
from codoscope.datasets import Datasets
from codoscope.reports.common import (
    ReportBase,
    ReportType,
    render_widgets_report,
    setup_default_layout,
)
from codoscope.state import SourceType, StateModel
from codoscope.widgets.line_counts_stats import line_counts_stats

LOGGER = logging.getLogger(__name__)


class PerSourceStatsReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.PER_SOURCE_STATS

    def weekly_stats(self, df: pandas.DataFrame) -> go.Figure:
        df = df.set_index("timestamp")
        df["activity_type"] = df["activity_type"].fillna(NA_REPLACEMENT)

        grouped_by_activity = df.groupby(["activity_type"])

        fig = go.Figure()
        for (activity_type,), group_df in grouped_by_activity:
            weekly_counts = group_df.resample("W").size().reset_index(name="count")
            fig.add_trace(
                go.Bar(
                    name=activity_type,
                    x=weekly_counts["timestamp"],
                    y=weekly_counts["count"],
                )
            )

        setup_default_layout(fig, "Weekly Stats")

        fig.update_layout(
            barmode="stack",
            showlegend=True,  # ensure legend even for single series
        )

        return fig

    def weekly_stats_by_user(self, df: pandas.DataFrame) -> go.Figure:
        df = df.set_index("timestamp")
        df["user"] = df["user"].fillna(NA_REPLACEMENT)
        df["activity_type"] = df["activity_type"].fillna(NA_REPLACEMENT)

        sorted_df = df.sort_values(by=["user", "activity_type"], ascending=True)
        grouped_by_user_activity = sorted_df.groupby(["user", "activity_type"])

        fig = go.Figure()
        for (author, activity_type), group_df in grouped_by_user_activity:
            weekly_counts = group_df.resample("W").size().reset_index(name="count")

            trace_name = "%s %s" % (author, activity_type)

            fig.add_trace(
                go.Bar(
                    name=trace_name,
                    x=weekly_counts["timestamp"],
                    y=weekly_counts["count"],
                )
            )

        setup_default_layout(fig, "Weekly Stats by Author")

        fig.update_layout(
            barmode="stack",
            showlegend=True,  # ensure legend even for single series
        )

        return fig

    def generate_for_source(
        self,
        state: StateModel,
        source_name: str,
        report_path: str,
        df: pandas.DataFrame,
    ):
        widgets: list[go.Figure | None] = [
            self.weekly_stats(df),
            self.weekly_stats_by_user(df),
        ]

        source_state = state.sources[source_name]
        if source_state.source_type == SourceType.GIT:
            widgets.append(line_counts_stats(df, agg_period="W", title="Weekly Line Counts"))

        render_widgets_report(
            report_path,
            widgets,
            title=f"source :: {source_name}",
        )

    def generate(self, config: dict, state: StateModel, datasets: Datasets) -> None:
        parent_dir_path = os.path.abspath(read_mandatory(config, "dir-path"))
        ensure_dir(parent_dir_path)

        activity_df = datasets.activity
        activity_df["timestamp"] = pandas.to_datetime(activity_df["timestamp"], utc=True)

        grouped_by_source = activity_df.groupby(["source_name"])

        for (source_name,), source_df in grouped_by_source:
            file_name = sanitize_filename(source_name)
            file_path = "%s.html" % os.path.join(parent_dir_path, file_name)
            LOGGER.info('rendering report for "%s"', source_name)
            self.generate_for_source(state, source_name, file_path, source_df)
