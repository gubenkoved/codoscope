import logging
import os.path

import pandas
import plotly.graph_objects as go

from codoscope.common import sanitize_filename, ensure_dir
from codoscope.config import read_mandatory
from codoscope.datasets import Datasets
from codoscope.reports.common import (
    ReportBase,
    ReportType,
    setup_default_layout,
    render_plotly_report,
)
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


class PerSourceStatsReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.PER_SOURCE_STATS

    def weekly_stats(self, df: pandas.DataFrame) -> go.Figure:
        df['source_subtype'] = df['source_subtype'].fillna('unspecified')
        grouped_by_subtype = df.groupby(['source_subtype'])

        fig = go.Figure()
        for (source_subtype, ), group_df in grouped_by_subtype:
            weekly_counts = group_df.resample('W').size().reset_index(name='count')
            fig.add_trace(
                go.Bar(
                    name=source_subtype,
                    x=weekly_counts['timestamp'],
                    y=weekly_counts['count'],
                )
            )

        setup_default_layout(fig, 'weekly stats')

        fig.update_layout(
            barmode='stack',
        )

        return fig

    def generate_for_source(self, source_name: str, report_path: str, df: pandas.DataFrame):
        df.set_index('timestamp', inplace=True)
        render_plotly_report(
            report_path, [
                self.weekly_stats(df),
            ],
            title=f'source :: {source_name}',
        )

    def generate(self, config: dict, state: StateModel, datasets: Datasets) -> None:
        parent_dir_path = os.path.abspath(read_mandatory(config, 'dir-path'))
        ensure_dir(parent_dir_path)

        activity_df = pandas.DataFrame(datasets.activity)
        activity_df['timestamp'] = pandas.to_datetime(
            activity_df['timestamp'], utc=True)

        grouped_by_source = activity_df.groupby(['source_name'])

        for (source_name, ), source_df in grouped_by_source:
            file_name = sanitize_filename(source_name)
            file_path = '%s.html' % os.path.join(parent_dir_path, file_name)
            LOGGER.info('rendering report for "%s"', source_name)
            self.generate_for_source(source_name, file_path, source_df)
