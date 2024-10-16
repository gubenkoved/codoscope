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


class PerUserStatsReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.PER_USER_STATS

    def weekly_stats(self, df: pandas.DataFrame) -> go.Figure:
        df['activity_type'] = df['activity_type'].fillna('unspecified')
        grouped = df.groupby(['source_name', 'activity_type'])

        fig = go.Figure()
        for (source_name, activity_type, ), group_df in grouped:
            weekly_counts = group_df.resample('W').size().reset_index(name='count')
            fig.add_trace(
                go.Bar(
                    name=f'{source_name} {activity_type}',
                    x=weekly_counts['timestamp'],
                    y=weekly_counts['count'],
                )
            )

        setup_default_layout(fig, 'weekly stats')

        fig.update_layout(
            barmode='stack',
        )

        return fig

    def generate_for_user(self, user_name: str, report_path: str, df: pandas.DataFrame):
        df.set_index('timestamp', inplace=True)
        render_plotly_report(
            report_path, [
                self.weekly_stats(df),
            ],
            title=f'user :: {user_name}',
        )

    def generate(self, config: dict, state: StateModel, datasets: Datasets) -> None:
        parent_dir_path = os.path.abspath(read_mandatory(config, 'dir-path'))
        ensure_dir(parent_dir_path)

        activity_data_frame = pandas.DataFrame(datasets.activity)
        activity_data_frame['timestamp'] = pandas.to_datetime(
            activity_data_frame['timestamp'], utc=True)

        grouped_by_user = activity_data_frame.groupby(['author'])

        for (user_name, ), user_df in grouped_by_user:
            file_name = sanitize_filename(user_name)
            file_path = '%s.html' % os.path.join(parent_dir_path, file_name)
            LOGGER.info('rendering report for user "%s"', user_name)
            self.generate_for_user(user_name, file_path, user_df)
