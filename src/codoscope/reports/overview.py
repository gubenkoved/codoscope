import datetime
import logging
import os
import os.path

import pandas
import pandas as pd
import plotly.graph_objects as go
import pytz

from codoscope.common import date_time_minutes_offset, ensure_dir_for_path
from codoscope.config import read_mandatory, read_optional
from codoscope.datasets import Datasets
from codoscope.reports.common import (
    ReportBase,
    ReportType,
    render_widgets_report,
    setup_default_layout,
)
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


def time_axis(steps=24):
    valess = []
    labels = []

    for offset in range(0, 24 * 60 + 1, 24 * 60 // steps):
        hours = offset // 60
        minutes = offset % 60
        valess.append(offset)
        labels.append(f'{hours:02}:{minutes:02}')

    return valess, labels


def format_minutes_offset(offset: int):
    hours = offset // 60
    minutes = offset % 60
    return f'{hours:02}:{minutes:02}'


def activity_scatter(
        activity_df: pandas.DataFrame,
        filter_expr: str | None = None,
        timezone_name: str | None = None) -> go.Figure | None:
    fig = go.Figure()

    title = 'Overview'
    title_extra = []

    if filter_expr:
        title_extra.append('filtered by "%s"' % filter_expr)

    if timezone_name:
        title_extra.append('timezone normalized to "%s"' % timezone_name)

    if title_extra:
        title += ' (%s)' % ', '.join(title_extra)

    setup_default_layout(
        fig,
        title=title,
    )

    tickvals, ticktext = time_axis()

    fig.update_layout(
        yaxis=dict(
            title='Time',
            tickmode='array',
            tickvals=tickvals,
            ticktext=ticktext,
        ),
        xaxis_title='Timestamp',
    )

    if timezone_name:
        LOGGER.debug('converting timestamps to timezone "%s"', timezone_name)
        activity_df['timestamp'] = pandas.to_datetime(activity_df['timestamp'], utc=True)
        activity_df['timestamp'] = activity_df['timestamp'].dt.tz_convert(timezone_name)

    # add time of the day fields
    activity_df['time_of_day_minutes_offset'] = activity_df.apply(
        lambda row: date_time_minutes_offset(row['timestamp']), axis=1)
    activity_df['time_of_day'] = activity_df.apply(
        lambda row: format_minutes_offset(row['time_of_day_minutes_offset']), axis=1)

    # initialize for missing authors
    activity_df['author'] = activity_df['author'].fillna('Unknown')

    # sort for predictable labels order for traces
    activity_df = activity_df.sort_values(
        by=['author', 'source_type', 'source_subtype', 'timestamp'])

    # apply filters if applicable
    if filter_expr:
        count_before_filter = len(activity_df)
        activity_df = apply_filter(activity_df, filter_expr)
        count_after_filter = len(activity_df)
        LOGGER.info(
            'filter "%s" left %d of %d data points',
            filter_expr, count_after_filter, count_before_filter)

    LOGGER.debug('data points to render: %d', len(activity_df))

    if len(activity_df) == 0:
        LOGGER.debug('no data to show')
        return None

    activity_df['source_subtype'] = activity_df['source_subtype'].fillna('')

    grouped_df = activity_df.groupby(['author', 'activity_type'])

    LOGGER.debug('groups count: %s', grouped_df.ngroups)

    hover_data_columns = [
        'commit_sha',
        'bitbucket_pr_title',
        'jira_item_key',
    ]

    for (author, activity_type), df in grouped_df:
        name = '%s %s' % (author, activity_type)

        # compose hover texts all the fields
        texts = []
        for row in df.itertuples():
            text_item = f'<b>{row.source_name}</b><br>'
            for col in hover_data_columns:
                col_val = getattr(row, col, None)
                if col_val and not pd.isna(col_val):
                    text_item += '%s<br>' % col_val
            texts.append(text_item)

        trace = go.Scattergl(
            name=name,
            showlegend=True,
            x=df['timestamp'],
            y=df['time_of_day_minutes_offset'],
            mode='markers',
            text=texts,
            hovertemplate='%{x}<br>%{text}',
            opacity=0.9,
            marker=dict(
                size=df['size_class'],
            ),
        )
        fig.add_trace(trace)

    return fig


def apply_filter(df: pandas.DataFrame, expr: str) -> pandas.DataFrame:
    local_vars = {col: df[col] for col in df.columns}
    filtered_df = df.eval(expr, local_dict=local_vars)
    return df[filtered_df]


class OverviewReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.OVERVIEW

    def generate(self, config: dict, state: StateModel, datasets: Datasets):
        out_path = os.path.abspath(read_mandatory(config, 'out-path'))
        ensure_dir_for_path(out_path)

        filter_expr = read_optional(config, 'filter')

        activity_df = pd.DataFrame(datasets.activity)

        LOGGER.info('total data points: %d', len(activity_df))

        render_widgets_report(
            out_path,
            [
                activity_scatter(
                    activity_df, filter_expr, config.get("timezone")
                ),
            ],
            title="overview",
        )
