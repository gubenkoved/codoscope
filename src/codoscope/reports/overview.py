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


def convert_datetime_to_timezone_inplace(data: list[dict], timezone) -> None:
    for item in data:
        for prop in item:
            if isinstance(item[prop], datetime.datetime):
                item[prop] = item[prop].astimezone(timezone)


def activity_scatter(
        activity_data: list[dict],
        filter_expr: str | None, timezone_name: str | None) -> go.Figure:
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
        LOGGER.info('converting timestamps to timezone "%s"', timezone_name)
        timezone = pytz.timezone(timezone_name)
        convert_datetime_to_timezone_inplace(activity_data, timezone)

    # add time of the day fields
    for item in activity_data:
        item['time_of_day_minutes_offset'] = date_time_minutes_offset(item['timestamp'])
        item['time_of_day'] = format_minutes_offset(item['time_of_day_minutes_offset'])

    # initialize for missing authors
    for item in activity_data:
        if item['author'] is None:
            item['author'] = 'Unknown'

    # sort for predictable labels order for traces
    activity_data.sort(
        key=lambda x: (x['author'], x['source_type'], x['source_subtype'] or '', x['timestamp']))

    # apply filters if applicable
    if filter_expr:
        count_before_filter = len(activity_data)
        data = filter(activity_data, filter_expr)
        count_after_filter = len(data)
        LOGGER.info('filter "%s" left %d of %d data points', filter_expr, count_after_filter, count_before_filter)

    LOGGER.info('data points to render: %d', len(activity_data))

    if len(activity_data) == 0:
        LOGGER.warning('no data to show')
        return fig

    complete_df = pandas.DataFrame(activity_data)
    complete_df['source_subtype'] = complete_df['source_subtype'].fillna('')

    grouped_df = complete_df.groupby(['author', 'activity_type'])

    LOGGER.info('groups count: %s', grouped_df.ngroups)

    hover_data_columns = [
        'commit_sha',
        'bitbucket_pr_title',
        'bitbucket_item_key',
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


def filter(data: list[dict], expr: str):
    compiled = compile(expr, 'filter', 'eval')
    return [x for x in data if eval(compiled, x)]


class OverviewReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.OVERVIEW

    def generate(self, config: dict, state: StateModel, datasets: Datasets):
        out_path = os.path.abspath(read_mandatory(config, 'out-path'))
        ensure_dir_for_path(out_path)

        filter_expr = read_optional(config, 'filter')

        render_widgets_report(
            out_path,
            [
                activity_scatter(
                    datasets.activity, filter_expr, config.get("timezone")
                ),
            ],
            title="overview",
        )
