import logging
import math
import os
import os.path

import textwrap
import pandas
import pandas as pd
import plotly.graph_objects as go

from codoscope.common import (
    NA_REPLACEMENT,
    date_time_minutes_offset,
    ensure_dir_for_path,
)
from codoscope.config import read_mandatory, read_optional
from codoscope.datasets import Datasets
from codoscope.reports.common import (
    ReportBase,
    ReportType,
    render_widgets_report,
    setup_default_layout
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


def limit_text_len(text: str, max_len: int) -> str:
    if len(text) > max_len:
        return text[:max_len - 3] + '...'
    return text


HOVER_TEXT_MAX_ITEM_LEN = 1000
HOVER_TEXT_WRAP_WIDTH = 140


def activity_scatter(
        activity_df: pandas.DataFrame, extended_mode: bool = False) -> go.Figure | None:
    fig = go.Figure()

    title = 'Overview'

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

    # add time of the day fields
    activity_df['time_of_day_minutes_offset'] = activity_df.apply(
        lambda row: date_time_minutes_offset(row['timestamp']), axis=1)
    activity_df['time_of_day'] = activity_df.apply(
        lambda row: format_minutes_offset(row['time_of_day_minutes_offset']), axis=1)

    # initialize for missing authors
    activity_df['author'] = activity_df['author'].fillna(NA_REPLACEMENT)

    # sort for predictable labels order for traces
    activity_df = activity_df.sort_values(
        by=['author', 'source_type', 'source_subtype', 'timestamp'])

    LOGGER.debug('data points to render: %d', len(activity_df))

    if len(activity_df) == 0:
        LOGGER.debug('no data to show')
        return None

    activity_df['source_subtype'] = activity_df['source_subtype'].fillna('')

    grouped_df = activity_df.groupby(['author', 'activity_type'])

    LOGGER.debug('groups count: %s', grouped_df.ngroups)

    # column name to label map
    hover_data_columns_map = {
        'commit_sha': None,
        'bitbucket_pr_title': None,
        'jira_item_key': None,
    }

    if extended_mode:
        hover_data_columns_map['commit_added_lines'] = 'added lines'
        hover_data_columns_map['commit_removed_lines'] = 'removed lines'
        hover_data_columns_map['commit_message'] = 'commit message'
        hover_data_columns_map['jira_summary'] = 'summary'
        # hover_data_columns_map['jira_message'] = 'message'

    for (author, activity_type), df in grouped_df:
        name = '%s %s' % (author, activity_type)

        # compose hover texts all the fields
        texts = []
        for row in df.itertuples():
            text_item = f'<b>{row.source_name}</b><br>'
            for col_name, col_label in hover_data_columns_map.items():
                col_val = getattr(row, col_name, None)
                if col_val and not pd.isna(col_val):
                    if col_label:
                        text_item += '<b>%s</b>: %s<br>' % (col_label, col_val)
                    else:  # no column label
                        text_item += '%s<br>' % col_val
                    text_item = limit_text_len(text_item, HOVER_TEXT_MAX_ITEM_LEN)
                    # split into lines to avoid too long hover text
                    text_item_lines = textwrap.wrap(
                        text_item, break_long_words=False, break_on_hyphens=False,
                        width=HOVER_TEXT_WRAP_WIDTH)
                    text_item = '<br>'.join(text_item_lines)
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


def people_timeline(df: pandas.DataFrame) -> go.Figure:
    df['author'] = df['author'].fillna(NA_REPLACEMENT)

    timestamp_range = [
        df['timestamp'].min(),
        df['timestamp'].max(),
    ]

    fig = go.Figure()

    # add line where start is equal to end timestamp
    fig.add_trace(
        go.Scatter(
            x=timestamp_range,
            y=timestamp_range,
            mode='lines',
            name='n/a',
            showlegend=False,
            opacity=0.4,
            line=dict(
                color='lightgray',
                width=1,
            ),
            hoverinfo='none',
        )
    )

    grouped_by_user = df.sort_values("timestamp", ascending=True).groupby(["author"])

    for (user, ), user_df in grouped_by_user:
        first_timestamp = user_df['timestamp'].min()
        last_timestamp = user_df['timestamp'].max()

        text_atoms = [
            "<b>first:</b> %s<br>" % first_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "<b>last:</b> %s<br>" % last_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        ]

        # compute totals per activity
        user_grouped_by_activity = user_df.groupby(['activity_type'])
        for (activity_type, ), activity_df in user_grouped_by_activity:
            text_atoms.append("<br><b>%s:</b> %d" % (activity_type, len(activity_df)))

        total_size_class = user_df['size_class'].sum()

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
                    ''.join(text_atoms),
                ],
                hovertemplate="%{text}",
                hoverinfo="text+name",
            )
        )

    setup_default_layout(fig, 'People Timeline')

    fig.update_layout(
        xaxis_title='First contribution',
        yaxis_title='Last contribution',
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
        out_path = os.path.abspath(read_mandatory(config, 'out-path'))
        ensure_dir_for_path(out_path)

        # TODO: include somewhere in the report filter and timezone used
        filter_expr = read_optional(config, 'filter')

        activity_df = datasets.activity
        activity_df = convert_timestamp_timezone(activity_df, config.get("timezone"))

        # apply filters if applicable
        if filter_expr:
            count_before_filter = len(activity_df)
            activity_df = apply_filter(activity_df, filter_expr)
            count_after_filter = len(activity_df)
            LOGGER.info(
                'filter "%s" left %d of %d data points',
                filter_expr, count_after_filter, count_before_filter)

        LOGGER.info('total data points: %d', len(activity_df))

        render_widgets_report(
            out_path,
            [
                activity_scatter(activity_df),
                people_timeline(activity_df),
            ],
            title="overview",
        )
