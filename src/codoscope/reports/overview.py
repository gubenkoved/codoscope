import datetime
import logging
import math
import os
import os.path

import pandas
import pandas as pd
import plotly.graph_objects as go
import pytz
import tzlocal

from codoscope.common import date_time_minutes_offset
from codoscope.reports.base import ReportBase, ReportType
from codoscope.sources.bitbucket import BitbucketState
from codoscope.sources.git import RepoModel
from codoscope.sources.jira import JiraState
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


def setup_default_layout(fig, title=None):
    fig.update_layout(
        title=title,
        title_font_family="Ubuntu",
        # title_font_variant="small-caps",
        font_family="Ubuntu",
        plot_bgcolor='white',
        xaxis=dict(
            showgrid=True,
            gridcolor='lightgray',
            gridwidth=1,
            griddash='dot',
            nticks=30,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='lightgray',
            gridwidth=1,
            griddash='dot',
            nticks=20,
        ),
        shapes=[  # Add an outer border
            dict(
                type="rect",
                xref="paper", yref="paper",  # Reference the entire paper (plot area)
                x0=0, y0=0, x1=1, y1=1,
                line=dict(color="gray", width=1.2)
            )
        ],
    )

    fig.update_layout(
        hoverlabel=dict(
            font_size=12,
            font_family="Ubuntu",
        )
    )


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


# TODO: separate out data extraction pipeline
def activity_scatter(state: StateModel, filter_expr: str | None, timezone_name: str | None):
    data = []

    for source_name, source in state.sources.items():
        if isinstance(source, RepoModel):
            for commit in source.commits_map.values():
                data.append({
                    'source_name': source_name,
                    'source_type': source.source_type.value,
                    'source_subtype': None,
                    'activity_type': 'commit',
                    'timestamp': commit.committed_datetime,
                    'author': commit.author_name,
                    'sha': commit.hexsha,
                    'message': commit.message,
                    'message_first_line': commit.message.split('\n')[0],
                    'changed_lines': commit.stats.total_changed_lines,
                    'size_class': max(5.0, min(20.0, 5 + 3 * math.log(commit.stats.total_changed_lines + 1, 10))),
                    'changed_files': list(commit.stats.changed_files),
                })
        elif isinstance(source, BitbucketState):
            for project_name, project in source.projects_map.items():
                for repo_name, repo in project.repositories_map.items():
                    for pr_name, pr in repo.pull_requests_map.items():
                        data.append({
                            'source_name': source_name,
                            'source_type': source.source_type.value,
                            'source_subtype': 'pr',
                            'activity_type': 'pr',
                            'timestamp': pr.created_on,
                            'size_class': 15,
                            'author': pr.author.display_name if pr.author else None,
                            'pr_title': pr.title,
                            'pr_id': pr.id,
                            'pr_url': pr.url,
                        })
                        for participant in pr.participants or []:
                            if not participant.has_approved:
                                continue
                            data.append({
                                'source_name': source_name,
                                'source_type': source.source_type.value,
                                'source_subtype': 'approved pr',
                                'activity_type': 'approved pr',
                                'timestamp': participant.participated_on,
                                'size_class': 8,
                                'author': participant.user.display_name,
                                'pr_title': pr.title,
                                'pr_id': pr.id,
                                'pr_url': pr.url,
                            })
                        for comment in pr.commentaries:
                            is_answering_your_own_pr = (
                                comment.author and
                                pr.author and
                                comment.author.account_id == pr.author.account_id
                            )
                            data.append({
                                'source_name': source_name,
                                'source_type': source.source_type.value,
                                'source_subtype': 'comment',
                                'activity_type': 'pr comment',
                                'timestamp': comment.created_on,
                                'size_class': 4 if is_answering_your_own_pr else 6,
                                'author': comment.author.display_name,
                                'is_answering_your_own_pr': is_answering_your_own_pr,
                                'pr_title': pr.title,
                                'pr_id': pr.id,
                                'pr_url': pr.url,
                            })
        elif isinstance(source, JiraState):
            for item in source.items_map.values():
                data.append({
                    'source_name': source_name,
                    'source_type': source.source_type.value,
                    'source_subtype': item.item_type,
                    'activity_type': 'created %s' % item.item_type,
                    'timestamp': item.created_on,
                    'size_class': 8,
                    'author': item.creator.display_name,
                    'item_key': item.key,
                })
                for comment in item.comments or []:
                    data.append({
                        'source_name': source_name,
                        'source_type': source.source_type.value,
                        'source_subtype': 'comment',
                        'activity_type': 'jira comment',
                        'timestamp': comment.created_on,
                        'size_class': 4,
                        'author': comment.created_by.display_name,
                        'item_key': item.key,
                    })
        else:
            LOGGER.warning('skipping source "%s" of type "%s"', source_name, source.source_type)

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
        convert_datetime_to_timezone_inplace(data, timezone)

    # add time of the day fields
    for item in data:
        item['time_of_day_minutes_offset'] = date_time_minutes_offset(item['timestamp'])
        item['time_of_day'] = format_minutes_offset(item['time_of_day_minutes_offset'])
        # item['timestamp'] = item['timestamp'].date()

    # initialize for missing authors
    for item in data:
        if item['author'] is None:
            item['author'] = 'Unknown'

    # sort for predictable labels order for traces
    data.sort(key=lambda x: (x['author'], x['source_type'], x['source_subtype'] or '', x['timestamp']))

    # apply filters if applicable
    if filter_expr:
        count_before_filter = len(data)
        data = filter(data, filter_expr)
        count_after_filter = len(data)
        LOGGER.info('filter "%s" left %d of %d data points', filter_expr, count_after_filter, count_before_filter)

    LOGGER.info('data points to render: %d', len(data))

    if len(data) == 0:
        LOGGER.warning('no data to show')
        return fig

    complete_df = pandas.DataFrame(data)
    complete_df['source_subtype'] = complete_df['source_subtype'].fillna('')

    grouped_df = complete_df.groupby(['author', 'activity_type'])

    LOGGER.info('groups count: %s', grouped_df.ngroups)

    hover_data_columns = [
        'sha',
        'pr_title',
        'item_key',
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

    def generate(self, config: dict, state: StateModel):
        out_path = os.path.abspath(config['out-path'])
        if not os.path.exists(os.path.dirname(out_path)):
            os.makedirs(os.path.dirname(out_path))

        filter_expr = config.get('filter')

        figures = [
            activity_scatter(state, filter_expr, config.get('timezone')),
        ]

        local_tz = tzlocal.get_localzone()
        now = datetime.datetime.now(local_tz)
        tz_name = local_tz.tzname(now)

        with open(out_path, 'w') as f:
            f.write('<html>\n')
            f.write('<head>\n')
            f.write('<title>codoscope :: overview</title>\n')
            f.write("""
                <link rel="preconnect" href="https://fonts.googleapis.com">
                <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
                <link href="https://fonts.googleapis.com/css2?family=Ubuntu:ital,wght@0,300;0,400;0,500;0,700;1,300;1,400;1,500;1,700&display=swap" rel="stylesheet">
                <link href="https://fonts.googleapis.com/css2?family=Ubuntu+Mono:ital,wght@0,400;0,700;1,400;1,700&family=Ubuntu:ital,wght@0,300;0,400;0,500;0,700;1,300;1,400;1,500;1,700&display=swap" rel="stylesheet">
            """)
            f.write('<style>body { font-family: "Ubuntu"; }</style>\n')
            f.write('</head>\n')
            f.write('<body>\n')
            for fig in figures:
                f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))
            f.write(
                f"""
                <div style="color: lightgray; font-size: 11px; text-align: right;">
                    <i>last updated on {now.strftime('%B %d, %Y at %H:%M:%S')} {tz_name}</i>\n
                </div>
                """
            )
            f.write('</body>\n')
            f.write('</html>\n')
