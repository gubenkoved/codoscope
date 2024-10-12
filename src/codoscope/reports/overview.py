import datetime
import logging
import math
import os
import os.path

import pandas
import plotly.graph_objects as go
import tzlocal

from codoscope.common import date_time_minutes_offset
from codoscope.sources.bitbucket import BitbucketState
from codoscope.sources.git import RepoModel
from codoscope.state import StateModel, SourceType
from codoscope.reports.base import ReportBase, ReportType

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
            font_family="Ubuntu"
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


# TODO: we need to realign the BitBucket data
#  following might work: find the tz offset for each user by comparing activity distributions
#  and figuring out how to move BitBucket data into Git data so that they align the most?
#  This is purely a heuristic though...
#  ... and this should probably be a separate pre-processing step, not inherent to reports
# TODO: separate out data extraction pipeline
def activity_scatter(state: StateModel, filter_expr: str | None):
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
                    'time_of_day_minutes_offset': commit.committed_date_time_minutes_offset,
                    'time_of_day': format_minutes_offset(commit.committed_date_time_minutes_offset),
                    'author': commit.author_name,
                    'sha': commit.hexsha,
                    'message': commit.message,
                    'message_first_line': commit.message.split('\n')[0],
                    'changed_lines': commit.stats.changed_lines,
                    'size_class': max(5.0, min(20.0, 5 + 3 * math.log(commit.stats.changed_lines + 1, 10))),
                    'changed_files': commit.stats.files,
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
                            'time_of_day_minutes_offset': date_time_minutes_offset(pr.created_on),
                            'time_of_day': format_minutes_offset(date_time_minutes_offset(pr.created_on)),
                            'size_class': 15,
                            'author': pr.author_name,
                        })
                        for comment in pr.commentaries:
                            is_answering_your_own_pr = comment.author_name == pr.author_name
                            data.append({
                                'source_name': source_name,
                                'source_type': source.source_type.value,
                                'source_subtype': 'comment',
                                'activity_type': 'comment',
                                'timestamp': comment.created_on,
                                'time_of_day_minutes_offset': date_time_minutes_offset(comment.created_on),
                                'time_of_day': format_minutes_offset(date_time_minutes_offset(comment.created_on)),
                                'size_class': 4 if is_answering_your_own_pr else 6,
                                'author': comment.author_name,
                                'is_answering_your_own_pr': is_answering_your_own_pr,
                            })
        else:
            LOGGER.warning('skipping source "%s" of type "%s"', source_name, source.source_type)

    fig = go.Figure()

    title = 'Overview'
    if filter_expr:
        title += ' (%s)' % filter_expr

    setup_default_layout(
        fig,
        title=title,
    )

    tickvals, ticktext = time_axis()

    fig.update_layout(
        yaxis=dict(
            title='Time of Day',
            tickmode='array',
            tickvals=tickvals,
            ticktext=ticktext,
        ),
        yaxis_title='Time of the day',
        xaxis_title='Timestamp',
    )

    # sort for predictable labels order for traces
    data.sort(key=lambda x: (x['author'], x['source_type'], x['source_subtype'], x['timestamp']))

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

    for (author, activity_type), df in grouped_df:
        name = '%s %s' % (author, activity_type)
        trace = go.Scattergl(
            name=name,
            showlegend=True,
            x=df['timestamp'],
            y=df['time_of_day_minutes_offset'],
            mode='markers',
            # TODO: handle it
            # hover_data=['source', 'timestamp', 'time_of_day', 'author', 'sha', 'changed_lines', 'changed_files', 'message_first_line'],
            opacity=0.9,
            marker=dict(
                size=df['size_class'],
                # TODO: do we even need it given there will be a color per trace?
                # color=df['author']
            ),
        )
        fig.add_trace(trace)

    return fig


def plot_commits_by_date(repo_model: RepoModel):
    pass


def commit_size_per_author(repo_model: RepoModel):
    # TODO: boxplot
    pass


def commits_by_time_of_day(repo_model: RepoModel):
    pass


# TODO: for each user we can show what he or she mostly changes?
#  Can summarize by (path, count) limiting amount of items for any given parent by some threshold
#  if crossed summarize by parent's parent and so on
def mostly_changed_places(repo_model: RepoModel):
    pass


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
            activity_scatter(state, filter_expr),
        ]

        local_tz = tzlocal.get_localzone()
        now = datetime.datetime.now(local_tz)
        tz_name = local_tz.tzname(now)

        total_commits = 0
        for source in state.sources.values():
            if isinstance(source, RepoModel):
                total_commits += source.commits_count

        with open(out_path, 'w') as f:
            f.write('<html>\n')
            f.write('<head>\n')
            f.write('<title>codoscope :: overview</title>\n')
            f.write("<link href='http://fonts.googleapis.com/css?family=Ubuntu' rel='stylesheet' type='text/css'>\n")
            f.write('<style>body { font-family: "Ubuntu"; }</style>\n')
            f.write('</head>\n')
            f.write('<body>\n')
            f.write(
                f"""
                <div style="color: lightgray; font-size: 11px; position: fixed; bottom: 10px; right: 10px;">
                    <i>last updated on {now.strftime('%B %d, %Y at %H:%M:%S')} {tz_name}, commits {total_commits}</i>\n
                </div>
                """
            )
            for fig in figures:
                f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))
            f.write('</body>\n')
            f.write('</html>\n')
