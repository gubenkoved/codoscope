import argparse
from copyreg import pickle

import pickle
import git
import coloredlogs
import logging
import plotly.express as px
import tzlocal
import os
import os.path
import datetime
import math

LOGGER = logging.getLogger(__name__)


# TODO: include changes files
class CommitStats:
    def __init__(self, insertions: int, deletions: int, files: int):
        self.insertions: int = insertions
        self.deletions: int = deletions
        self.files: int = files

    @property
    def changed_lines(self):
        return self.insertions + self.deletions


class CommitModel:
    def __init__(
            self,
            hexsha: str,
            author_name: str,
            author_email: str,
            committed_datetime: datetime.datetime,
            message: str,
            stats: CommitStats,
            parent_hexsha: list[str],
    ):
        self.hexsha: str = hexsha
        self.author_name: str = author_name
        self.author_email: str = author_email
        self.committed_datetime: datetime.datetime = committed_datetime
        self.message: str = message
        self.stats: CommitStats = stats
        self.parent_hexsha: list[str] = parent_hexsha

    @property
    def is_merge_commit(self):
        return len(self.parent_hexsha) > 1

    @property
    def committed_date_time_minutes_offset(self):
        time = self.committed_datetime.time()
        return time.hour * 60 + time.minute


class RepoModel:
    def __init__(self):
        self.commits_map: dict[str, CommitModel] = {}


def load_state(path: str) -> RepoModel | None:
    LOGGER.info('loading state from "%s"', path)
    try:
        with open(path, 'rb') as f:
            state = pickle.load(f)
            return state
    except Exception as err:
        LOGGER.warning('failed to load state from "%s" due to %s', path, err)
        return None


def save_sate(path: str, state: RepoModel):
    LOGGER.info('saving state to "%s"', path)
    with open(path, 'wb') as f:
        pickle.dump(state, f)


def ingest(path: str, state_path: str, branches: list[str] = None) -> RepoModel:
    repo_model = load_state(state_path) or RepoModel()

    if not branches:
        branches = [
            'master',
        ]

    repo = git.Repo(path)

    LOGGER.info(f'fetching repo...')
    repo.remotes.origin.fetch()

    LOGGER.info(f'iterating branches...')
    commits_counter = 0

    for branch in branches:
        LOGGER.info(f'processing "%s"', branch)
        remote_branch = f'origin/{branch}'
        for commit in repo.iter_commits(remote_branch):
            if commit.hexsha in repo_model.commits_map:
                continue

            commits_counter += 1

            author = '%s (%s)' % (commit.author.name, commit.author.email)
            LOGGER.debug(
                f'  processing commit #%d: %s by "%s" at "%s"',
                commits_counter, commit.hexsha, author, commit.committed_datetime)

            commit_stats = commit.stats.total

            commit_model = CommitModel(
                commit.hexsha,
                commit.author.name,
                commit.author.email,
                commit.committed_datetime,
                commit.message,
                stats=CommitStats(
                    commit_stats['insertions'],
                    commit_stats['deletions'],
                    commit_stats['files'],
                ),
                parent_hexsha=[parent.hexsha for parent in (commit.parents or [])],
            )
            repo_model.commits_map[commit.hexsha] = commit_model

    LOGGER.info(f'ingested commits: %d', commits_counter)

    save_sate(state_path, repo_model)

    return repo_model


def setup_default_layout(fig, title=None):
    fig.update_layout(
        title=title,
        title_font_family="Ubuntu",
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


def plot_commits_scatter(repo_model: RepoModel):
    data = []

    for commit in repo_model.commits_map.values():
        data.append({
            'timestamp': commit.committed_datetime,
            'time_of_day_minutes_offset': commit.committed_date_time_minutes_offset,
            'time_of_day': format_minutes_offset(commit.committed_date_time_minutes_offset),
            'author': commit.author_name,
            'sha': commit.hexsha,
            'message': commit.message,
            'message_first_line': commit.message.split('\n')[0],
            'changed_lines': commit.stats.changed_lines,
            'changed_lines_size_class': max(2.0, min(20.0, 1.5 + 3 * math.log(commit.stats.changed_lines + 1, 10))),
            'changed_files': commit.stats.files,
        })

    data.sort(key=lambda x: (x['author'], x['timestamp']))

    fig = px.scatter(
        x='timestamp',
        y='time_of_day_minutes_offset',
        color='author',
        size='changed_lines_size_class',
        size_max=20,
        hover_data=['timestamp', 'time_of_day', 'author', 'sha', 'changed_lines', 'changed_files', 'message_first_line'],
        data_frame=data,
        opacity=0.9,
    )

    fig.update_traces(
        marker_symbol='circle',
    )

    setup_default_layout(fig, 'All commits')

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


def plot_all(repo_model: RepoModel, out_path: str):
    figures = [
        plot_commits_scatter(repo_model),
    ]

    out_path = os.path.abspath(out_path)
    if not os.path.exists(os.path.dirname(out_path)):
        os.makedirs(os.path.dirname(out_path))

    local_tz = tzlocal.get_localzone()
    now = datetime.datetime.now(local_tz)
    tz_name = local_tz.tzname(now)

    with open(out_path, 'w') as f:
        f.write('<html>\n')
        f.write('<head>\n')
        f.write('<title>Repo stats</title>\n')
        f.write("<link href='http://fonts.googleapis.com/css?family=Ubuntu' rel='stylesheet' type='text/css'>\n")
        f.write('<style>body { font-family: "Ubuntu"; }</style>\n')
        f.write('</head>\n')
        f.write('<body>\n')
        f.write(
            '<i style="color: lightgray; font-size: 11px;">last updated on %s %s, commits %d</i>\n' % (
                now.strftime('%B %d, %Y at %H:%M:%S'), tz_name, len(repo_model.commits_map)))
        for fig in figures:
            f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))
        f.write('</body>\n')
        f.write('</html>\n')


# TODO: allow config to specify both repo path and branches as config for ingestion
#  so that we can aggregate the data and show all the activity in a single place;
#  dash framework can be used to have interactive filters probably... or simple "layers"
#  can be implemented with plotly using separate traces;
#  then we can add other sources and end up with composed activity report per user
#  In config add user remapping like canonical name and then all the aliases nested, then we can merge the data
def main():
    parser = argparse.ArgumentParser(description='Git stats')
    parser.add_argument('--path', type=str, help='Path to the git repository')
    parser.add_argument('--state-path', type=str, required=True, help='Path to the state file')
    parser.add_argument('--log-level', type=str, default='INFO', help='Log level')
    parser.add_argument('--out-path', type=str, required=True, help='Path to file where HTML will be written')
    parser.add_argument('--branch', type=str, action='append', help='Branches to process (allowed multiple times)')

    args = parser.parse_args()

    log_level = args.log_level.upper()
    LOGGER.setLevel(log_level)
    coloredlogs.install(level=log_level)

    repo_model = ingest(
        args.path,
        args.state_path,
        args.branch,
    )

    plot_all(repo_model, args.out_path)


if __name__ == '__main__':
    main()
