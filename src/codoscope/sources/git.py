import datetime
import fnmatch
import logging
import math

import git

from codoscope.common import date_time_minutes_offset
from codoscope.state import SourceState, SourceType

LOGGER = logging.getLogger(__name__)


class ChangedFileStatModel:
    def __init__(self, insertions: int, deletions: int):
        self.insertions: int = insertions
        self.deletions: int = deletions


class CommitStats:
    def __init__(self, changed_files: dict[str, ChangedFileStatModel]):
        self.changed_files: dict[str, ChangedFileStatModel] = changed_files

    @property
    def total_insertions(self):
        return sum([file_stat.insertions for file_stat in self.changed_files.values()])

    @property
    def total_deletions(self):
        return sum([file_stat.deletions for file_stat in self.changed_files.values()])

    @property
    def total_changed_lines(self):
        return self.total_insertions + self.total_deletions


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
        return date_time_minutes_offset(self.committed_datetime)


class RepoModel(SourceState):
    def __init__(self):
        super().__init__()
        self.commits_map: dict[str, CommitModel] = {}

    @property
    def source_type(self) -> SourceType:
        return SourceType.GIT

    @property
    def commits_count(self):
        return len(self.commits_map)


def ingest_git_repo(
        config: dict,
        repo_state: RepoModel | None,
        path: str,
        branches: list[str] = None,
        ingestion_limit: int | None = None) -> RepoModel:
    repo_state = repo_state or RepoModel()

    repo = git.Repo(path)
    remote_name = config.get('remote', 'origin')
    remote = repo.remote(remote_name)

    LOGGER.info(f'fetching repo...')
    remote.fetch()

    commits_counter = 0

    if branches is None:
        branches = ['master', 'main']

    def is_matching_filters(ref):
        for branch in branches:
            if fnmatch.fnmatch(ref.path, f'refs/remotes/{remote_name}/{branch}'):
                return True
        return False

    ingestion_limit = ingestion_limit or math.inf

    for ref in remote.refs:
        if commits_counter >= ingestion_limit:
            break

        if not is_matching_filters(ref):
            continue

        LOGGER.info(f'processing "%s"', ref.path)

        for commit in repo.iter_commits(ref):
            if commit.hexsha in repo_state.commits_map:
                continue

            if commits_counter >= ingestion_limit:
                LOGGER.warning('  ingestion limit of %d reached', ingestion_limit)
                break

            commits_counter += 1

            author = '%s (%s)' % (commit.author.name, commit.author.email)
            LOGGER.debug(
                f'  processing commit #%d: %s by "%s" at "%s"',
                commits_counter, commit.hexsha, author, commit.committed_datetime)

            changed_files = {
                file_name: ChangedFileStatModel(
                    file_stat['insertions'], file_stat['deletions'])
                for file_name, file_stat in commit.stats.files.items()
            }

            commit_model = CommitModel(
                commit.hexsha,
                commit.author.name,
                commit.author.email,
                commit.committed_datetime,
                commit.message,
                stats=CommitStats(
                    changed_files,
                ),
                parent_hexsha=[parent.hexsha for parent in (commit.parents or [])],
            )
            repo_state.commits_map[commit.hexsha] = commit_model

            if commits_counter % 1000 == 0:
                LOGGER.info(f'  ingested %d commits', commits_counter)

    LOGGER.info(f'ingested %d new commits', commits_counter)

    return repo_state
