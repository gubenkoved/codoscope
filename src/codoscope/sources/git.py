import datetime
import logging

import git

from codoscope.state import SourceState, SourceType

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

class RepoModel(SourceState):
    def __init__(self):
        super().__init__(SourceType.GIT)
        self.commits_map: dict[str, CommitModel] = {}

    @property
    def commits_count(self):
        return len(self.commits_map)


def ingest_git_repo(
        repo_state: RepoModel | None, path: str,
        branches: list[str] = None, ingestion_limit: int | None = None) -> RepoModel:
    repo_state = repo_state or RepoModel()

    repo = git.Repo(path)

    LOGGER.info(f'fetching repo...')
    repo.remotes.origin.fetch()

    LOGGER.info(f'iterating branches...')
    commits_counter = 0

    for branch in branches:
        LOGGER.info(f'processing "%s"', branch)
        remote_branch = f'origin/{branch}'

        for commit in repo.iter_commits(remote_branch):
            if commit.hexsha in repo_state.commits_map:
                continue

            if ingestion_limit is not None and commits_counter >= ingestion_limit:
                LOGGER.warning('ingestion limit of %d reached', ingestion_limit)
                break

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
            repo_state.commits_map[commit.hexsha] = commit_model

    LOGGER.info(f'ingested commits: %d', commits_counter)

    return repo_state
