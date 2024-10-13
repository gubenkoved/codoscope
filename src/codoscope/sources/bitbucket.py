import datetime
import logging
import math
from typing import Optional

import atlassian.bitbucket as api
import dateutil.parser

from codoscope.state import SourceState, SourceType

LOGGER = logging.getLogger(__name__)


class CommentModel:
    def __init__(
            self, author_name: str | None, message: str | None,
            created_on: datetime.datetime | None):
        self.author_name: str | None = author_name
        self.message: str | None = message
        self.created_on: datetime.datetime | None = created_on


class PullRequestParticipantModel:
    def __init__(
            self, participant_name: str | None, display_name: str | None,
            has_approved: bool | None, participated_on: datetime.datetime | None):
        self.participant_name: str | None = participant_name
        self.display_name: str | None = display_name
        self.has_approved: bool | None = has_approved
        self.participated_on: datetime.datetime | None = participated_on


class PullRequestModel:
    def __init__(
            self,
            id: str,
            url: str,
            author_name: str | None,
            title: str | None,
            description: str | None,
            source_branch: str | None, destination_branch: str | None,
            state: str | None,
            participants: list[PullRequestParticipantModel] | None,
            commentaries: list[CommentModel],
            created_on: datetime.datetime,
            updated_on: datetime.datetime
    ):
        self.id: str = id
        self.url: str = url
        self.author_name: str | None = author_name
        self.title: str | None = title
        self.description: str | None = description
        self.source_branch: str | None = source_branch
        self.destination_branch: str | None = destination_branch
        self.commentaries: list[CommentModel] = commentaries
        self.state: str | None = state
        self.participants: list[PullRequestParticipantModel] | None = participants
        self.created_on: datetime.datetime = created_on
        self.updated_on: datetime.datetime = updated_on
        self.meta_version: int | None = 1


class RepositoryModel:
    def __init__(self):
        self.created_on: datetime.datetime | None = None
        self.pull_requests_map: dict[str, PullRequestModel] = {}
        # ingestion cutoff date
        self.cutoff_date: datetime.datetime | None = None

    @property
    def pull_requests_count(self):
        return len(self.pull_requests_map)

    @property
    def pull_requests_comments_count(self):
        return sum(len(pr.commentaries) for pr in self.pull_requests_map.values())


class ProjectModel:
    def __init__(self):
        self.repositories_map: dict[str, RepositoryModel] = {}

    @property
    def repositories_count(self):
        return len(self.repositories_map)

    @property
    def pull_requests_count(self):
        return sum(repo.pull_requests_count for repo in self.repositories_map.values())


class BitbucketState(SourceState):
    def __init__(self):
        super().__init__(SourceType.BITBUCKET)
        self.projects_map: dict[str, ProjectModel] = {}

    @property
    def projects_count(self):
        return len(self.projects_map)

    @property
    def repositories_count(self):
        return sum(project.repositories_count for project in self.projects_map.values())

    @property
    def pull_requests_count(self):
        return sum(project.pull_requests_count for project in self.projects_map.values())


def safe_get(what, fn, default=None):
    try:
        return fn()
    except Exception as err:
        LOGGER.warning('unable to get "%s" due to %s', what, err)
        return default


DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def format_datetime(x):
    if not x:
        return None
    return x.strftime(DATETIME_FORMAT)


def parse_datetime(s: str) -> Optional[datetime.datetime]:
    if not s:
        return None
    return datetime.datetime.strptime(s, DATETIME_FORMAT)


def total_pr_comments_count(state: BitbucketState) -> int:
    return sum(
        len(pr.commentaries) for project in state.projects_map.values()
        for repo in project.repositories_map.values()
        for pr in repo.pull_requests_map.values()
    )


def ingest_bitbucket(config: dict, state: BitbucketState | None) -> BitbucketState:
    state = state or BitbucketState()

    prs_count_before = state.pull_requests_count
    prs_comments_count_before = total_pr_comments_count(state)

    bitbucket = api.Cloud(
        url=config['url'],
        username=config['username'],
        password=config['password'],
    )

    workspace = bitbucket.workspaces.get(config['workspace'])

    ingestion_limit = config.get('ingestion-limit', math.inf)
    ingestion_counter = 0

    for config_project in config['projects']:
        if ingestion_counter >= ingestion_limit:
            break

        project_name = config_project['name']
        project = workspace.projects.get(project_name)
        project_state = state.projects_map.setdefault(project_name, ProjectModel())

        for config_repo in config_project['repositories']:
            if ingestion_counter >= ingestion_limit:
                break

            repo_name = config_repo['name']
            LOGGER.info('ingesting repository "%s" (project "%s")', repo_name, project_name)

            repo = project.repositories.get(repo_name)
            repo_state = project_state.repositories_map.setdefault(repo_name, RepositoryModel())

            repo_state.crated_on = repo.created_on
            repo_state.url = repo.url

            # by default only open PRs are returned
            query = '(state="MERGED" or state="OPEN" or state="DECLINED" or state="SUPERSEDED")'
            if repo_state.cutoff_date:
                query += ' and updated_on > %s' % format_datetime(repo_state.cutoff_date)

            for pr in repo.pullrequests.each(
                    q=query,
                    sort='updated_on',
            ):
                pr_author = safe_get('author', lambda: pr.author)
                author_name = pr_author.display_name if pr_author else None

                LOGGER.debug('  processing "%s" which is created on %s by %s', pr.url, pr.created_on, author_name or '???')

                repo_state.cutoff_date = pr.updated_on

                pr_participants = []
                for participant in pr.participants():
                    participant_name = participant.user.nickname
                    display_name = participant.user.display_name
                    has_approved = participant.has_approved
                    participated_on = participant.participated_on
                    pr_participant = PullRequestParticipantModel(
                        participant_name,
                        display_name,
                        has_approved,
                        participated_on
                    )
                    pr_participants.append(pr_participant)

                pr_comments = []
                for comment in pr.comments():
                    pr_comments.append(CommentModel(
                        comment.user.display_name,
                        comment.raw,
                        dateutil.parser.parse(comment.data['created_on']),
                    ))

                pr_model = PullRequestModel(
                    pr.id,
                    pr.url,
                    author_name,
                    pr.title,
                    pr.description,
                    pr.source_branch,
                    pr.destination_branch,
                    pr.data.get('state'),
                    pr_participants,
                    pr_comments,
                    pr.created_on,
                    pr.updated_on
                )
                repo_state.pull_requests_map[pr.id] = pr_model

                # check if we reached the ingestion limit
                ingestion_counter += 1
                if ingestion_counter >= ingestion_limit:
                    LOGGER.warning('ingestion limit of %d reached', ingestion_limit)
                    break

    LOGGER.info(
        'ingested %d new PRs and %d new PR comments',
        state.pull_requests_count - prs_count_before,
        total_pr_comments_count(state) - prs_comments_count_before
    )

    return state
