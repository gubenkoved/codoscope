import logging
import math

from codoscope.sources.bitbucket import BitbucketState
from codoscope.sources.git import RepoModel
from codoscope.sources.jira import JiraState
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


class Datasets:
    def __init__(self, state: StateModel):
        self.state = state
        self.activity: list[dict] | None = None

    def extract(self):
        self.activity = extract_activity(self.state)


def extract_activity(state: StateModel) -> list[dict]:
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
                    'author_email': commit.author_email,
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
                    'author_email': item.creator.email,
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
                        'author_email': comment.created_by.email,
                        'item_key': item.key,
                    })
        else:
            LOGGER.warning('skipping source "%s" of type "%s"', source_name, source.source_type)

    return data
