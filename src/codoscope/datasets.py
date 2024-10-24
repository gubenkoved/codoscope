import logging
import math

import pandas

from codoscope.sources.bitbucket import BitbucketState
from codoscope.sources.git import RepoModel
from codoscope.sources.jira import JiraState
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


class Datasets:
    def __init__(
        self,
        activity: pandas.DataFrame,
        reviews: pandas.DataFrame,
    ) -> None:
        self.activity: pandas.DataFrame = activity
        self.reviews: pandas.DataFrame = reviews

    @classmethod
    def extract(cls, state: StateModel) -> "Datasets":
        return Datasets(
            extract_activity(state),
            extract_reviews(state),
        )


def extract_activity(state: StateModel) -> pandas.DataFrame:
    data = []

    for source_name, source in state.sources.items():
        if isinstance(source, RepoModel):
            for commit in source.commits_map.values():
                data.append(
                    {
                        "source_name": source_name,
                        "source_type": source.source_type.value,
                        "source_subtype": None,
                        "activity_type": "commit",
                        "timestamp": commit.committed_datetime,
                        "user": commit.author_name,
                        "user_email": commit.author_email,
                        "commit_sha": commit.hexsha,
                        "commit_message": commit.message,
                        "commit_added_lines": commit.stats.total_insertions,
                        "commit_removed_lines": commit.stats.total_deletions,
                        "commit_changed_lines": commit.stats.total_changed_lines,
                        "commit_changed_files": list(commit.stats.changed_files),
                        "commit_is_merge_commit": commit.is_merge_commit,
                        "size_class": max(
                            5.0,
                            min(
                                20.0,
                                5 + 3 * math.log(commit.stats.total_changed_lines + 1, 10),
                            ),
                        ),
                    }
                )
        elif isinstance(source, BitbucketState):
            for project_name, project in source.projects_map.items():
                for repo_name, repo in project.repositories_map.items():
                    for pr_name, pr in repo.pull_requests_map.items():
                        data.append(
                            {
                                "source_name": source_name,
                                "source_type": source.source_type.value,
                                "source_subtype": "pr",
                                "activity_type": "pr",
                                "timestamp": pr.created_on,
                                "size_class": 15,
                                "user": (pr.author.display_name if pr.author else None),
                                "bitbucket_pr_title": pr.title,
                                "bitbucket_pr_description": pr.description,
                                "bitbucket_pr_id": pr.id,
                                "bitbucket_project_name": project_name,
                                "bitbucket_repo_name": repo_name,
                            }
                        )
                        for participant in pr.participants or []:
                            if not participant.has_approved:
                                continue
                            data.append(
                                {
                                    "source_name": source_name,
                                    "source_type": source.source_type.value,
                                    "source_subtype": "approved pr",
                                    "activity_type": "approved pr",
                                    "timestamp": participant.participated_on,
                                    "size_class": 8,
                                    "user": (
                                        participant.user.display_name if participant.user else None
                                    ),
                                    "bitbucket_pr_title": pr.title,
                                    "bitbucket_pr_id": pr.id,
                                    "bitbucket_project_name": project_name,
                                    "bitbucket_repo_name": repo_name,
                                }
                            )
                        for comment in pr.commentaries:
                            is_answering_your_own_pr = (
                                comment.author
                                and pr.author
                                and comment.author.account_id == pr.author.account_id
                            )
                            data.append(
                                {
                                    "source_name": source_name,
                                    "source_type": source.source_type.value,
                                    "source_subtype": "comment",
                                    "activity_type": "pr comment",
                                    "timestamp": comment.created_on,
                                    "size_class": (4 if is_answering_your_own_pr else 6),
                                    "user": (
                                        comment.author.display_name if comment.author else None
                                    ),
                                    "bitbucket_is_answering_your_own_pr": is_answering_your_own_pr,
                                    "bitbucket_pr_title": pr.title,
                                    "bitbucket_pr_id": pr.id,
                                    "bitbucket_pr_comment": comment.message,
                                    "bitbucket_project_name": project_name,
                                    "bitbucket_repo_name": repo_name,
                                }
                            )
        elif isinstance(source, JiraState):
            for item in source.items_map.values():
                data.append(
                    {
                        "source_name": source_name,
                        "source_type": source.source_type.value,
                        "source_subtype": item.item_type,
                        "activity_type": "created %s" % item.item_type,
                        "timestamp": item.created_on,
                        "size_class": 8,
                        "user": item.creator.display_name,
                        "user_email": item.creator.email,
                        "jira_item_key": item.key,
                        "jira_description": item.description,
                        "jira_summary": item.summary,
                    }
                )
                for comment in item.comments or []:
                    data.append(
                        {
                            "source_name": source_name,
                            "source_type": source.source_type.value,
                            "source_subtype": "comment",
                            "activity_type": "jira comment",
                            "timestamp": comment.created_on,
                            "size_class": 4,
                            "user": comment.created_by.display_name,
                            "user_email": comment.created_by.email,
                            "jira_item_key": item.key,
                            "jira_message": comment.message,
                        }
                    )
        else:
            LOGGER.warning(
                'skipping source "%s" of type "%s"',
                source_name,
                source.source_type,
            )

    df = pandas.DataFrame(data)

    # note: Int64 can hold NaN values (as opposed to int)
    df["bitbucket_pr_id"] = df["bitbucket_pr_id"].astype("Int64")
    df["commit_added_lines"] = df["commit_added_lines"].astype("Int64")
    df["commit_removed_lines"] = df["commit_removed_lines"].astype("Int64")
    df["commit_changed_lines"] = df["commit_changed_lines"].astype("Int64")

    return df


def extract_reviews(state: StateModel) -> pandas.DataFrame:
    data = []

    for source_name, source in state.sources.items():
        if isinstance(source, BitbucketState):
            for project_name, project in source.projects_map.items():
                for repo_name, repo in project.repositories_map.items():
                    for pr_name, pr in repo.pull_requests_map.items():
                        if not pr.author:
                            continue
                        for pr_participant in pr.participants or []:
                            if not pr_participant.user:
                                continue
                            data.append(
                                {
                                    "source_name": source_name,
                                    "source_type": source.source_type.value,
                                    "reviewer_user": pr_participant.user.display_name,
                                    "reviewee_user": pr.author.display_name,
                                    "is_self_review": pr.author.account_id
                                    == pr_participant.user.account_id,
                                    "has_approved": pr_participant.has_approved,
                                    "timestamp": pr_participant.participated_on,
                                    "bitbucket_pr_title": pr.title,
                                    "bitbucket_pr_id": pr.id,
                                    "bitbucket_project_name": project_name,
                                    "bitbucket_repo_name": repo_name,
                                }
                            )

    df = pandas.DataFrame(data)
    return df
