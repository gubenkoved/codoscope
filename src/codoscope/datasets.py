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
        commits_df: pandas.DataFrame,
        bitbucket_df: pandas.DataFrame,
        jira_df: pandas.DataFrame,
        reviews_df: pandas.DataFrame,
    ) -> None:
        self.commits_df: pandas.DataFrame = commits_df
        self.bitbucket_df: pandas.DataFrame = bitbucket_df
        self.jira_df: pandas.DataFrame = jira_df
        self.reviews_df: pandas.DataFrame = reviews_df

    def get_all_activity(self) -> pandas.DataFrame:
        df = pandas.concat(self.get_activity_data_frames().values())
        df.sort_values(by="timestamp", ascending=True, na_position="first", inplace=True)
        return df

    def get_activity_data_frames(self) -> dict[str, pandas.DataFrame]:
        return {
            "commits": self.commits_df,
            "bitbucket": self.bitbucket_df,
            "jira": self.jira_df,
        }

    def get_all_data_frames(self) -> dict[str, pandas.DataFrame]:
        return dict(
            self.get_activity_data_frames(),
            reviews=self.reviews_df,
        )

    @classmethod
    def extract(cls, state: StateModel) -> "Datasets":
        LOGGER.info("extracting datasets...")
        return Datasets(
            commits_df=extract_commits(state),
            bitbucket_df=extract_bitbucket(state),
            jira_df=extract_jira(state),
            reviews_df=extract_reviews(state),
        )


BASE_ACTIVITY_SCHEMA = {
    "source_name": "string",
    "source_type": "string",
    "source_subtype": "string",
    "activity_type": "string",
    # "timestamp": "datetime64[ns]",
    "timestamp": "object",
    "user": "string",
    "user_email": "string",
    "size_class": "int",
}


def extract_commits(state: StateModel) -> pandas.DataFrame:
    schema = dict(
        BASE_ACTIVITY_SCHEMA,
        **{
            "commit_sha": "string",
            "commit_message": "string",
            # note: Int64 can hold NaN values (as opposed to int)
            "commit_added_lines": "Int64",
            "commit_removed_lines": "Int64",
            "commit_changed_lines": "Int64",
            "commit_changed_files_map": "object",
            "commit_is_merge_commit": "bool",
        }
    )

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
                        "commit_changed_files_map": {
                            k: {
                                "added": v.insertions,
                                "deleted": v.deletions,
                            }
                            for k, v in commit.stats.changed_files.items()
                        },
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

    df: pandas.DataFrame = pandas.DataFrame(
        data=data,
        columns=list(schema),
    ).astype(schema)

    df.sort_values(by="timestamp", ascending=True, na_position="first", inplace=True)

    return df


def extract_bitbucket(state: StateModel) -> pandas.DataFrame:
    schema = dict(
        BASE_ACTIVITY_SCHEMA,
        **{
            "bitbucket_project_name": "string",
            "bitbucket_repo_name": "string",
            "bitbucket_pr_title": "string",
            "bitbucket_pr_description": "string",
            "bitbucket_pr_id": "Int64",
            "bitbucket_pr_comment": "string",
        }
    )
    data = []
    for source_name, source in state.sources.items():
        if isinstance(source, BitbucketState):
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
                                # TODO: populate
                                "user_email": None,
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
                                    "user_email": None,
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
                                    "user_email": None,
                                    "bitbucket_is_answering_your_own_pr": is_answering_your_own_pr,
                                    "bitbucket_pr_title": pr.title,
                                    "bitbucket_pr_id": pr.id,
                                    "bitbucket_pr_comment": comment.message,
                                    "bitbucket_project_name": project_name,
                                    "bitbucket_repo_name": repo_name,
                                }
                            )

    df: pandas.DataFrame = pandas.DataFrame(
        data=data,
        columns=list(schema),
    ).astype(schema)

    df.sort_values(by="timestamp", ascending=True, na_position="first", inplace=True)

    return df


def extract_jira(state: StateModel) -> pandas.DataFrame:
    schema = dict(
        BASE_ACTIVITY_SCHEMA,
        **{
            "jira_item_key": "string",
            "jira_description": "string",
            "jira_summary": "string",
            "jira_message": "string",
        }
    )

    data = []
    for source_name, source in state.sources.items():
        if isinstance(source, JiraState):
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

    df: pandas.DataFrame = pandas.DataFrame(
        data=data,
        columns=list(schema),
    ).astype(schema)

    df.sort_values(by="timestamp", ascending=True, na_position="first", inplace=True)

    return df


def set_column_type(df: pandas.DataFrame, column_name: str, data_type: str) -> None:
    if column_name in df.columns:
        df[column_name] = df[column_name].astype(data_type)


def extract_reviews(state: StateModel) -> pandas.DataFrame:
    schema = {
        "source_name": "string",
        "source_type": "string",
        "reviewer_user": "string",
        "reviewee_user": "string",
        "is_self_review": "bool",
        "has_approved": "bool",
        "timestamp": "object",
        "bitbucket_project_name": "string",
        "bitbucket_repo_name": "string",
        "bitbucket_pr_title": "string",
        "bitbucket_pr_id": "Int64",
        "bitbucket_pr_created_date": "object",
    }

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
                                    "bitbucket_project_name": project_name,
                                    "bitbucket_repo_name": repo_name,
                                    "bitbucket_pr_title": pr.title,
                                    "bitbucket_pr_id": pr.id,
                                    "bitbucket_pr_created_date": pr.created_on,
                                }
                            )

    df = pandas.DataFrame(
        data=data,
        columns=list(schema),
    ).astype(schema)

    df.sort_values(
        by=["bitbucket_pr_created_date", "timestamp"],
        ascending=True,
        na_position="first",
        inplace=True,
    )

    return df
