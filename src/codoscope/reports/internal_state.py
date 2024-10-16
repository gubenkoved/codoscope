import collections
import logging
import os
import os.path

from codoscope.common import ensure_dir_for_path
from codoscope.config import read_mandatory
from codoscope.datasets import Datasets
from codoscope.exceptions import ConfigError
from codoscope.reports.common import ReportBase, ReportType
from codoscope.sources.bitbucket import BitbucketState
from codoscope.sources.git import RepoModel
from codoscope.sources.jira import JiraState
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


class InternalStateReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.INTERNAL_STATE

    def generate(self, config: dict, state: StateModel, datasets: Datasets):
        out_path = os.path.abspath(read_mandatory(config, 'out-path'))
        ensure_dir_for_path(out_path)

        with open(out_path, 'w') as f:
            f.write('<html>\n')
            f.write('<head>\n')
            f.write('<title>codoscope :: internal state</title>\n')
            f.write("""
                <link rel="preconnect" href="https://fonts.googleapis.com">
                <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
                <link href="https://fonts.googleapis.com/css2?family=Ubuntu:ital,wght@0,300;0,400;0,500;0,700;1,300;1,400;1,500;1,700&display=swap" rel="stylesheet">
                <link href="https://fonts.googleapis.com/css2?family=Ubuntu+Mono:ital,wght@0,400;0,700;1,400;1,700&family=Ubuntu:ital,wght@0,300;0,400;0,500;0,700;1,300;1,400;1,500;1,700&display=swap" rel="stylesheet">
            """)
            f.write("""
            <style>
                body {
                    font-family: "Ubuntu Mono";
                    padding-left: 20px;
                }
                ul {
                    margin-block-start: 0.2em;
                    margin-block-end: 0.2em;
                }
                h1, h2, h3, h4 {
                    margin-block-start: 0.5em;
                    margin-block-end: 0.2em;
                    border-bottom: black 1px dotted;
                    display: inline-block;
                }
            </style>
            """)
            f.write('</head>\n')
            f.write('<body>\n')

            for state.source_name, source_state in state.sources.items():
                f.write(f'<h2>{state.source_name}</h1>\n')
                f.write('<ul>\n')
                f.write('<li>Type: %s</li>\n' % source_state.source_type.value)
                f.write('<li>Created at: %s</li>\n' % source_state.created_at)
                f.write('<li>Version: %d</li>\n' % source_state.version)

                if isinstance(source_state, RepoModel):
                    f.write('<li>Commits count: %d</li>\n' % source_state.commits_count)
                    authors_count = collections.Counter(
                        x.author_name for x in source_state.commits_map.values()
                    )
                    f.write('<li>Unique authors: %d</li>\n' % len(authors_count))
                elif isinstance(source_state, BitbucketState):
                    f.write('<li>Total repositories count: %d</li>\n' % source_state.repositories_count)
                    f.write('<li>Total PRs count: %d</li>\n' % source_state.pull_requests_count)

                    for project_name, project in source_state.projects_map.items():
                        for repo_name, repo in project.repositories_map.items():
                            f.write(f'<h3>{project_name} :: {repo_name}</h3>\n')
                            f.write('<ul>\n')
                            f.write('<li>Cutoff date: %s</li>\n' % repo.cutoff_date)
                            f.write('<li>PRs count: %d</li>\n' % repo.pull_requests_count)
                            f.write('<li>PRs comments count: %d</li>\n' % repo.pull_requests_comments_count)
                            f.write('</ul>\n')

                elif isinstance(source_state, JiraState):
                    f.write('<li>Cutoff date: %s</li>\n' % source_state.cutoff_date)
                    unique_users_count = collections.Counter(
                        x.creator.account_id for x in source_state.items_map.values()
                    )
                    f.write('<li>Unique users: %d</li>\n' % len(unique_users_count))
                    per_type_count = collections.Counter(
                        x.item_type for x in source_state.items_map.values()
                    )
                    f.write('<li>Total items count: %d</li>\n' % source_state.items_count)
                    total_comments_count = sum(
                        len(x.comments or []) for x in source_state.items_map.values()
                    )
                    f.write('<li>Total comments count: %d</li>\n' % total_comments_count)
                    f.write('<li>Items count by type:</li>\n')
                    f.write('<ul>\n')
                    for item_type, count in per_type_count.most_common():
                        f.write(f'<li>{item_type}: {count}</li>\n')
                    f.write('</ul>\n')
                    f.write('</li>\n')
                else:
                    raise ConfigError(f'Unknown source state type: {source_state}')

                f.write('</ul>\n')

            f.write('</body>\n')
            f.write('</html>\n')
