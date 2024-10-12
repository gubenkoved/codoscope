import logging
import os
import os.path
import collections

from codoscope.reports.base import ReportBase, ReportType
from codoscope.sources.bitbucket import BitbucketState
from codoscope.sources.git import RepoModel
from codoscope.sources.jira import JiraState
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


class InternalStateReport(ReportBase):
    @classmethod
    def get_type(cls) -> ReportType:
        return ReportType.INTERNAL_STATE

    def generate(self, config: dict, state: StateModel):
        out_path = os.path.abspath(config['out-path'])
        if not os.path.exists(os.path.dirname(out_path)):
            os.makedirs(os.path.dirname(out_path))

        with open(out_path, 'w') as f:
            f.write('<html>\n')
            f.write('<head>\n')
            f.write('<title>codoscope :: internal state</title>\n')
            f.write("<link href='http://fonts.googleapis.com/css?family=Ubuntu%20Mono' rel='stylesheet' type='text/css'>\n")
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
                h1, h2, h3 {
                    margin-block-start: 0.5em;
                    margin-block-end: 0.2em;
                } 
            </style>
            """)
            f.write('</head>\n')
            f.write('<body>\n')

            for state.source_name, source_state in state.sources.items():
                f.write(f'<h2>{state.source_name}</h1>\n')
                f.write('<ul>\n')
                f.write('<li>Type: %s</li>\n' % source_state.source_type.value)

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
                            f.write(f'<h3>{project_name} :: {repo_name}</h2>')
                            f.write('<ul>\n')
                            f.write('<li>Cutoff date: %s</li>\n' % repo.cutoff_date)
                            f.write('<li>PRs count: %d</li>\n' % repo.pull_requests_count)
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
                    f.write('<ul>\n')
                    for item_type, count in per_type_count.most_common():
                        f.write(f'<li>{item_type}: {count}</li>\n')
                    f.write('</ul>\n')
                    f.write('<li>Total items count: %d</li>\n' % source_state.items_count)
                    total_comments_count = sum(
                        len(x.comments or []) for x in source_state.items_map.values()
                    )
                    f.write('<li>Total comments count: %d</li>\n' % total_comments_count)
                else:
                    raise Exception(f'Unknown source state type: {source_state}')

                f.write('</ul>\n')

            f.write('</body>\n')
            f.write('</html>\n')
