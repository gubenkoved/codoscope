import datetime
import logging
import math

import atlassian.jira as api
import dateutil.parser
import pytz

from codoscope.state import SourceState, SourceType

LOGGER = logging.getLogger(__name__)


class ActorModel:
    def __init__(self, account_id: str, display_name: str, email: str | None):
        self.account_id: str = account_id
        self.display_name: str = display_name
        self.email: str | None = email


class JiraCommentModel:
    def __init__(self, created_by: ActorModel, created_on: datetime.datetime):
        self.created_by: ActorModel = created_by
        self.created_on: datetime.datetime = created_on


class JiraItemModel:
    def __init__(
            self,
            id: str,
            key: str,
            item_type: str,
            summary: str,
            status_name: str,
            status_category_name: str,
            creator: ActorModel,
            assignee: ActorModel | None,
            reporter: ActorModel | None,
            components: list[str] | None,
            comments: list[JiraCommentModel] | None,
            created_on: datetime.datetime,
            updated_on: datetime.datetime | None):
        self.id: str = id
        self.key: str = key
        self.item_type: str = item_type
        self.summary: str = summary
        self.status_name: str = status_name
        self.status_category_name: str = status_category_name
        self.creator: ActorModel = creator
        self.assignee: ActorModel | None = assignee
        self.reporter: ActorModel | None = reporter
        self.components: list[str] | None = components
        self.comments: list[JiraCommentModel] | None = comments
        self.created_on: datetime.datetime | None = created_on
        self.updated_on: datetime.datetime | None = updated_on


class JiraState(SourceState):
    def __init__(self):
        super().__init__(SourceType.JIRA)
        self.items_map: dict[str, JiraItemModel] = {}
        # self.users_map: dict[str, ActorModel] = {}
        self.cutoff_date: datetime.datetime | None = None


def ingest_jira(config: dict, state: JiraState | None) -> JiraState:
    state = state or JiraState()

    jira = api.Jira(
        url=config['url'],
        username=config['username'],
        password=config['password'],
    )

    ingestion_counter = 0
    ingestion_limit = config.get('ingestion-limit', math.inf)

    def format_date(datetime: datetime.datetime) -> str:
        utc = datetime.astimezone(pytz.utc)
        return utc.strftime('%Y-%m-%d %H:%M')

    def get_query(cutoff_date: datetime) -> str:
        if cutoff_date:
            query = f'Updated > "{format_date(cutoff_date)}" ORDER BY Updated ASC'
        else:
            query = 'ORDER BY Updated ASC'
        return query

    def convert_actor(data) -> ActorModel | None:
        if not data:
            return None
        return ActorModel(
            data['accountId'],
            data['displayName'],
            data.get('emailAddress')
        )

    def convert_components(data):
        if not data:
            return None
        return [component['name'] for component in data]

    def convert_comments(data) -> list[JiraCommentModel]:
        if not data:
            return []
        return [
            JiraCommentModel(
                convert_actor(comment['author']),
                dateutil.parser.parse(comment['created'])
            )
            for comment in data
        ]

    cutoff_date = state.cutoff_date
    query = get_query(cutoff_date)
    start = 0

    while True:
        response = jira.jql(query, start=start)

        if not response['issues']:
            break

        start += len(response['issues'])

        for issue in response['issues']:
            ingestion_counter += 1
            issue_model = JiraItemModel(
                issue['id'],
                issue['key'],
                issue['fields']['issuetype']['name'],
                issue['fields']['summary'],
                issue['fields']['status']['name'],
                issue['fields']['status']['statusCategory']['name'],
                convert_actor(issue['fields']['creator']),
                convert_actor(issue['fields'].get('assignee')),
                convert_actor(issue['fields'].get('reporter')),
                convert_components(issue['fields'].get('components')),
                convert_comments(issue['fields'].get('comment', {}).get('comments')),
                dateutil.parser.parse(issue['fields']['created']),
                dateutil.parser.parse(issue['fields']['updated']),
            )
            state.items_map[issue['id']] = issue_model
            cutoff_date = dateutil.parser.parse(issue['fields']['updated'])

        if ingestion_counter >= ingestion_limit:
            LOGGER.warning('ingestion limit of %d reached', ingestion_limit)
            break

    state.cutoff_date = cutoff_date

    return state
