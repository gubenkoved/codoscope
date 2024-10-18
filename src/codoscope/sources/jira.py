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
    def __init__(self, message: str, created_by: ActorModel, created_on: datetime.datetime):
        self.message: str = message
        self.created_by: ActorModel = created_by
        self.created_on: datetime.datetime = created_on


class JiraItemModel:
    def __init__(
            self,
            id: str,
            key: str,
            item_type: str,
            summary: str,
            description: str,
            status_name: str,
            status_category_name: str,
            creator: ActorModel,
            assignee: ActorModel | None,
            reporter: ActorModel | None,
            components: list[str] | None,
            labels: list[str] | None,
            comments: list[JiraCommentModel] | None,
            created_on: datetime.datetime,
            updated_on: datetime.datetime | None):
        self.id: str = id
        self.key: str = key
        self.item_type: str = item_type
        self.summary: str = summary
        self.description: str = description
        self.status_name: str = status_name
        self.status_category_name: str = status_category_name
        self.creator: ActorModel = creator
        self.assignee: ActorModel | None = assignee
        self.reporter: ActorModel | None = reporter
        self.components: list[str] | None = components
        self.labels: list[str] | None = labels
        self.comments: list[JiraCommentModel] | None = comments
        self.created_on: datetime.datetime | None = created_on
        self.updated_on: datetime.datetime | None = updated_on


class JiraState(SourceState):
    def __init__(self):
        super().__init__()
        self.items_map: dict[str, JiraItemModel] = {}
        # self.users_map: dict[str, ActorModel] = {}
        self.cutoff_date: datetime.datetime | None = None

    @property
    def source_type(self) -> SourceType:
        return SourceType.JIRA

    @property
    def items_count(self):
        return len(self.items_map)

    @property
    def total_comments_count(self):
        return sum(len(x.comments or []) for x in self.items_map.values())


def ingest_jira(config: dict, state: JiraState | None) -> JiraState:
    state = state or JiraState()

    # capture the count before ingestion
    count_before = state.items_count
    count_comments_before = state.total_comments_count

    jira = api.Jira(
        url=config['url'],
        username=config['username'],
        password=config['password'],
    )

    ingestion_counter = 0
    ingestion_limit = config.get('ingestion-limit', math.inf)

    myself = jira.myself()
    my_timezone_name = myself['timeZone']

    LOGGER.debug('timezone set in user profile: %s', my_timezone_name)
    my_timezone = pytz.timezone(my_timezone_name)

    def format_datetime_to_user_tz(datetime: datetime.datetime) -> str:
        local_datetime = datetime.astimezone(my_timezone)
        return local_datetime.strftime('%Y-%m-%d %H:%M')

    # NOTE: Jira JQL API will use user's timezone to interpret the datetime here
    # so in order to make it work properly we need to convert the datetime to
    # that timezone
    def get_query(cutoff_date: datetime) -> str:
        if cutoff_date:
            query = f'Updated >= "{format_datetime_to_user_tz(cutoff_date)}" ORDER BY Updated ASC'
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
                comment['body'],
                convert_actor(comment['author']),
                dateutil.parser.parse(comment['created'])
            )
            for comment in data
        ]

    cutoff_date = state.cutoff_date
    query = get_query(cutoff_date)
    limit = max(10, config.get('jql-query-limit', 100))
    start = 0

    while True:
        response = jira.jql(query, start=start, limit=limit)

        if not response['issues']:
            break

        for issue in response['issues']:
            ingestion_counter += 1
            fields = issue['fields']
            issue_model = JiraItemModel(
                issue['id'],
                issue['key'],
                fields['issuetype']['name'],
                fields['summary'],
                fields['description'],
                fields['status']['name'],
                fields['status']['statusCategory']['name'],
                convert_actor(fields['creator']),
                convert_actor(fields.get('assignee')),
                convert_actor(fields.get('reporter')),
                convert_components(fields.get('components')),
                fields.get('labels'),
                convert_comments(fields.get('comment', {}).get('comments')),
                dateutil.parser.parse(fields['created']),
                dateutil.parser.parse(fields['updated']),
            )
            state.items_map[issue['id']] = issue_model
            cutoff_date = dateutil.parser.parse(issue['fields']['updated'])

        # graceful handling for the last page w/o false-positive warnings
        if response['total'] <= start + len(response['issues']):
            LOGGER.info('last page of items reached')
            break

        # determine next step
        # we prefer to use cutoff based approach for the cases where it is changed
        # after ingesting the page of results to avoid inherent issues with paging
        # over mutable data;
        # if after ingesting the page we still have same cutoff datetime, then
        # use paging approach to get the next page (think of the case where there are
        # tons of items updated during a very short period of time)
        if query != get_query(cutoff_date):
            # prefer cutoff approach (no paging)
            query = get_query(cutoff_date)
            start = 0
            LOGGER.info('advancing JQL filter by cutoff date to %s', cutoff_date)
        else: # use paging approach
            start += len(response['issues'])
            LOGGER.warning(
                'using paging because unable to advance JQL filter by cutoff '
                'date (most likely due to a lot of times changed in a short '
                'period of time around %s)', cutoff_date)

        if ingestion_counter >= ingestion_limit:
            LOGGER.warning('ingestion limit of %d reached', ingestion_limit)
            break

    state.cutoff_date = cutoff_date

    LOGGER.info(
        'ingested %d new items, %d new comments',
        state.items_count - count_before,
        state.total_comments_count - count_comments_before,
    )

    return state
