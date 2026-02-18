import datetime
import logging
import math

import atlassian.jira as api
import dateutil.parser
import pytz

from codoscope.config import read_optional
from codoscope.state import SourceState, SourceType, VersionedState

LOGGER = logging.getLogger(__name__)


class ActorModel(VersionedState):
    def __init__(
        self,
        account_id: str,
        display_name: str,
        email: str | None,
    ):
        super().__init__()
        self.account_id: str = account_id
        self.display_name: str = display_name
        self.email: str | None = email


class UserModel(VersionedState):
    def __init__(
        self,
        account_id: str,
        display_name: str | None,
        email: str | None,
        is_active: bool,
        account_type: str | None,
    ):
        super().__init__()
        self.account_id: str = account_id
        self.display_name: str | None = display_name
        self.email: str | None = email
        self.is_active: bool = is_active
        self.account_type: str | None = account_type


class JiraCommentModel(VersionedState):
    def __init__(
        self,
        comment_id: str,
        message: str,
        created_by: ActorModel,
        created_on: datetime.datetime,
    ):
        super().__init__()
        self.comment_id: str = comment_id
        self.message: str = message
        self.created_by: ActorModel = created_by
        self.created_on: datetime.datetime = created_on


class JiraChangeLogItemModel(VersionedState):
    def __init__(
        self,
        actor: ActorModel | None,
        created_on: datetime.datetime,
        field: str,
        from_value: str | None,
        to_value: str | None,
    ):
        super().__init__()
        self.actor: ActorModel = actor
        self.created_on: datetime.datetime = created_on
        self.field: str = field
        self.from_value: str | None = from_value
        self.to_value: str | None = to_value


class JiraItemModel(VersionedState):
    def __init__(
        self,
        id: str,
        key: str,
        item_type: str,
        summary: str,
        description: str | None,
        status_name: str,
        status_category_name: str,
        creator: ActorModel,
        assignee: ActorModel | None,
        reporter: ActorModel | None,
        components: list[str] | None,
        labels: list[str] | None,
        comments: list[JiraCommentModel] | None,
        change_log: list[JiraChangeLogItemModel] | None,
        created_on: datetime.datetime,
        updated_on: datetime.datetime | None,
    ):
        super().__init__()
        self.id: str = id
        self.key: str = key
        self.item_type: str = item_type
        self.summary: str = summary
        self.description: str | None = description
        self.status_name: str = status_name
        self.status_category_name: str = status_category_name
        self.creator: ActorModel = creator
        self.assignee: ActorModel | None = assignee
        self.reporter: ActorModel | None = reporter
        self.components: list[str] | None = components
        self.labels: list[str] | None = labels
        self.comments: list[JiraCommentModel] | None = comments
        self.change_log: list[JiraChangeLogItemModel] | None = change_log
        self.created_on: datetime.datetime | None = created_on
        self.updated_on: datetime.datetime | None = updated_on


class JiraState(SourceState):
    def __init__(self):
        super().__init__()
        self.items_map: dict[str, JiraItemModel] = {}
        self.cutoff_date: datetime.datetime | None = None
        # maintains map from account ID to user for discovered users
        self.users_map: dict[str, UserModel] = {}
        self.users_refresh_date: datetime.datetime | None = None
        self.version = 2

    @property
    def source_type(self) -> SourceType:
        return SourceType.JIRA

    @property
    def items_count(self):
        return len(self.items_map)

    @property
    def total_comments_count(self):
        return sum(len(x.comments or []) for x in self.items_map.values())

    def __setstate__(self, state):
        # initialize the new property
        if getattr(state, "version", 1) == 1:
            self.users_refresh_date = None

        self.__dict__.update(state)


# constant used by the BitBucket API when returning comments inline with the
# issue data; when there are more comments, we need to fetch them separately
DEFAULT_COMMENTS_LIMIT = 100

# values more than 100 do not seem to work
COMMENTS_PAGE_SIZE = 100

DEFAULT_USER_REFRESH_INTERVAL_DAYS = 7.0


# after recent API changes JIRA only returns 100 comments by default and current
# python API does not expose a way to get all comments for an issue
def _get_all_comments(jira: api.Jira, issue_id: str) -> list[dict]:
    offset = 0
    result = []
    while True:
        url = "{base_url}/{issue_id}/comment".format(
            base_url=jira.resource_url("issue"),
            issue_id=issue_id,
        )
        response = jira.get(
            url,
            params={
                "startAt": offset,
                "maxResults": COMMENTS_PAGE_SIZE,
            },
        )
        comments_page = response["comments"]
        result.extend(comments_page)
        offset += len(comments_page)

        # stop if we do not have full page (use effective page size)
        if len(comments_page) < response["maxResults"]:
            break

    return result


def __populate_users_map(state: JiraState, jira_api: api.Jira, page_size: int = 1000):
    offset = 0
    while True:
        page = jira_api.users_get_all(start=offset, limit=page_size)
        offset += len(page)

        for user in page:
            user_model = UserModel(
                user["accountId"],
                user.get("displayName"),
                user.get("emailAddress"),
                user["active"],
                user.get("accountType"),
            )
            state.users_map[user_model.account_id] = user_model

        # last page reached
        if len(page) < page_size:
            break


def ingest_jira(config: dict, state: JiraState | None) -> JiraState:
    state = state or JiraState()

    # capture the count before ingestion
    count_before = state.items_count
    count_comments_before = state.total_comments_count

    is_cloud = config.get("cloud", True)

    jira = api.Jira(
        url=config["url"],
        username=config["username"],
        password=config["password"],
        cloud=is_cloud,
    )

    ingestion_counter = 0
    ingestion_limit = config.get("ingestion-limit", math.inf)

    myself = jira.myself()
    my_timezone_name = myself["timeZone"]

    LOGGER.debug("timezone set in user profile: %s", my_timezone_name)
    my_timezone = pytz.timezone(my_timezone_name)

    def format_datetime_to_user_tz(dt: datetime.datetime) -> str:
        local_datetime = dt.astimezone(my_timezone)
        return local_datetime.strftime("%Y-%m-%d %H:%M")

    # NOTE: Jira JQL API will use user's timezone to interpret the datetime here
    # so in order to make it work properly we need to convert the datetime to
    # that timezone
    def get_query(cutoff_date: datetime.datetime) -> str:
        if cutoff_date:
            query = f'Updated >= "{format_datetime_to_user_tz(cutoff_date)}" ORDER BY Updated ASC'
        else:
            query = "ORDER BY Updated ASC"
        return query

    def convert_actor(data) -> ActorModel | None:
        if not data:
            return None
        return ActorModel(
            account_id=data["accountId"],
            display_name=data["displayName"],
            email=data.get("emailAddress"),
        )

    def convert_components(data):
        if not data:
            return None
        return [component["name"] for component in data]

    def convert_comments(data: list[dict]) -> list[JiraCommentModel]:
        if not data:
            return []
        return [
            JiraCommentModel(
                comment_id=comment["id"],
                message=comment["body"],
                created_by=convert_actor(comment["author"]),
                created_on=dateutil.parser.parse(comment["created"]),
            )
            for comment in data
        ]

    def replace_empty_string(string: str | None) -> str | None:
        if not string:
            return None
        return string

    def convert_change_log(data, included_fields) -> list[JiraChangeLogItemModel]:
        if not data:
            return []
        change_log = []
        for history_item in data["histories"]:
            for subitem in history_item["items"]:
                if subitem["field"] not in included_fields:
                    continue
                change_log.append(
                    JiraChangeLogItemModel(
                        actor=convert_actor(history_item.get("author")),
                        created_on=dateutil.parser.parse(history_item["created"]),
                        field=subitem["field"],
                        from_value=replace_empty_string(subitem.get("fromString")),
                        to_value=replace_empty_string(subitem.get("toString")),
                    )
                )
        return change_log

    cutoff_date = state.cutoff_date

    if config.get("cutoff-date"):
        # YAML has built-in support for date and datetime types
        cutoff_date = config["cutoff-date"]
        if isinstance(cutoff_date, datetime.date):
            cutoff_date = datetime.datetime.combine(cutoff_date, datetime.time.min)
        LOGGER.warning('overriding cutoff date with "%s"', cutoff_date)

    query = get_query(cutoff_date)
    limit = max(10, config.get("jql-query-limit", 100))
    start = 0
    next_page_token = None

    while True:
        if is_cloud:
            response = jira.enhanced_jql(
                query,
                limit=limit,
                expand="changelog,comments",
                nextPageToken=next_page_token,
            )
        else:
            response = jira.jql(
                query,
                start=start,
                limit=limit,
                expand="changelog,comments",
            )

        if not response["issues"]:
            break

        for issue in response["issues"]:
            ingestion_counter += 1
            fields = issue["fields"]
            issue_id = issue["id"]
            comments_data = fields.get("comment", {})
            comments = comments_data.get("comments")

            if len(comments) < comments_data["total"]:
                LOGGER.debug(
                    "issue %s seems to have more comments (%d) than inlined (%d), "
                    "fetching separately",
                    issue_id,
                    comments_data["total"],
                    len(comments),
                )
                comments = _get_all_comments(jira, issue_id)

            issue_model = JiraItemModel(
                issue_id,
                issue["key"],
                fields["issuetype"]["name"],
                fields.get("summary"),
                fields.get("description"),
                fields["status"]["name"],
                fields["status"]["statusCategory"]["name"],
                convert_actor(fields["creator"]),
                convert_actor(fields.get("assignee")),
                convert_actor(fields.get("reporter")),
                convert_components(fields.get("components")),
                fields.get("labels"),
                convert_comments(comments),
                # TODO: load change log using paging as well, as the list is
                #  truncated in this API
                convert_change_log(issue.get("changelog"), included_fields=["status"]),
                dateutil.parser.parse(fields["created"]),
                dateutil.parser.parse(fields["updated"]),
            )
            state.items_map[issue["id"]] = issue_model
            cutoff_date = dateutil.parser.parse(issue["fields"]["updated"])

        # contrary to https://docs.atlassian.com/software/jira/docs/api/REST/9.17.0/#api/2/search-search
        # the response does NOT contain "total" field, but it has "isLast" marker
        if "isLast" in response:
            is_last_page = response["isLast"]
        else:
            is_last_page = response["total"] <= start + len(response["issues"])

        # graceful handling for the last page w/o false-positive warnings
        if is_last_page:
            LOGGER.info("last page of items reached")
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
            next_page_token = None
            LOGGER.info("advancing JQL filter by cutoff date to %s", cutoff_date)
        else:  # use paging approach if we can not advance the query itself
            start += len(response["issues"])
            next_page_token = response["nextPageToken"]
            LOGGER.warning(
                "using paging because unable to advance JQL filter by cutoff "
                "date (most likely due to a lot of items changed in a short "
                "period of time around %s)",
                cutoff_date,
            )

        if ingestion_counter >= ingestion_limit:
            LOGGER.warning("ingestion limit of %d reached", ingestion_limit)
            break

    state.cutoff_date = cutoff_date

    LOGGER.info(
        "ingested %d new items, %d new comments",
        state.items_count - count_before,
        state.total_comments_count - count_comments_before,
    )

    try:
        refresh_interval_days = read_optional(
            config, "users-refresh-interval-days", DEFAULT_USER_REFRESH_INTERVAL_DAYS
        )
        refresh_interval_seconds = refresh_interval_days * 24 * 60 * 60
        now = datetime.datetime.now()
        seconds_since_refresh = (
            (now - state.users_refresh_date).total_seconds()
            if state.users_refresh_date
            else float("+inf")
        )

        if seconds_since_refresh >= refresh_interval_seconds:
            LOGGER.info("updating users map...")
            __populate_users_map(state, jira)
            state.users_refresh_date = now
    except Exception as err:
        LOGGER.warning('unable to update users map due to "%s"', err)

    return state
