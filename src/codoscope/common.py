import logging
import os
import os.path

import jinja2
import pandas
import pathvalidate

LOGGER = logging.getLogger(__name__)


NA_REPLACEMENT = "unspecified"
MODULE_DIR: str = os.path.dirname(__file__)
TEMPLATES_DIR: str = os.path.join(MODULE_DIR, "templates")

WEEKDAY_ORDER = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


def date_time_minutes_offset(datetime):
    time = datetime.time()
    return time.hour * 60 + time.minute


def format_minutes_offset(offset: int) -> str:
    hours = offset // 60
    minutes = offset % 60
    return f"{hours:02}:{minutes:02}"


def sanitize_filename(string: str) -> str:
    """
    Given arbitrary string returns string that is safe to be used
    as filename with forbidden characters replaced/removed.
    """
    return pathvalidate.sanitize_filename(string)


def ensure_dir_for_path(path: str):
    path = os.path.abspath(path)
    dir_path = os.path.dirname(path)
    ensure_dir(dir_path)


def ensure_dir(dir_path: str):
    if not os.path.exists(dir_path):
        LOGGER.info(f'creating directory "{dir_path}"')
        os.makedirs(dir_path)


def convert_timezone(
    df: pandas.DataFrame,
    column_name: str = "timestamp",
    timezone_name: str | None = None,
    inplace: bool = False,
) -> pandas.DataFrame:
    if timezone_name:
        LOGGER.debug(
            'converting "%s" to timezone "%s" (inplace? %s)', column_name, timezone_name, inplace
        )
        if not inplace:
            df = df.copy()
        df[column_name] = pandas.to_datetime(df[column_name], utc=True)
        df[column_name] = df[column_name].dt.tz_convert(timezone_name)
    return df


def apply_filter(
    df: pandas.DataFrame,
    expr: str,
) -> pandas.DataFrame:
    if not expr:
        return df
    count_before_filter = len(df)
    filtered_df = df.query(expr)
    count_after_filter = len(filtered_df)
    LOGGER.info(
        'filter "%s" left %d of %d data points',
        expr,
        count_after_filter,
        count_before_filter,
    )
    return filtered_df


def render_jinja_template(template_name: str, context: dict) -> str:
    jinja_env = jinja2.Environment()
    with open(os.path.join(TEMPLATES_DIR, template_name)) as f:
        template_text = f.read()
        template = jinja_env.from_string(template_text)
        rendered_text = template.render(context)
        return rendered_text
