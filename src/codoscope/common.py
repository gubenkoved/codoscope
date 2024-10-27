import logging
import os
import os.path

import pandas

LOGGER = logging.getLogger(__name__)


NA_REPLACEMENT = "unspecified"


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
    # TODO: implement me
    return string


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
