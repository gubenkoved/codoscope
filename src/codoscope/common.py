import logging
import os
import os.path

LOGGER = logging.getLogger(__name__)


def date_time_minutes_offset(datetime):
    time = datetime.time()
    return time.hour * 60 + time.minute


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
