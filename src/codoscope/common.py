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
