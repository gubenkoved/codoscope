def date_time_minutes_offset(datetime):
    time = datetime.time()
    return time.hour * 60 + time.minute
