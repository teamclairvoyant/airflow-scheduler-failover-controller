import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_datetime_as_str(date):
    return date.strftime(TIMESTAMP_FORMAT)


def get_string_as_datetime(date_str):
    return datetime.datetime.strptime(date_str, TIMESTAMP_FORMAT)
