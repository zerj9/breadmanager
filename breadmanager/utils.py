from datetime import datetime, timedelta


def date_range_generator(start: datetime, end: datetime, interval: timedelta):
    current = start
    while current < end:
        yield current
        current += interval

    if current > end:
        yield end
    else:
        yield current
