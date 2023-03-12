import datetime as dt


class DateTimeParser:
    def __init__(self, year, month, day):
        self.year = year
        self.month = month
        self.day = day

    def parse(self, _time):
        if _time != '24':
            return dt.datetime(self.year, self.month, self.day, hour=int(_time))
        else:
            return dt.datetime(self.year, self.month, self.day, hour=0) + dt.timedelta(days=1)


def datetime_parse(_time, year, month, day):
    if _time != '24':
        return dt.datetime(year, month, day, hour=int(_time))
    else:
        return dt.datetime(year, month, day, hour=0) + dt.timedelta(days=1)