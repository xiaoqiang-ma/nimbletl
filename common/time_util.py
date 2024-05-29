import datetime


def date_str2date(date_str: str) -> datetime.date:
    return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()


def get_now_date() -> datetime.date:
    return datetime.datetime.now().date()
