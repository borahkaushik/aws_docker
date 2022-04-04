from datetime import datetime
from typing import Tuple, Type

def get_date_attributes(date: datetime) -> Tuple[str, str, str]:
    """
    Get year, month, and day in our standard format. Year is a string with full 4 digit form. Month
    and date are both two digit strings where the first digit is 0 if the month or day value is
    less than 10.

    Args:
        date: the :class:`datetime` object to parse.

    Returns:
        A tuple[year, month, day] in standard format.
    """
    month, day = process_date(date.month, date.day)
    return str(date.year), month, day


def process_date(month: int, day: int) -> Tuple[str, str]:
    """
    Processes month and day to convert them into standard format.

    Args:
        month: the month to convert in int format.
        day: the day to convert in int format.

    Returns:
        Tuple[month, day] where month and day are both two digit strings with the first digit 0 if
        the raw value is less than 10.
    """
    std_month: str = '0{}'.format(month) if month < 10 else str(month)
    std_day: str = '0{}'.format(day) if day < 10 else str(day)

    return std_month, std_day

DATE_PARTITION: str = "year={}/month={}/day={}".format(*get_date_attributes(datetime.now()))