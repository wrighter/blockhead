"""
File: util.py
Author: Matthew Wright
Email: matt@wrighters.net
Github: https://github.com/wrighter
Description:  Generic utility functions
"""

import pytz
import tzlocal

def to_local(date):
    """ localizes to local time zone """
    local_timezone = tzlocal.get_localzone()
    return local_timezone.localize(date)

def to_utc(date):
    """ converts the datetime to utc, assumes local timezone,
    will localize offset-naive datetimes to local time """
    if date.tzinfo is None:
        date = to_local(date)
    local_timezone = tzlocal.get_localzone()
    return date.astimezone(pytz.utc)
