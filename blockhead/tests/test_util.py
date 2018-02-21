import datetime

from blockhead.util import to_utc, to_local

def test_to_utc():
    now = datetime.datetime(2018, 1, 3, 14, 15)
    now1 = to_local(now)
    utc1 = to_utc(now)

    assert now1 - utc1 == datetime.timedelta(0)
    assert utc1.tzname() == 'UTC'
