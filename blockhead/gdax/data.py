"""
File: data.py
Author: Matthew Wright
Email: matt@wrighters.net
Github: https://github.com/wrighter
Description:  Data file handling for historical gdax data
"""

from __future__ import absolute_import

import pathlib
import time
import logging
import requests

from dateutil.tz import tzutc
from dateutil.parser import parse

import gdax
import pandas as pd

from blockhead.util import to_utc
   
def get_bars(pair, start, end, interval,
             directory='output', client=None):
    """ fetches bars from the cache, and if missing gets
    them from gdax and stores them. Assumes that the passed in
    start/end times are in local time, will convert them to UTC.

    pair -- the gdax currency pair
    start -- the start datetime, or a string that will be parsed
    end -- the end datetime, or a string that will be parsed
    interval -- the bar interval in seconds
    directory -- the cache directory
    client -- the gdax client to use to fetch bars if needed
    """

    if isinstance(start, str):
        start = parse(start)
    if isinstance(end, str):
        end = parse(end)
    # convert start and end to UTC
    start = to_utc(start)
    end = to_utc(end)

    # get date range
    dates = pd.date_range(start, end, tz='UTC')
    # iterate through dates and build up all the bars
    path = pathlib.Path(directory, pair, str(interval))
    all_bars = []
    def date_utc(col):
        """ UTC date parser for bars """
        return parse(col, tzinfos=tzutc)

    for date in dates:
        barfile = path / date.strftime('%Y-%m-%d')
        if barfile.exists():
            logging.debug("loading existing bars for %s", date)
            bars = pd.read_csv(barfile.open('r'),
                               parse_dates=[0, 6],
                               index_col=6,
                               date_parser=date_utc)
            bars['close_time'] = bars.index
            all_bars.append(bars)
        else:
            client = client or gdax.PublicClient()
            dstart = max(start, date.replace(hour=0, minute=0, second=0))
            dend = min(end, (date + pd.Timedelta('1d')).replace(hour=0, minute=0, second=0))
            all_bars.append(fetch_bars(pair, dstart, dend, interval, client))
    return pd.concat(all_bars)[start:end]

def fetch_bars(pair, start, end, interval, client=None, batch=350):
    """ fetches bars from GDAX's public api
    pair -- the gdax currency pair
    start -- the start datetime
    end -- the end datetime
    interval -- the bar interval in seconds
    client -- the gdax client to use to fetch bars if needed
    batch -- the batch size to use for fetching
    """
    # convert start and end to UTC
    start = to_utc(start)
    end = to_utc(end)

    full_start = start
    full_end = end
    client = client or gdax.PublicClient()
    sleep_time = 0.5
    all_bars = list()
    complete = start
    while end > complete:
        # work backwards from end_date to start_date by batch size
        try:
            start = end - pd.Timedelta('%ss' % interval) * batch
            bars = fetch_bar_batch(pair, start, end, interval, client)
        except requests.HTTPError as err:
            print(err, type(err.errno), err.errno, err.response.status_code)
            if err.response.status_code == 429:
                sleep_time += 1
                time.sleep(sleep_time)
                continue
            else:
                raise err
        logging.debug("fetched %s bars ending at %s", len(bars), end)
        all_bars.append(bars)
        end = bars.index[0]
        time.sleep(sleep_time)

    all_bars.reverse() # since we went backward in blocks
    bars = pd.concat(all_bars)
    return bars[full_start:full_end]

def fetch_bar_batch(pair, start, end, interval, client=None):
    """ fetches one batch of bars from GDAX's public api
    pair -- the gdax currency pair
    start -- the start datetime
    end -- the end datetime
    interval -- the bar interval in seconds
    client -- the gdax client to use to fetch bars if needed
    """
    client = client or gdax.PublicClient()
    res = client.get_product_historic_rates(pair, start, end, granularity=interval)
    bars = pd.DataFrame(res, columns=['open_time', 'low', 'high', 'open', 'close', 'volume'])
    bars['open_time'] = pd.to_datetime(bars['open_time'], unit='s')
    bars['close_time'] = bars['open_time'].shift()
    first_close = bars.loc[bars.index[0], 'open_time'] + pd.Timedelta('%s' % interval)
    bars.loc[bars.index[0], 'close_time'] = first_close
    bars.index = bars['close_time']
    bars = bars.tz_localize('UTC')
    bars = bars.sort_index()

    return bars
