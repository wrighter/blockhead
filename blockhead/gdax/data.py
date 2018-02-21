"""
File: data.py
Author: Matthew Wright
Email: matt@wrighters.net
Github: https://github.com/wrighter
Description:  Data file handling for historical gdax data
"""

import pathlib
import time
import logging
import requests

import gdax
import pandas as pd

from blockhead.util import to_utc
   
def get_bars(pair, start, end, interval,
             directory='output', client=None):
    """ fetches bars from the cache, and if missing gets
    them from gdax and stores them. Assumes that the passed in
    start/end times are in local time, will convert them to UTC.

    pair -- the gdax currency pair
    start -- the start datetime
    end -- the end datetime
    interval -- the bar interval in seconds
    directory -- the cache directory
    client -- the gdax client to use to fetch bars if needed
    """
    # convert start and end to UTC
    start = to_utc(start)
    end = to_utc(end)

    # get date range
    dates = pd.date_range(start, end) 
    # iterate through dates and build up all the bars
    path = pathlib.Path(directory, pair, interval)
    all_bars = []
    for date in dates:
        barfile = path / date.strftime('%Y-%m-%d')
        if barfile.exists():
            all_bars.append(pd.read_csv(barfile.open('w'), index_col=1))
        else:
            client = client or gdax.PublicClient()
            all_bars.append(fetch_bars)
    return pd.concat[all_bars]

def fetch_bars(pair, start, end, interval, client=None, batch=200):
    """ fetches bars from GDAX's public api
    pair -- the gdax currency pair
    start -- the start datetime
    end -- the end datetime
    interval -- the bar interval in seconds
    client -- the gdax client to use to fetch bars if needed
    batch -- the batch size to use for fetching
    """
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
    return bars

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
    bars = bars.sort_index()

    return bars
