#!/usr/bin/env python
"""
File: fetch_bars.py
Author: Matthew Wright
Email: matt@wrighters.net
Github: https://github.com/wrighter
Description: script to fetch bars for a currency pair from GDAX
over a date range
"""

import argparse
import logging
import sys
import pathlib
import datetime
import time
import requests
import gdax

import pandas as pd
from dateutil import parser

from blockhead.gdax import data
from blockhead.gdax.ws_client import GDAXWebsocketClient

def main(args):
    """ the main function """
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-o', '--output_dir', default='output', help='Directory to save file')
    argparser.add_argument('-d', '--debug', help='enable debug logging', action='store_true')
    argparser.add_argument('-i', '--interval', default=60, help='bar interval in seconds')
    argparser.add_argument('-q', '--quantity', default=350,
                           help='number of bars to fetch at a time')
    argparser.add_argument("--start_date",
                           default=(datetime.datetime.utcnow() - datetime.timedelta(days=1)),
                           help="start datetime")
    argparser.add_argument("--end_date", default=datetime.datetime.utcnow(),
                           help="end datetime")
    argparser.add_argument('pair', type=str, help='The currency pair')

    args = argparser.parse_args(sys.argv[1:])
    if args.debug:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.debug(args)

    if isinstance(args.start_date, str):
        args.start_date = parser.parse(args.start_date)
    if isinstance(args.end_date, str):
        args.end_date = parser.parse(args.end_date)

    if args.start_date > args.end_date:
        print("Start date %s is after end date %s" % (args.start_date, args.end_date))
        sys.exit(1)

    dates = pd.date_range(args.start_date, args.end_date)

    all_bars = list()

    end = args.end_date

    # make a client to keep from instantiating many of these
    client = gdax.PublicClient()
    bars = data.fetch_bars(args.pair, args.start_date, args.end_date,
                           args.interval, client, args.quantity)
    logging.debug("fetched and combined %s bars", len(bars))

    # files are saved in the output directory, then by symbol pair, then by date
    path = pathlib.Path(args.output_dir, args.pair, str(args.interval))
    path.mkdir(parents=True, exist_ok=True)

    dates = pd.unique(bars.index.date)
    logging.debug("fetched for %s", dates)
    for date in dates:
        outfile = path / date.strftime('%Y-%m-%d')
        if outfile.exists():
            if outfile.is_file():
                outfile.rename(outfile.with_suffix('.bak'))
        sub = bars[bars.index.date == date]
        sub.to_csv(outfile.open('w'), index=False)
        logging.debug("Wrote %s rows to %s", len(sub), outfile)

if __name__ == '__main__':
    main(sys.argv)
