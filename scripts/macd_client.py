#!/usr/bin/env python
"""
File: gdax_client.py
Author: Matt Wright
Email: matt@wrighters.net
Github: https://github.com/wrighter
Description:  Simple proof of concept that connects to GDAX,
builds bars, and can place orders when certain conditions are met
"""
import sys
import logging
import argparse
import datetime
from decimal import Decimal
import asyncio
import pandas as pd

from blockhead.gdax.ws_client import GDAXWebsocketClient
from blockhead.gdax.order_manager import OrderManager

def main(args):
    client = GDAXWebsocketClient(args.config, products=args.pair,
                                 auth=True,
                                 #channels=["heartbeat", "full"])
                                 channels=["heartbeat", "full"],
                                 should_print=True)

    products = client.get_products()
    product_map = dict(zip([_['id'] for _ in products], products))
    if args.pair not in product_map:
        logging.error("Pair not found in products")
        sys.exit(1)

    ordermanager = OrderManager(client.authclient, args.pair)

    logging.debug(ordermanager)

    ticker = client.authclient.get_product_ticker(args.pair)

    if ticker:
        logging.info("got ticker for %s, %s", args.pair, ticker)
    else:
        logging.error("failed to get ticker: %s" % ticker)
        sys.exit(1)

    accounts = client.authclient.get_accounts()
    accounts = dict(zip([_['currency'] for _ in accounts], accounts))
    (first, second) = args.pair.split('-')
    logging.info("You have %s %s", first, accounts[first]['available'])
    logging.info("You have %s %s", second, accounts[second]['available'])

    # assume that trade_qty is first in pair, and that we need
    # at least that amount in the pair on either side to run
    if args.trade_qty:
        total_qty = Decimal(accounts[first]['available'])
        total_qty += Decimal(accounts[second]['available'])/Decimal(ticker['price'])
        if total_qty < args.trade_qty:
            logging.error('Insufficient funds, exiting')
            sys.exit(1)
        else:
            logging.info('We have %s available to trade %s', total_qty, args.trade_qty)
            logging.info("%s:%s, %s:%s", first, accounts[first]['available'],
                    second, accounts[second]['available'])

    bars = client.get_bars(args.pair, args.lookback * 26)

    logging.debug("starting ws client")
    client.start()

    # TODO
    # figure out total quantity in orders outstanding
    # figure out how to update orders as they are filled from live feed
    # decide on whether to place an order or not on indicator update
    # decide on price to pay
    # place order and add to order list
    # see the order show up in the feed
    def do_indicator(bars):
        """ handle updates """
        # XXX move to just doing emas on our own instead of a growing dataframe
        ema1 = bars['close'].ewm(span=12 * args.lookback).mean()[-1]
        ema2 = bars['close'].ewm(span=26 * args.lookback).mean()[-1]
        logging.info("close: %s ema1: %s ema2: %s",
                     bars['close'][-1], ema1, ema2)
        try:
            logging.info("bid: %s ask: %s", client.get_bid(), client.get_ask())
        except ValueError as verr:
            logging.info("No book yet")

        if ema1 > ema2:
            logging.info("long")
        else:
            logging.info("short")

    def on_bar(client, bars, loop):
        """ handles appending to the bar """
        logging.debug("message_count = %s", client.message_count)
        bar = client.get_bar()
        # append to bars
        now = datetime.datetime.utcnow()
        td = datetime.timedelta(seconds=now.second,
                                microseconds=now.microsecond)
        close_time = now - td
        open_time = close_time - datetime.timedelta(minutes=1)
        bar['close_time'] = close_time
        bar['open_time'] = open_time
        bars = pd.concat([bars, pd.DataFrame(bar, index=[close_time])])
      
        do_indicator(bars)

        logging.info(bars.tail(2))
        td = datetime.timedelta(seconds=60-now.second,
                                microseconds=now.microsecond)
        handle = loop.call_at(loop.time() + td.total_seconds(), on_bar,
                              client, bars, loop)


    do_indicator(bars)
    loop = asyncio.get_event_loop()
    now = datetime.datetime.now()
    td = datetime.timedelta(seconds=60-now.second,
                            microseconds=now.microsecond)
    handle = loop.call_at(loop.time() + td.total_seconds(), on_bar,
                          client, bars, loop)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info("Quitting")
        handle.cancel()
        client.close()
        loop.stop()
        sys.exit(0)
    loop.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simple GDAX market maker')
    parser.add_argument('config',
                        type=argparse.FileType('r'), 
                        help='path to a config containing keys and urls')
    parser.add_argument('pair', help='currency pair to trade')
    parser.add_argument('-t', '--trade_qty',
                        default=None,
                        type=float,
                        help='quantity to trade')
    parser.add_argument('-l', '--lookback',
                        default=15,
                        type=int,
                        help='minutes of lookback for moving averages')

    parser.add_argument('-d', '--debug', help='enable debug logging', action='store_true')

    args = parser.parse_args(sys.argv[1:])
    if args.debug:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.debug(args)
    main(args)
