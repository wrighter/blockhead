#!/usr/bin/env python
"""
File: macd_client.py
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

from blockhead.gdax.tick_data import TickData
from blockhead.gdax.order_manager import OrderManager, Order, FollowStrategy

async def run(loop, args):
    """ run the model """
    client = TickData(args.config, args.pair,
                      timeout_sec=args.timeout)

    products = await client.get_products()
    product_map = dict(zip([_['id'] for _ in products], products))
    if args.pair not in product_map:
        logging.error("Pair not found in products")
        sys.exit(1)

    ordermanager = OrderManager(client, args.pair)
    await ordermanager.init()
    logging.debug(ordermanager)

    ticker = await client.get_product_ticker(args.pair)

    if ticker:
        logging.info("got ticker for %s, %s", args.pair, ticker)
    else:
        logging.error("failed to get ticker: %s", ticker)
        sys.exit(1)

    accounts = await client.get_account()
    accounts = dict(zip([_['currency'] for _ in accounts], accounts))
    (first, second) = args.pair.split('-')
    logging.info("You have %s %s", first, accounts[first]['available'])
    logging.info("You have %s %s", second, accounts[second]['available'])

    # assume that quantity is first in pair, and that we need
    # at least that amount in the pair on either side to run
    if args.quantity:
        total_qty = Decimal(accounts[first]['available'])
        total_qty += Decimal(accounts[second]['available'])/Decimal(ticker['price'])
        if total_qty < args.quantity:
            logging.error('Insufficient funds, exiting')
            sys.exit(1)
        else:
            logging.info('We have %s available to trade %s', total_qty, args.quantity)
            logging.info("%s:%s, %s:%s", first, accounts[first]['available'],
                         second, accounts[second]['available'])

    bars = await client.get_bars(args.pair, args.lookback * 26)

    logging.debug("starting ws client")

    # TODO
    # figure out total quantity in orders outstanding
    # figure out how to update orders as they are filled from live feed
    # decide on whether to place an order or not on indicator update
    # decide on price to pay
    # place order and add to order list
    # see the order show up in the feed
    def do_indicator(bars):
        """ handle updates """
        if not client.initialized:
            return
        # XXX move to just doing emas on our own instead of a growing dataframe
        ema1 = bars['close'].ewm(span=12 * args.lookback).mean()[-1]
        ema2 = bars['close'].ewm(span=26 * args.lookback).mean()[-1]
        logging.info("close: %s ema1: %s ema2: %s",
                     bars['close'][-1], ema1, ema2)
        try:
            logging.info("bid: %s ask: %s",
                         client.orderbook.get_bid(args.pair),
                         client.orderbook.get_ask(args.pair))
        except ValueError as _:
            logging.info("No book yet")

        if ema1 > ema2:
            logging.info("long")
        else:
            logging.info("short")

    def on_bar(client, bars, loop):
        """ handles appending to the bar """
        logging.debug("sequence_id = %s",
                      client.orderbook._sequences[args.pair])
        if not client.initialized:
            logging.warning("TickData not yet initialized, missing a bar")
            return
        current_bar = client.current_bar.get_bar()
        # append to bars
        now = datetime.datetime.utcnow()
        tdelta = datetime.timedelta(seconds=now.second,
                                    microseconds=now.microsecond)
        close_time = now - tdelta
        open_time = close_time - datetime.timedelta(minutes=1)
        current_bar['close_time'] = close_time
        current_bar['open_time'] = open_time
        bars = pd.concat([bars, pd.DataFrame(current_bar, index=[close_time])])

        do_indicator(bars)

        logging.info(bars.tail(2))
        tdelta = datetime.timedelta(seconds=60-now.second,
                                    microseconds=now.microsecond)
        loop.call_at(loop.time() + tdelta.total_seconds(), on_bar,
                     client, bars, loop)

    do_indicator(bars)
    now = datetime.datetime.now()
    tdelta = datetime.timedelta(seconds=60-now.second,
                                microseconds=now.microsecond)
    loop.call_at(loop.time() + tdelta.total_seconds(), on_bar,
                 client, bars, loop)

    await client.run()

def main():
    """ main function, parses args and sets up loop """
    parser = argparse.ArgumentParser(description='Simple GDAX market maker')
    parser.add_argument('config',
                        type=argparse.FileType('r'),
                        help='path to a config containing keys and urls')
    parser.add_argument('pair', help='currency pair to trade')
    parser.add_argument('quantity',
                        default=None,
                        type=float,
                        help='quantity to trade')
    parser.add_argument('-l', '--lookback',
                        default=15,
                        type=int,
                        help='minutes of lookback for moving averages')
    parser.add_argument('--timeout',
                        type=int,
                        default=10,
                        help='timeout for trader requests (in seconds)')
    parser.add_argument('-d', '--debug', help='enable debug logging', action='store_true')

    args = parser.parse_args(sys.argv[1:])
    if args.debug:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.debug(args)

    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(run(loop, args))
    except KeyboardInterrupt:
        logging.info("Quitting")
        loop.stop()

    loop.close()

if __name__ == '__main__':
    main()
