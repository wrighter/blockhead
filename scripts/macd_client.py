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

    accounts = await client.get_account('')
    accounts = dict(zip([_['currency'] for _ in accounts], accounts))
    (first, second) = args.pair.split('-')
    logging.info("You have %s %s", first, accounts[first]['available'])
    logging.info("You have %s %s", second, accounts[second]['available'])

    inventory = Decimal(0)
    # assume that quantity is first in pair, and that we need
    # at least that amount in the pair on either side to run
    if args.quantity:
        total_qty = Decimal(accounts[second]['available'])/Decimal(ticker['price'])
        if args.use_inventory:
            current = Decimal(accounts[first]['available'])
            inventory = current.min(args.quantity)
            total_qty += current
        if total_qty < args.quantity:
            logging.error('Insufficient funds, exiting')
            sys.exit(1)
        else:
            logging.info('We have %s available to trade %s', total_qty, args.quantity)
            logging.info("%s:%s, %s:%s", first, accounts[first]['available'],
                         second, accounts[second]['available'])
            logging.info("Initial inventory is %s", inventory)

    bars = await client.get_bars(args.pair, args.lookback * 26)

    logging.debug("starting ws client")

    # TODO
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
        if ema1 > ema2:
            logging.info("long")
        else:
            logging.info("short")

        if client.initialized:
            # TODO, look at sign flips without fills
            if ema1 > ema2:
                # if long and inventory < quantity, let's buy
                qty = (Decimal(args.quantity) - inventory).quantize(Order.FIVE_PLACES)
                qty -= ordermanager.total_outstanding()
                if qty > 0:
                    asyncio.ensure_future(start_order(loop, qty))
                    logging.info("created order for %s", qty)
            else:
                # if short, get rid of any inventory, cannot short
                if inventory + ordermanager.total_outstanding() > 0:
                    qty = -inventory.quantize(Order.FIVE_PLACES)
                    asyncio.ensure_future(start_order(loop, qty))
                    logging.info("created order for %s", qty)
            try:
                logging.info("bid: %s ask: %s",
                             client.orderbook.get_bid(args.pair),
                             client.orderbook.get_ask(args.pair))
            except ValueError as _:
                logging.info("No book yet")

    async def start_order(loop, quantity):
        """ starts an order strategy """
        strategy = await ordermanager.add_strategy(FollowStrategy(quantity))
        tdelta = datetime.timedelta(seconds=1,
                                    microseconds=now.microsecond)
        loop.call_at(loop.time() + tdelta.total_seconds(), do_order_update,
                     loop, strategy, client)

    def do_order_update(loop, strategy, client):
        """ callback for checking positions """
        if not strategy.is_complete():
            asyncio.ensure_future(strategy.update_orders())
        tdelta = datetime.timedelta(seconds=1,
                                    microseconds=now.microsecond)
        loop.call_at(loop.time() + tdelta.total_seconds(),
                     do_order_update, loop, strategy, client)

    def on_bar(client, bars, loop):
        """ handles appending bar and updating model """
        now = datetime.datetime.utcnow()
        if client.initialized:
            current_bar = client.current_bar.get_bar()
            # append to bars
            tdelta = datetime.timedelta(seconds=now.second,
                                        microseconds=now.microsecond)
            close_time = now - tdelta
            open_time = close_time - datetime.timedelta(minutes=1)
            current_bar['close_time'] = close_time
            current_bar['open_time'] = open_time
            bars = pd.concat([bars, pd.DataFrame(current_bar, index=[close_time])])

        do_indicator(bars)

        logging.info('Recent bar data: %s', bars.tail(2))
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
                        type=Decimal,
                        help='quantity to trade')
    parser.add_argument('-l', '--lookback',
                        default=15,
                        type=int,
                        help='minutes of lookback for moving averages')
    parser.add_argument('--timeout',
                        type=int,
                        default=10,
                        help='timeout for trader requests (in seconds)')
    parser.add_argument('--use_inventory',
                        action='store_true',
                        help='use existing inventory to sell when short')
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
