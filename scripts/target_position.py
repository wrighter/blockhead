#!/usr/bin/env python
"""
File: target_position.py
Author: Matt Wright
Email: matt@wrighters.net
Github: https://github.com/wrighter
Description: script that exercises the order manager and will
work an order to obtain the desired position. Can either sell
a part of a position, or buy more given enough in the USD
account. Options to get aggressive or work as a marketable limit order
"""
import sys
import logging
import argparse
import datetime
import functools
from decimal import Decimal
import asyncio

from blockhead.gdax.tick_data import TickData
from blockhead.gdax.order_manager import OrderManager, Order, SimpleStrategy, FollowStrategy

async def run(loop, client, ordermanager, args):
    """ main run function, runs until position is obtained or user exits """
    products = await client.get_products()
    product_map = dict(zip([_['id'] for _ in products], products))
    if args.pair not in product_map:
        logging.error("Pair not found in products")
        sys.exit(1)

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

    inventory = 0
    # assume that quantity is for first in pair, and that we need
    # at least that amount in the pair on either side to run
    if args.quantity > 0:
        total_qty = Decimal(accounts[second]['available'])/Decimal(ticker['price'])
    elif args.quantity < 0:
        inventory = Decimal(accounts[first]['available'])
        total_qty = inventory
    else:
        print("Nothing to do. Exiting")
        sys.exit(0)

    if float(total_qty) < abs(args.quantity):
        logging.error('Insufficient funds, you have %s, trading %s, exiting',
                      total_qty, abs(args.quantity))
        sys.exit(1)
    else:
        logging.info('We have %s available to trade %s', total_qty, args.quantity)
        logging.info("%s:%s, %s:%s", first, accounts[first]['available'],
                     second, accounts[second]['available'])

    # for this script we need to get to our target. This will have some parameters
    # -limit price
    # -time to wait (until improving price)
    # -level (how deep in book to place order)
    # -etc
    # Create a higher level order that represents this. Underneath, it will
    # create at least one real order, and track the status on that order.
    # It will handle all order events by doing the correct thing
    # If it eventually terminates (due to error or being filled or timing out)
    # it will notify the script which will terminate.
    strategy = await ordermanager.add_strategy(FollowStrategy(args.quantity))

    logging.debug("starting ws client")

    def do_update(loop, strategy, client):
        """ callback for checking on position status on a timer """
        # XXX check on the order, perhaps update its state?
        if strategy.is_complete():
            logging.debug("All done, exiting")
            client.stop()
        else:
            asyncio.ensure_future(strategy.update_orders())
        td = datetime.timedelta(seconds=1,
                                microseconds=now.microsecond)
        handle = loop.call_at(loop.time() + td.total_seconds(), do_update, loop,
                              strategy, client)

    now = datetime.datetime.now()
    td = datetime.timedelta(seconds=1, microseconds=now.microsecond)
    handle = loop.call_at(loop.time() + td.total_seconds(), do_update, loop,
                          strategy, client)

    await client.run()

def main():
    parser = argparse.ArgumentParser(description='Simple GDAX passive order router')
    parser.add_argument('config',
                        type=argparse.FileType('r'),
                        help='path to a config containing keys and urls')
    parser.add_argument('pair', help='currency pair to trade')
    parser.add_argument('quantity',
                        type=float,
                        help='quantity to trade')
    parser.add_argument('--tradefile',
                        type=str,
                        help='trade file to write tick data')
    parser.add_argument('--limit_price',
                        type=float,
                        help='limit price to bid/offer to obtain position')
    parser.add_argument('--follow',
                        action='store_true',
                        help='follow the market at check interval')
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


    client = TickData(args.config, args.pair,
                      trade_log_file_path=args.tradefile,
                      timeout_sec=args.timeout)

    ordermanager = OrderManager(client, args.pair)

    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(run(loop, client, ordermanager, args))
    except KeyboardInterrupt:
        logging.info("Quitting")
        loop.stop()

    loop.close()

if __name__ == '__main__':
    main()
