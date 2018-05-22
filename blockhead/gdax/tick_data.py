"""
File: tick_data.py
Author: Matthew Wright
Email: matt@wrighters.net
Github: https://github.com/wrighter
Description: delegates to OrderBook, provides access to live
tick data as well as ways to access account and ticker data.
Also uses message data and can calculate useful information
such as bars with it.
"""

import datetime
import logging

from decimal import Decimal, MIN_EMIN, MAX_EMAX
from collections import defaultdict

from blockhead.gdax.data import parse_config, fetch_bars
from gdax.trader import Trader
from gdax.orderbook import OrderBook

class Bar(object):
    """ Turns ticks in to bars """
    def __init__(self):
        self.high_price = MIN_EMIN
        self.low_price = MAX_EMAX
        self.open_price = None
        self.close_price = None
        self.prev_close_price = None
        self.volume = Decimal(0)

    def handle_close(self, close, size):
        """ canonical bar close processing """
        if close is None:
            return
        self.close_price = close
        if self.close_price > self.high_price:
            self.high_price = self.close_price
        if self.close_price < self.low_price:
            self.low_price = self.close_price
        if self.open_price is None:
            self.open_price = self.close_price
        self.volume += size

    def get_bar(self):
        """ close and return the current bar """
        if self.close_price is None:
            self.close_price = self.prev_close_price
        if self.high_price == MIN_EMIN:
            self.high_price = self.close_price
        if self.low_price == MAX_EMAX:
            self.low_price = self.close_price
        last_bar = {'open': self.open_price,
                    'high': self.high_price,
                    'low': self.low_price,
                    'close': self.close_price,
                    'volume': self.volume}
        self.prev_close_price = self.close_price
        return last_bar

class TickData(object):
    """ TickData will allow for clients to obtain messages and
    process them after they are saved in the OrderBook """
    def __init__(self, config, product_id='ETH-USD', use_heartbeat=False,
                 trade_log_file_path=None, timeout_sec=10):

        cfg = parse_config(config)
        self.product_id = product_id
        self.api_key = cfg['api_key']
        self.api_secret = cfg['api_secret']
        self.passphrase = cfg['api_passphrase']
        self.timeout_sec = timeout_sec
        self.trade_log_file_path = trade_log_file_path
        self.authtrader = Trader(#product_id=self.product_id,
                                 api_key=self.api_key,
                                 api_secret=self.api_secret,
                                 passphrase=self.passphrase,
                                 timeout_sec=timeout_sec)
        self.orderbook = None
        self.initialized = False
        self.current_bar = None
        self.event_listeners = defaultdict(list)
        self.order_callbacks = list()
        self._stop = None

    def __str__(self):
        """ string representation for logging """
        return f"<TickData {self.product_id} initialized? {self.initialized}>"

    async def run(self):
        """ run with an orderbook, handling messages until done """
        async with OrderBook(self.product_id,
                             self.api_key,
                             self.api_secret,
                             self.passphrase,
                             trade_log_file_path=self.trade_log_file_path,
                             timeout_sec=self.timeout_sec) as orderbook:
            self.orderbook = orderbook
            while True:
                if self._stop:
                    self.orderbook = None
                    return
                msg = await self.handle_message(self.orderbook)
                if msg:
                    pass
                    #logging.debug(msg)

    def stop(self):
        """ signal to stop processing ticks """
        self._stop = True

    async def handle_message(self, orderbook):
        """ tick handling """
        msg = await orderbook.handle_message()
        if msg is None:
            return msg
        if not self.initialized:
            self.current_bar = Bar()
            self.initialized = True
            await self.notify_listeners('initialized')
            logging.debug("TickData is initialized")
        if msg['type'] == 'match':
            # bar processing
            self.current_bar.handle_close(Decimal(msg['price']), Decimal(msg['size']))
        if 'user_id' in msg:
            await self.notify_order_listeners(msg)
        return msg

    async def get_products(self):
        """ returns the set of products at GDAX """
        return await self.authtrader.get_products()

    async def get_product_ticker(self, pair):
        """ get the ticker for this product """
        return await self.authtrader.get_product_ticker(pair)

    async def get_account(self, account_id=''):
        """ get the accounts or a single account for this api_key"""
        return await self.authtrader.get_account(account_id)

    async def buy(self, product_id=None, price=None, size=None, funds=None,
            **kwargs):
        """ buys using this trader """
        return await self.authtrader.buy(product_id, price, size, funds, **kwargs)

    async def sell(self, product_id=None, price=None, size=None, funds=None,
            **kwargs):
        """ sells using this trader """
        return await self.authtrader.sell(product_id, price, size, funds, **kwargs)

    def add_order_callback(self, callback):
        """ adds a callback that will be notified of all orders
        seen in the feed for this account """
        self.order_callbacks.append(callback)

    async def notify_order_listeners(self, msg):
        """ notify the order listeners of the order updates """
        for cback in self.order_callbacks:
            await cback(msg)

    def add_listener(self, cback, event):
        """ adds this listener to the event callbacks """
        self.event_listeners[event].append(cback)

    async def notify_listeners(self, event):
        """ notify all listeners  of the event """
        for cback in self.event_listeners.get(event, []):
            await cback(event)

    async def get_bars(self, pair, qty, granularity=60, end=None):
        """ get qty granularity second bars for pair
        pair -- the currency pair
        qty -- number of periods to fetch
        granularity -- the number of seconds to include
        end -- the end time for the bars, in utc """
        end = end or datetime.datetime.utcnow()
        start = end - datetime.timedelta(seconds=qty * granularity)
        return await fetch_bars(pair, start, end, granularity,
                                client=self.authtrader, batch=min(300,qty))
