"""
File: order_manager.py
Author: Matthew Wright
Email: matt@wrighters.net
Github: https://github.com/wrighter
Description:  Order management functions
"""

import logging
import uuid

from decimal import Decimal
from collections import defaultdict

from abc import ABC, abstractmethod

class Order:
    """ Base class to encapsulate an order on the exchange, and that 
    can track its current state when given all messages.
    Supports creating an order for the size to buy/sell at the
    inside bid/ask by default.
    """
    FIVE_PLACES = Decimal(10) ** -5

    def __init__(self, size, pair, omgr, limit_price=None):
        """ initialize with signed size """
        self.size = size
        self.pair = pair
        self.limit_price = limit_price
        self.total = abs(size)
        self.outstanding = abs(size)
        self.placed = Decimal(0)
        self.filled = Decimal(0)
        self.state = 'initial'
        self.omgr = omgr
        self.client_oid = str(uuid.uuid4())
        self.order_id = None
        self.details = dict()

    def __str__(self):
        """ standard string representation """
        return f'<Order size: {self.size}, o: {self.outstanding}, lp: {self.limit_price}. p: {self.placed}, f: {self.filled}, s: {self.state}, id: {self.order_id}'

    def update(self, details):
        """ update our details dict with info from the feed """
        self.details.update(details)
        logging.debug("Order: update - %s", self.details)

    async def begin(self):
        """ tell the order to begin its process, using the
        initialized ordermanager """
        if self.size > 0:
            res = await self.omgr.buy(self)
        else:
            res = await self.omgr.sell(self)
        if res['status'] == 'rejected':
            logging.warning('Order rejected: %s', res['reason'])
            self.state = 'done'
        else:
            self.placed = res['size']
        self.update(res)

    def handle_order_update(self, msg):
        """ callback to handle our updates """
        self.update(msg)
        if msg['type'] == 'received':
            logging.debug('Order received')
            self.state = 'received'
        elif msg['type'] == 'open':
            logging.debug('Order went active')
            self.state = 'open'
        elif msg['type'] == 'match':
            logging.debug('Order matched')
            self.filled += Decimal(msg['size'])
            self.filled = self.filled.quantize(Order.FIVE_PLACES)
            if self.filled == self.total:
                logging.debug('Order completed')
        elif msg['type'] == 'done':
            logging.debug('Order %s', msg['reason'])
            self.state = 'done'

class OrderManager(object):
    """
    Class to manage orders on GDAX. Using an authclient, will get initial
    orders, place new ones, and connect to the websocket feed and monitor
    order updates.
    """
    # TODO figure out what we want to do about existing orders
    # TODO add cancel/replace functionality
    def __init__(self, tickdata, pair):
        self.tickdata = tickdata
        self.tickdata.add_listener(self.on_init, 'initialized')
        self.tickdata.add_order_callback(self.handle_order_update)
        self.client = self.tickdata.authtrader
        self.pair = pair

        self.pending_orders = []
        self.pending_strategies = []
        self.strategies = []
        self.order_lookup = dict()

    async def init(self):
        """ async initialization """
        pass

    async def add_order(self, size):
        """ adds an order to buy/sell the target size """
        order = Order(size, self.pair, self)
        # track by client order id as well
        self.order_lookup[order.client_oid] = order
        if self.tickdata.initialized:
            await order.begin()
        else:
            self.pending_orders.append(order)
        return order

    async def add_strategy(self, strategy):
        """ adds a strategy which will handle order details """
        self.strategies.append(strategy)
        if self.tickdata.initialized:
            await strategy.init(self)
        else:
            self.pending_strategies.append(strategy)
        return strategy

    async def on_init(self, _):
        """ when client has data, we can process pending orders """
        for order in self.pending_orders:
            await order.begin()
        for strat in self.pending_strategies:
            await strat.init(self)
        self.pending_orders.clear()
        self.pending_strategies.clear()

    async def cancel_all(self):
        """ cancel all orders for the pair
        XXX this is for all orders for this pair, even ones we didn't create
        """
        return await self.client.cancel_all(product_id=self.pair)

    async def cancel_order(self, order):
        """ cancel the order """
        logging.debug("Canceling order: %s", order)
        resp = await self.client.cancel_order(order.order_id)
        return resp

    async def get_order(self, order):
        """ get the order """
        resp = await self.client.get_order(order.order_id)
        return resp

    async def sell(self, order):
        """ create a new sell limit order at this price """
        if order.limit_price is None:
            # default sell at the ask
            # TODO too much knowledge here
            order.limit_price = self.tickdata.orderbook.get_ask(order.pair)
        logging.debug('Placing order: %s', order) 
        res = await self.client.sell(size=order.total,
                                     price=order.limit_price,
                                     product_id=order.pair,
                                     time_in_force='GTT',
                                     post_only=True,
                                     type='limit',
                                     cancel_after='day',
                                     client_oid=order.client_oid)
        logging.debug("Placed order: %s", res)
        if 'id' in res:
            order.order_id = res['id']
            self.order_lookup[order.order_id] = order
        return res

    async def buy(self, order):
        """ create a new buy limit order at this price """
        if order.limit_price is None:
            # buy at the bid
            order.limit_price = self.tickdata.orderbook.get_bid(order.pair)
 
        logging.debug('Placing order: %s', order) 
        res = await self.client.buy(size=order.total,
                                    price=order.limit_price,
                                    product_id=order.pair,
                                    time_in_force='GTT',
                                    post_only=True,
                                    type='limit',
                                    cancel_after='day',
                                    client_oid=order.client_oid)
        logging.debug("Placed order: %s", res)
        if 'id' in res:
            order.order_id = res['id']
            self.order_lookup[order.order_id] = order
        return res

    async def handle_order_update(self, msg):
        """ callback to handle all updates for our orders"""
        for key in ['client_oid', 'order_id', 'maker_order_id', 'taker_order_id']:
            if key in msg:
                order = self.order_lookup.get(msg[key])
                if order:
                    order.handle_order_update(msg)
                    return
        logging.error("Could not find matching order: %s", msg)

    async def update_orders(self):
        """
        hook for updating the strategy. Can look at the state of current
        orders and the book and determine whether changes need to be made.
        """
        for strat in self.strategies:
            await strat.update_orders()

class Strategy(ABC):
    """ Order handling strategy. Will place and modify orders """
    def __init__(self, name):
        self.name = name
        self.omgr = None
        self.initialized = False

    @abstractmethod
    async def init(self, omgr):
        pass

    @abstractmethod
    async def update_orders(self):
        pass

    @abstractmethod
    def is_complete(self):
        pass

    def make_order(self):
        """ makes a new order object and tracks it """
        order = Order(self.size, self.omgr.pair, self.omgr)
        # track by client order id as well
        self.omgr.order_lookup[order.client_oid] = order
        return order

class SimpleStrategy(Strategy):
    def __init__(self, size):
        super().__init__("SimpleStrategy")
        self.size = size
        self.order = None

    async def init(self, omgr):
        self.omgr = omgr
        self.order = self.make_order()
        await self.order.begin()
        self.initialized = True

    async def update_orders(self):
        """ do nothing, just leave orders there """
        pass

    def is_complete(self):
        return self.order and self.order.state == 'done'

class FollowStrategy(Strategy):
    def __init__(self, size):
        super().__init__("FollowStrategy")
        self.size = size
        self.order = None

    async def init(self, omgr):
        self.omgr = omgr
        self.order = self.make_order()
        await self.order.begin()
        self.initialized = True

    async def update_orders(self):
        """ Look at top of book, move order to follow on update """
        # TODO partial fills
        if not self.initialized:
            return
        if self.size > 0:
            # TODO simplify
            current_bid = self.omgr.tickdata.orderbook.get_bid(self.order.pair)
            if current_bid > self.order.limit_price and self.order.state != 'done':
                resp = await self.omgr.cancel_order(self.order)
                logging.debug("Cancel response: %s", resp)
                self.order = self.make_order()
                res = await self.omgr.buy(self.order)
        else:
            current_ask = self.omgr.tickdata.orderbook.get_ask(self.order.pair)
            if current_ask < self.order.limit_price and self.order.state != 'done':
                resp = await self.omgr.get_order(self.order)
                logging.debug("Order before cancel: %s", resp)
                resp = await self.omgr.cancel_order(self.order)
                logging.debug("Cancel response: %s", resp)
                self.order = self.make_order()
                res = await self.omgr.sell(self.order)

    def is_complete(self):
        """ the strategy is complete """
        return self.order and self.order.state == 'done'

