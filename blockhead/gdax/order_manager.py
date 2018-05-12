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

class Order:
    """ Base class to encapsulate an order, tracking current state.
    Supports creating an order for the quantity to buy/sell at the
    inside bid/ask by default.
    """
    FIVE_PLACES = Decimal(10) ** -5

    def __init__(self, quantity, pair, limit_price=None):
        """ initialize with signed quantity """
        self.quantity = quantity
        self.pair = pair
        self.limit_price = limit_price
        self.total = abs(quantity)
        self.outstanding = abs(quantity)
        self.placed = Decimal(0)
        self.filled = Decimal(0)
        self.state = 'initial'
        self.mgr = None

    def __str__(self):
        """ standard string representation """
        return '<Order q: {0}, o: {1}, lp: {2}. p: {3}, f: {4}, s: {5}'.format(
                self.quantity, self.outstanding, self.limit_price, 
                self.placed, self.filled, self.state)

    async def begin(self, mgr):
        """ tell the order to begin its process, using the
        initialized ordermanager """
        self.mgr = mgr
        # XXX fix this
        mgr.tickdata.add_order_callback(self.handle_order_update)
        if self.quantity > 0:
            # buy at the bid
            price = mgr.tickdata.orderbook.get_bid(self.pair)
            res = await mgr.buy(price=str(price), size=self.total)
        else:
            # sell at the ask
            price = mgr.tickdata.orderbook.get_ask(self.pair)
            res = await mgr.sell(price=str(price), size=self.total)
        self.placed = res['size']
        logging.debug("Order: placed - %s", res)

    def handle_order_update(self, msg):
        """ callback to handle our updates """
        logging.debug("Order update: %s", msg)
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
    orders, place new ones, and hook up to the websocket feed and place
    update the internal order status as needed
    """
    def __init__(self, tickdata, pair):
        self.tickdata = tickdata
        self.tickdata.add_listener(self.on_init, 'initialized')
        self.client = self.tickdata.authtrader
        self.pair = pair

        self.buys = defaultdict(lambda: [])
        self.sells = defaultdict(lambda: [])
        self.buy_qty = 0
        self.sell_qty = 0
        self.pending_orders = []

    async def init(self):
        """ async initialization """
        orders = await self.client.get_orders()
        logging.debug('Outstanding orders: %s', orders)
        if len(orders) == 0:
            return
        buy = [_ for _ in orders if _['side'] == 'buy']
        sell = [_ for _ in orders if _['side'] == 'sell']
        for _ in buy:
            self.buys[_['price']].append(_)
        for _ in sell:
            self.sells[_['price']].append(_)
        self.buy_qty = 0
        for (_, orders) in self.buys.items():
            for order in orders:
                self.buy_qty += Decimal(order['size'])
        self.sell_qty = 0
        for (_, orders) in self.sells.items():
            for order in orders:
                self.sell_qty += Decimal(order['size'])

    def __str__(self):
        buydata = list()
        for (_, buys) in self.buys.items():
            buydata.extend(["%s@%s" % (o['size'], o['price'])
                            for o in buys])
        selldata = list()
        for (_, sells) in self.sells.items():
            selldata.extend(["%s@%s" % (o['size'], o['price'])
                             for o in sells])
        return ("Buys: %s" % self.buy_qty + ",".join(buydata) +
                " Sells: %s" % self.sell_qty + ",".join(selldata))

    async def add_order(self, quantity):
        """ adds an order to buy/sell the target quantity """
        order = Order(quantity, self.pair)
        if self.tickdata.initialized:
            await order.begin(self)
        else:
            self.add_pending_order(order)
        return order

    def add_pending_order(self, order):
        """ begin these orders when initialized """
        self.pending_orders.append(order)

    async def on_init(self, _):
        """ when client has data, we can process pending orders """
        for order in self.pending_orders:
            await order.begin(self)
        self.pending_orders.clear()

    async def cancel_all(self):
        """ cancel all orders for the pair """
        return await self.client.cancel_all(product_id=self.pair)

    async def cancel_buys(self):
        """ cancels all orders to buy for the pair """
        responses = list()
        for (_, buys) in self.buys.items():
            for order in buys:
                responses.append(await self.client.cancel(order['id']))
        return responses

    async def cancel_sells(self):
        """ cancels all orders to sell for the pair """
        responses = list()
        for (_, sells) in self.sells.items():
            for order in sells:
                responses.append(await self.client.cancel(order['id']))
        return responses

    async def cancel_buy(self, price):
        """ cancels all buy orders at this price """
        responses = list()
        for buys in self.buys.get(price, []):
            for order in buys:
                responses.append(await self.client.cancel(order['id']))
        return responses

    async def cancel_sell(self, price):
        """ cancels all sell orders at this price """
        responses = list()
        for sells in self.sells.get(price, []):
            for order in sells:
                responses.append(await self.client.cancel(order['id']))
        return responses

    async def sell(self, price, size):
        """ create a new sell order at this price """
        price = str(price)
        order_id = str(uuid.uuid4())
        self.tickdata.observe_order(order_id)
        order = await self.client.sell(size=str(size), price=price, product_id=self.pair,
                                       time_in_force='GTT', post_only=True, type='limit',
                                       cancel_after='day', client_oid=order_id)
        if 'id' in order:
            self.buys[price] = order
            self.tickdata.observe_order(order['id'])
        return order

    async def buy(self, price, size):
        """ create a new buy order at this price """
        price = str(price)
        order_id = str(uuid.uuid4())
        self.tickdata.observe_order(order_id)
        order = await self.client.buy(size=str(size), price=price, product_id=self.pair,
                                      time_in_force='GTT', post_only=True, type='limit',
                                      cancel_after='day', client_oid=order_id)
        if 'id' in order:
            self.buys[price] = order
            self.tickdata.observe_order(order['id'])
        return order
