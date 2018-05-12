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
    """ Base class to encapsulate an order on the exchange, and that 
    can track its current state when given all messages.
    Supports creating an order for the size to buy/sell at the
    inside bid/ask by default.
    """
    FIVE_PLACES = Decimal(10) ** -5

    def __init__(self, size, pair, mgr, limit_price=None):
        """ initialize with signed size """
        self.size = size
        self.pair = pair
        self.limit_price = limit_price
        self.total = abs(size)
        self.outstanding = abs(size)
        self.placed = Decimal(0)
        self.filled = Decimal(0)
        self.state = 'initial'
        self.mgr = mgr
        self.client_oid = str(uuid.uuid4())
        self.order_id = None

    def __str__(self):
        """ standard string representation """
        return f'<Order size: {self.size}, o: {self.outstanding}, lp: {self.limit_price}. p: {self.placed}, f: {self.filled}, s: {self.state}'

    async def begin(self):
        """ tell the order to begin its process, using the
        initialized ordermanager """
        if self.size > 0:
            res = await self.mgr.buy(self)
        else:
            res = await self.mgr.sell(self)
        self.placed = res['size']
        logging.debug("Order: placed - %s", res)

    def handle_order_update(self, msg):
        """ callback to handle our updates """
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
    order updates
    """
    def __init__(self, tickdata, pair):
        self.tickdata = tickdata
        self.tickdata.add_listener(self.on_init, 'initialized')
        self.tickdata.add_order_callback(self.handle_order_update)
        self.client = self.tickdata.authtrader
        self.pair = pair

        self.buys = defaultdict(lambda: [])
        self.sells = defaultdict(lambda: [])
        self.buy_qty = 0
        self.sell_qty = 0
        self.pending_orders = []
        self.order_lookup = dict()

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

    async def add_order(self, size):
        """ adds an order to buy/sell the target size """
        order = Order(size, self.pair, self)
        self.order_lookup[order.client_oid] = order
        if self.tickdata.initialized:
            await order.begin()
        else:
            self.pending_orders.append(order)
        return order

    async def on_init(self, _):
        """ when client has data, we can process pending orders """
        for order in self.pending_orders:
            await order.begin()
        self.pending_orders.clear()

    async def cancel_all(self):
        """ cancel all orders for the pair
        XXX this is for all orders for this pair, even ones we didn't create
        """
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

    async def sell(self, order):
        """ create a new sell limit order at this price """
        if order.limit_price is None:
            # default sell at the ask
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
        if 'id' in res:
            order.order_id = res['id']
            self.order_lookup[order.order_id] = order
        return res

    async def handle_order_update(self, msg):
        """ callback to handle all updates for our orders"""
        logging.debug("Order update: %s", msg)
        for key in ['client_oid', 'order_id', 'maker_order_id', 'taker_order_id']:
            if key in msg:
                order = self.order_lookup.get(msg[key])
                if order:
                    order.handle_order_update(msg)
                    return
        logging.error("Could not find matching order: %s", msg)

