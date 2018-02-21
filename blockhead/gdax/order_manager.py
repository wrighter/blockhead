from decimal import Decimal
from sortedcontainers import SortedDict

class OrderManager(object):
    """
    Class to manage orders. Using an authclient, will get initial
    orders, place new ones, and hook up to the websocket feed and place
    update the internal order status as needed
    """
    def __init__(self, client, pair):
        self.client = client
        self.pair = pair
        orders = self.client.get_orders(product_id=self.pair)
        buys = [_ for _ in orders[0] if _['side'] == 'buy']
        sells = [_ for _ in orders[0] if _['side'] == 'sell']
        self.buys = SortedDict(zip([_['price'] for _ in buys], buys))
        self.sells = SortedDict(zip([_['price'] for _ in sells], sells))
        self.buy_qty = sum([Decimal(o['size'])
                            for (_, o) in self.buys.items()])
        self.sell_qty = sum([Decimal(o['size'])
                             for (_, o) in self.buys.items()])

    def __str__(self):
        buydata = ["%s@%s" % (o['size'], o['price'])
                   for o in self.buys.items()]
        selldata = ["%s@%s" % (o['size'], o['price'])
                    for o in self.sells.items()]
        return ("Buys: %s" % self.buy_qty + ",".join(buydata) +
                "\nSells: %s" % self.sell_qty + ",".join(selldata))

    def cancel_all(self):
        pass

    def cancel_buys(self):
        pass

    def cancel_sells(self):
        pass

    def cancel_buy(self, price):
        pass

    def cancel_sell(self, price):
        pass

    def sell(self, price, qty):
        pass

    def buy(self, price, qty):
        pass

