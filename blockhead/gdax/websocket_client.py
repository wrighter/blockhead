"""
GDAX connectivity, manages keys, maintaining current order
book, and bar generation
"""
import logging
import datetime
import configparser
import gdax
import pandas as pd
from bintrees import RBTree
from decimal import Decimal, MIN_EMIN, MAX_EMAX

class GDAXWebsocketClient(gdax.WebsocketClient):

    def __init__(self, config, products, auth, channels):
        super(GDAXWebsocketClient, self).__init__(products=products,
                auth=auth, channels=channels)
        self.config = self.parse_config(config)
        self.authclient = gdax.AuthenticatedClient(self.api_key,
                self.api_secret, self.api_passphrase, self.api)
        self._asks = RBTree()
        self._bids = RBTree()
        self._sequence = -1
        self._current_ticker = None

    def parse_config(self, config):
        """ parses the data from the config """
        cparser = configparser.ConfigParser()
        cparser.read_file(config)
        try:
            self.api_key = cparser['keys']['key']
            self.api_secret = cparser['keys']['secret']
            self.api_passphrase = cparser['keys']['passphrase']
            self.api = cparser['uris']['api']
            self.url = cparser['uris']['wsapi']
        except KeyError as ke:
            logging.error(ke)
            raise ke

    def get_products(self):
        return self.authclient.get_products()

    def get_product_ticker(self):
        return self.authclient.get_product_ticker()

    def get_accounts(self):
        return self.authclient.get_accounts()

    def get_orders(self):
        return self.authclient.get_orders()

    def on_open(self):
        self._sequence = -1
        self.message_count = 0
        self.init_bar()
        logging.debug("on_open")

    def on_close(self):
        logging.debug("on_close")

    def reset_book(self):
        self._asks = RBTree()
        self._bids = RBTree()
        res = self.authclient.get_product_order_book(product_id=self.products[0], level=3)
        for bid in res['bids']:
            self.add({
                'id': bid[2],
                'side': 'buy',
                'price': Decimal(bid[0]),
                'size': Decimal(bid[1])
            })
        for ask in res['asks']:
            self.add({
                'id': ask[2],
                'side': 'sell',
                'price': Decimal(ask[0]),
                'size': Decimal(ask[1])
            })
        self._sequence = res['sequence']

    def init_bar(self):
        self.high_price = MIN_EMIN
        self.low_price = MAX_EMAX
        self.open_price = None
        self.close_price = None
        self.prev_close_price = None
        self.volume = Decimal(0)

    def get_bar(self):
        if self.close_price is None:
            self.close_price = self.prev_close_price
        if self.high_price == MIN_EMIN:
            self.high_price = self.close_price
        if self.low_price == MAX_EMAX:
            self.low_price = self.close_price
        bar = {'open': self.open_price,
               'high': self.high_price,
               'low': self.low_price,
               'close': self.close_price,
               'volume': self.volume}
        self.prev_close_price = self.close_price
        self.init_bar()
        return bar

    def on_message(self, msg):
        self.message_count += 1
        if self._sequence == -1:
            self.reset_book()
            logging.debug("reset order book")
            return

        sequence = msg.get('sequence')
        if sequence is not None:
            if sequence <= self._sequence:
                # ignore older messages (e.g. before order book
                # initialization from getProductOrderBook)
                return
            elif sequence > self._sequence + 1:
                self.on_sequence_gap(self._sequence, sequence)
                logging.debug('gap from %s detected in %s', self._sequence, msg)
                return

        if msg['type'] == 'error':
            logging.error(msg['message'])
        elif msg['type'] == 'subscriptions':
            logging.info('Subscriptions')
            for _ in msg['channels']:
                logging.info('%s - products: %s', _['name'],
                        ",".join(_['product_ids']))
        elif msg['type'] == 'heartbeat':
            # can check for missed messages? or does sequence handle this?
            pass
        elif msg['type'] == 'match':
            self.match(msg)
            self._current_ticker = msg
            # bar processing
            self.close_price = Decimal(msg['price'])
            if self.close_price > self.high_price:
                self.high_price = self.close_price
            if self.close_price < self.low_price:
                self.low_price = self.close_price
            if self.open_price is None:
                self.open_price = self.close_price
            self.volume += Decimal(msg['size'])
        elif msg['type'] == 'open':
            self.add(msg)
        elif msg['type'] == 'done' and 'price' in msg:
            self.remove(msg)
        elif msg['type'] == 'change':
            self.change(msg)
        elif msg['type'] in ['received', 'ticker']:
            # we'll ignore these for now
            pass
        elif msg['type'] == 'done' and 'price' not in msg:
            pass
        else:
            logging.debug(msg['type'])
        if sequence is not None:
            self._sequence = sequence

    def on_sequence_gap(self, gap_start, gap_end):
        self.reset_book()
        logging.error('messages missing (%s - %s). Re-initializing  book at sequence: %s',
                    gap_start, gap_end, self._sequence)

    def add(self, order):
        order = {
            'id': order.get('order_id') or order['id'],
            'side': order['side'],
            'price': Decimal(order['price']),
            'size': Decimal(order.get('size') or order['remaining_size'])
        }
        if order['side'] == 'buy':
            bids = self.get_bids(order['price'])
            if bids is None:
                bids = [order]
            else:
                bids.append(order)
            self.set_bids(order['price'], bids)
        else:
            asks = self.get_asks(order['price'])
            if asks is None:
                asks = [order]
            else:
                asks.append(order)
            self.set_asks(order['price'], asks)

    def remove(self, order):
        price = Decimal(order['price'])
        if order['side'] == 'buy':
            bids = self.get_bids(price)
            if bids is not None:
                bids = [o for o in bids if o['id'] != order['order_id']]
                if len(bids) > 0:
                    self.set_bids(price, bids)
                else:
                    self.remove_bids(price)
        else:
            asks = self.get_asks(price)
            if asks is not None:
                asks = [o for o in asks if o['id'] != order['order_id']]
                if len(asks) > 0:
                    self.set_asks(price, asks)
                else:
                    self.remove_asks(price)

    def match(self, order):
        size = Decimal(order['size'])
        price = Decimal(order['price'])

        if order['side'] == 'buy':
            bids = self.get_bids(price)
            if not bids:
                return
            assert bids[0]['id'] == order['maker_order_id']
            if bids[0]['size'] == size:
                self.set_bids(price, bids[1:])
            else:
                bids[0]['size'] -= size
                self.set_bids(price, bids)
        else:
            asks = self.get_asks(price)
            if not asks:
                return
            assert asks[0]['id'] == order['maker_order_id']
            if asks[0]['size'] == size:
                self.set_asks(price, asks[1:])
            else:
                asks[0]['size'] -= size
                self.set_asks(price, asks)

    def change(self, order):
        try:
            new_size = Decimal(order['new_size'])
        except KeyError:
            return

        try:
            price = Decimal(order['price'])
        except KeyError:
            return

        if order['side'] == 'buy':
            bids = self.get_bids(price)
            if bids is None or not any(o['id'] == order['order_id'] for o in bids):
                return
            index = [b['id'] for b in bids].index(order['order_id'])
            bids[index]['size'] = new_size
            self.set_bids(price, bids)
        else:
            asks = self.get_asks(price)
            if asks is None or not any(o['id'] == order['order_id'] for o in asks):
                return
            index = [a['id'] for a in asks].index(order['order_id'])
            asks[index]['size'] = new_size
            self.set_asks(price, asks)

        tree = self._asks if order['side'] == 'sell' else self._bids
        node = tree.get(price)

        if node is None or not any(o['id'] == order['order_id'] for o in node):
            return

    def get_current_ticker(self):
        return self._current_ticker

    def get_current_book(self):
        result = {
            'sequence': self._sequence,
            'asks': [],
            'bids': [],
        }
        for ask in self._asks:
            try:
                # There can be a race condition here, where a price point is removed
                # between these two ops
                this_ask = self._asks[ask]
            except KeyError:
                continue
            for order in this_ask:
                result['asks'].append([order['price'], order['size'], order['id']])
        for bid in self._bids:
            try:
                # There can be a race condition here, where a price point is removed
                # between these two ops
                this_bid = self._bids[bid]
            except KeyError:
                continue

            for order in this_bid:
                result['bids'].append([order['price'], order['size'], order['id']])
        return result

    def get_ask(self):
        return self._asks.min_key()

    def get_asks(self, price):
        return self._asks.get(price)

    def remove_asks(self, price):
        self._asks.remove(price)

    def set_asks(self, price, asks):
        self._asks.insert(price, asks)

    def get_bid(self):
        return self._bids.max_key()

    def get_bids(self, price):
        return self._bids.get(price)

    def remove_bids(self, price):
        self._bids.remove(price)

    def set_bids(self, price, bids):
        self._bids.insert(price, bids)


    def get_bars(self, pair, qty, granularity=60, end=None):
        """ get qty 1 minute bars for pair
        pair -- the currency pair
        qty -- number of periods to fetch
        granularity -- the number of seconds to include
        end -- the end time for the bars, in utc """
        end = end or datetime.datetime.utcnow()
        start = end - datetime.timedelta(minutes=qty)
        res = self.authclient.get_product_historic_rates(pair, start, end, granularity=60)
        bars = pd.DataFrame(res, columns=['open_time', 'low', 'high', 'open', 'close', 'volume'])
        bars['open_time'] = pd.to_datetime(bars['open_time'], unit='s')
        bars['close_time'] = bars['open_time'].shift()
        first_close = bars.loc[bars.index[0], 'open_time'] + pd.Timedelta('%s' % granularity)
        bars.loc[bars.index[0], 'close_time'] = first_close
        bars.index = bars['close_time']
        bars = bars.sort_index()

        return bars
