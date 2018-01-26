#!/usr/bin/env python
# Simple proof of concept that connects to GDAX, builds
# bars, and can place orders when certain conditions are met


import sys
import logging
import argparse
import configparser
import gdax
import time
import datetime
import decimal
import asyncio
import pandas as pd

class GDAXWebsocketClient(gdax.WebsocketClient):
    def on_open(self):
        self.message_count = 0
        self.init_bar()
        logging.debug("on_open")

    def init_bar(self):
        self.high_price = decimal.MIN_EMIN
        self.low_price = decimal.MAX_EMAX
        self.open_price = None
        self.close_price = None
        self.prev_close_price = None
        self.volume = decimal.Decimal(0)

    def get_bar(self):
        if self.close_price is None:
            self.close_price = self.prev_close_price
        if self.high_price == decimal.MIN_EMIN:
            self.high_price = self.close_price
        if self.low_price == decimal.MAX_EMAX:
            self.low_price = self.close_price
        bar = { 'open': self.open_price,
                'high': self.high_price,
                'low': self.low_price,
                'close': self.close_price,
                'volume': self.volume}
        self.prev_close_price = self.close_price
        self.init_bar()
        return bar

    def on_message(self, msg):
        self.message_count += 1
        if msg['type'] == 'error':
            logging.error(msg['message'])
        elif msg['type'] == 'subscriptions':
            logging.info('Subscriptions')
            for _ in msg['channels']:
                logging.info('%s - products: %s', _['name'],
                        ",".join(_['product_ids']))
        elif msg['type'] == 'heartbeat':
            logging.info('heartbeat at %s' % msg['time'])
        elif msg['type'] == 'match':
            # for now, let's just look at trades
            self.close_price = decimal.Decimal(msg['price'])
            if self.close_price > self.high_price:
                self.high_price = self.close_price
            if self.close_price < self.low_price:
                self.low_price = self.close_price
            if self.open_price is None:
                self.open_price = self.close_price
            self.volume += decimal.Decimal(msg['size'])
        elif msg['type'] in ['change', 'open', 'received', 'ticker', 'done']:
            # we'll ignore these for now
            pass
        else:
            logging.debug(msg['type'])

    def on_close(self):
        logging.info("on_close")


def parse_config(config):
    """ parses the data from the config """
    cparser = configparser.ConfigParser()
    cparser.read_file(config)
    try:
        key = cparser['keys']['key']
        secret = cparser['keys']['secret']
        passphrase = cparser['keys']['passphrase']
        api = cparser['uris']['api']
        wsapi = cparser['uris']['wsapi']
    except KeyError as ke:
        logging.error(ke)
        raise ke
    return (key, secret, passphrase, api, wsapi)

def get_bars(client, pair, qty):
    """ get qty 1 minute bars for pair """
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(minutes=qty)
    res = client.get_product_historic_rates(pair, start, end, granularity=60)
    df = pd.DataFrame(res, columns=['open_time', 'low', 'high', 'open', 'close', 'volume'])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='s')
    df['close_time'] = df['open_time'].shift()
    df.loc[df.index[0], 'close_time'] = df.loc[df.index[0], 'open_time'] + pd.Timedelta('1m')
    df.index = df['close_time']
    df = df.sort_index()
    
    return df

def main(args):
    (key, secret, passphrase, api, wsapi) = parse_config(args.config)

    pc = gdax.PublicClient(api)
    products = pc.get_products()
    product_map = dict(zip([_['id'] for _ in products], products))
    if args.pair not in product_map:
        print("Pair not found in products")
        sys.exit(1)

    bars = get_bars(pc, args.pair, 100)
    #ac = gdax.AuthenticatedClient(key, secret, passphrase, api)
    #accounts = ac.get_accounts()

    # auth is broken in gdax-python
    #wsClient = GDAXWebsocketClient(url=wsapi, products=[args.pair], auth=True,
    #                               api_key=key, api_secret=secret, api_passphrase=passphrase)

    wsClient = GDAXWebsocketClient(url=wsapi, products=[args.pair],
                                   auth=False)
    logging.debug("starting ws client")
    wsClient.start()
    
    def on_bar(wsClient, bars, loop):
        logging.debug("message_count = %s", wsClient.message_count)
        bar = wsClient.get_bar()
        # append to bars
        now = datetime.datetime.utcnow()
        td = datetime.timedelta(seconds=now.second,
                                microseconds=now.microsecond)
        close_time = now - td
        open_time = close_time - datetime.timedelta(minutes=1)
        bar['close_time'] = close_time
        bar['open_time'] = open_time
        bars = pd.concat([bars, pd.DataFrame(bar, index=[close_time])])
       
        ema1 = bars['close'].ewm(span=12).mean()[-1]
        ema2 = bars['close'].ewm(span=26).mean()[-1]
        logging.info("close: %s ema1: %s ema2: %s",
                     bars['close'][-1], ema1, ema2)
        if ema1 > ema2:
            logging.info("long")
        else:
            logging.info("short")

        logging.info(bars.tail())
        td = datetime.timedelta(seconds=60-now.second,
                                microseconds=now.microsecond)
        handle = loop.call_at(loop.time() + td.total_seconds(), on_bar,
                              wsClient, bars, loop)

    loop = asyncio.get_event_loop()
    now = datetime.datetime.now()
    td = datetime.timedelta(seconds=60-now.second,
                            microseconds=now.microsecond)
    handle = loop.call_at(loop.time() + td.total_seconds(), on_bar,
                          wsClient, bars, loop)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info("Quitting")
        handle.cancel()
        wsClient.close()
        loop.stop()
        sys.exit(0)
    loop.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simple GDAX market maker')
    parser.add_argument('config',
                        type=argparse.FileType('r'), 
                        help='path to a config containing keys and urls')
    parser.add_argument('pair', help='currency pair to trade')
    parser.add_argument('-d', '--debug', help='enable debug logging', action='store_true')

    args = parser.parse_args(sys.argv[1:])
    if args.debug:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.debug(args)
    main(args)

