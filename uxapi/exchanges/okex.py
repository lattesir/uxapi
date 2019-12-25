import json
import zlib
import time
import asyncio
import bisect
from itertools import zip_longest, chain
import binascii

import ccxt
import pendulum

from uxapi import register
from uxapi import UXSymbol
from uxapi import WSHandler
from uxapi import UXPatch
from uxapi.helpers import (
    hmac,
    extend,
    all_equal,
    contract_delivery_time,
    to_timestamp,
    is_sorted
)


@register
class Okex(UXPatch, ccxt.okex3):
    id = 'okex'
    
    def __init__(self, market_type, config=None):
        return super().__init__(market_type, extend({
            'options': {
                'defaultType': market_type,
            }
        }, config or {}))

    def describe(self):
        return self.deep_extend(super().describe(), {
            'deliveryHourUTC': 8,

            'has': {
                'fetchOpenOrders': True,
                'fetchClosedOrders': True,
                'fetchMyTrades': False,
            },

            'urls': {
                'wsapi': 'wss://real.okex.com:8443/ws/v3',
            },

            'wsapi': {
                'spot': {
                    'ticker': 'spot/ticker:{symbol}',
                    'orderbook': 'spot/depth{level}:{symbol}',
                    'ohlcv': 'spot/candle{period_in_sec}s:{symbol}',
                    'trade': 'spot/trade:{symbol}',
                    'myorder': 'spot/order:{symbol}',
                    'account': 'spot/account:{currency}',
                    'margin_account': 'spot/margin_account:{symbol}',
                },
                'futures': {
                    'ticker': 'futures/ticker:{symbol}',
                    'orderbook': 'futures/depth{level}:{symbol}',
                    'ohlcv': 'futures/candle{period_in_sec}s:{symbol}',
                    'trade': 'futures/trade:{symbol}',
                    'myorder': 'futures/order:{symbol}',
                    'position': 'futures/position:{symbol}',
                    'account': 'futures/account:{currency}',
                    'estimated_price': 'futures/estimated_price:{symbol}',
                    'instruments': 'futures/instruments',
                    'price_range': 'futures/price_range:{symbol}',
                    'mark_price': 'futures/mark_price:{symbol}',
                },
                'swap': {
                    'ticker': 'swap/ticker:{symbol}',
                    'orderbook': 'swap/depth{level}:{symbol}',
                    'ohlcv': 'swap/candle{period_in_sec}s:{symbol}',
                    'trade': 'swap/trade:{symbol}',
                    'myorder': 'swap/order:{symbol}',
                    'position': 'swap/position:{symbol}',
                    'account': 'swap/account:{symbol}',
                    'order_algo': 'swap/order_algo:{symbol}',
                    'funding_rate': 'swap/funding_rate:{symbol}',
                    'price_range': 'swap/price_range:{symbol}',
                    'mark_price': 'swap/mark_price:{symbol}',
                },
                'index': {
                    'ticker': 'index/ticker:{symbol}',
                    'ohlcv': 'index/candle{period_in_sec}s:{symbol}',
                }
            }
        })

    def _fetch_markets(self, params=None):
        markets = super()._fetch_markets(params)
        for market in markets:
            contract_value = self.safe_float(market['info'], 'contract_val')
            if contract_value:
                market['contractValue'] = contract_value
            if market['type'] == 'futures':
                delivery_date = self.safe_string(market['info'], 'delivery')
                if delivery_date:
                    delivery_time = pendulum.from_format(delivery_date, 'YYYY-MM-DD')
                    delivery_time = delivery_time.add(hours=self.deliveryHourUTC)
                    market['deliveryTime'] = delivery_time.to_iso8601_string()
                else:
                    market['deliveryTime'] = None
        return markets

    def _create_order(self, uxsymbol, type, side, amount, price, params):
        if uxsymbol.market_type in ('futures', 'swap') and type == 'limit':
            if side == 'buy':
                type = '1'     # open long
            elif side == 'sell':
                type = '2'     # open short
            else:
                raise ValueError('invalid side argument')
        return super()._create_order(uxsymbol, type, side, amount, price, params)

    def order_book_merger(self):
        return OkexOrderBookMerger()

    def wshandler(self, topic_set):
        return OkexWSHandler(self, self.urls['wsapi'], topic_set)
        
    def convert_symbol(self, uxsymbol):
        if uxsymbol.market_type == 'spot':
            return uxsymbol.name
        if uxsymbol.market_type == 'swap':
            return f'{uxsymbol.base}-{uxsymbol.quote}-SWAP'
        if uxsymbol.market_type == 'futures':
            delivery_time = contract_delivery_time(
                expiration=uxsymbol.contract_expiration,
                delivery_hour=self.deliveryHourUTC)
            if uxsymbol.base == 'USDT':
                return f'{uxsymbol.quote}-{uxsymbol.base}-{delivery_time:%y%m%d}'
            elif uxsymbol.quote == 'USD':
                return f'{uxsymbol.base}-{uxsymbol.quote}-{delivery_time:%y%m%d}'
            else:
                raise ValueError(f'invalid symbol: {uxsymbol}')
        if uxsymbol.market_type == 'index':
            return f'{uxsymbol.base}-{uxsymbol.quote}'
        raise ValueError(f'unknown market_type: {uxsymbol.market_type}')

    def convert_topic(self, uxtopic):
        maintype = uxtopic.maintype
        subtypes = uxtopic.subtypes
        template = self.wsapi[self.market_type][maintype]
        
        if maintype == 'account':
            return template.format(currency=uxtopic.extrainfo)
        elif maintype == 'instruments':
            return template
        else:
            params = {}
            uxsymbol = UXSymbol(uxtopic.exchange_id, uxtopic.market_type,
                                uxtopic.extrainfo)
            params['symbol'] = self.market_id(uxsymbol)
            if maintype == 'ohlcv':
                params['period_in_sec'] = self.timeframes[subtypes[0]]
            if maintype == 'orderbook':
                params['level'] = subtypes[0] if subtypes else '5'
                if params['level'] == 'full':
                    params['level'] = ''
            return template.format(**params)


class OkexWSHandler(WSHandler):
    def on_connected(self):
        self.pre_processors.append(self.on_error_message)

    def on_error_message(self, msg):
        if msg.get('event') == 'error':
            err_msg = msg['message']
            err_code = msg['errorCode']
            raise RuntimeError(f'{err_msg}({err_code})')
        else:
            return msg

    def create_keepalive_task(self):
        self.last_message_timestamp = time.time()
        return super().create_keepalive_task()

    async def keepalive(self):
        interval = 10
        while True:
            await asyncio.sleep(interval)
            now = time.time()
            if now - self.last_message_timestamp >= interval:
                await self.send('ping')

    def on_keepalive_message(self, msg):
        self.last_message_timestamp = time.time()
        if msg == 'pong':
            raise StopIteration
        else:
            return msg

    @property
    def login_required(self):
        private_types = {'myorder', 'position', 'account', 'margin_account'}
        topic_types = {topic.maintype for topic in self.topic_set}
        for type in private_types:
            if type in topic_types:
                return True
        return False

    async def login(self, credentials):
        server_timestamp = await self.fetch_server_timestamp()
        await self.send(self.login_command(server_timestamp, credentials))

    async def fetch_server_timestamp(self):
        url = 'http://www.okex.com/api/general/v3/time'
        async with self.session.get(url) as resp:
            result = await resp.json()
        return result['epoch']

    def login_command(self, server_timestamp, credentials):
        payload = server_timestamp + 'GET' + '/users/self/verify'
        signature = hmac(
            bytes(credentials['secret'], 'utf8'),
            bytes(payload, 'utf8'),
        )
        return {
            'op': 'login',
            'args': [
                credentials['apiKey'],
                credentials['password'],
                server_timestamp,
                signature.decode(),
            ]
        }

    def on_login_message(self, msg):
        if msg.get('event') == 'login':
            self.logger.info(f'logged in')
            self.on_logged_in()
            raise StopIteration
        else:
            return msg

    def subscribe_commands(self, topic_set):
        command = {
            'op': 'subscribe',
            'args': list(topic_set),
        }
        return [command]

    def on_subscribe_message(self, msg):
        if msg.get('event') == 'subscribe':
            topic = msg['channel']
            self.logger.info(f'{topic} subscribed')
            self.on_subscribed(topic)
            raise StopIteration
        else:
            return msg

    def decode(self, data):
        bytes_ = zlib.decompress(data, wbits=-zlib.MAX_WBITS)
        msg = bytes_.decode()
        try:
            jsonmsg = json.loads(msg)
        except json.JSONDecodeError:
            return msg
        else:
            return jsonmsg


class OkexOrderBookMerger:
    def __init__(self):
        self.snapshot = None
        self.prices = None

    def __call__(self, msg):
        self.merge(msg['action'], msg['data'])
        return self.snapshot

    def merge(self, action, patches):
        if action == 'partial':
            tsgetter = lambda p: to_timestamp(p['timestamp'])
            assert is_sorted(patches, key=tsgetter)
            self.snapshot = patches[-1]
            self.prices = {
                'asks': [float(item[0]) for item in self.snapshot['asks']],
                'bids': [-float(item[0]) for item in self.snapshot['bids']]
            }
            assert is_sorted(self.prices['asks'])
            assert is_sorted(self.prices['bids'])
        elif action == 'update':
            for patch in patches:
                self.do_merge(patch)
        else:
            raise ValueError('invalid action')
        self.validate()

    def do_merge(self, patch):
        self.snapshot['timestamp'] = patch['timestamp']
        self.snapshot['checksum'] = patch['checksum']
        self.merge_asks_bids('asks', patch['asks'])
        self.merge_asks_bids('bids', patch['bids'])

    def merge_asks_bids(self, key, patch):
        for item in patch:
            if key == 'asks':
                price = float(item[0])
            else:
                price = -float(item[0])
            amount = float(item[1])
            array = self.prices[key]
            i = bisect.bisect_left(array, price)
            if i < len(array) and array[i] == price:
                if amount == 0:
                    array.pop(i)
                    self.snapshot[key].pop(i)
                else:
                    self.snapshot[key][i] = item
            else:
                if amount != 0:
                    array.insert(i, price)
                    self.snapshot[key].insert(i, item)

    def validate(self):
        asks = (item[:2] for item in self.snapshot['asks'][:25])
        bids = (item[:2] for item in self.snapshot['bids'][:25])
        items = filter(None, chain(*zip_longest(bids, asks)))
        text = ':'.join(chain(*items))
        crc32 = binascii.crc32(text.encode())
        checksum = self.snapshot['checksum'] & 0xffffffff
        if crc32 != checksum:
            raise RuntimeError('invalid order book data')