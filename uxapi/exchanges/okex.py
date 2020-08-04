import json
import zlib
import time
import asyncio
import bisect
from itertools import zip_longest, chain
import binascii

import pendulum

from uxapi import register_exchange
from uxapi.exchanges.ccxt.okex import okex
from uxapi import UXSymbol
from uxapi import WSHandler
from uxapi import UXPatch
from uxapi.helpers import (
    hmac,
    deep_extend,
    contract_delivery_time,
)


@register_exchange('okex')
class Okex(UXPatch, okex):
    def __init__(self, market_type, config=None):
        return super().__init__(market_type, deep_extend({
            'options': {
                'fetchMarkets': [market_type],
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
                'option': {
                    'position': 'option/position:{currency}-USD',
                    'account': 'option/account:{currency}-USD',
                    'myorder': 'option/order:{currency}-USD',
                    'instruments': 'option/instruments:{currency}-USD',
                    'summary': 'option/summary:{currency}-USD',
                    'ticker': 'option/ticker:{symbol}',
                    'orderbook': 'option/depth{level}:{symbol}',
                    'trade': 'option/trade:{symbol}',
                    'ohlcv': 'option/candle{period_in_sec}s:{symbol}',
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
            underlying = self.safe_string(market['info'], 'underlying')
            if underlying and underlying.endswith('-USDT'):
                quote, base = underlying.split('-')
                market['base'] = market['baseId'] = base
                market['quote'] = market['quoteId'] = quote

            contract_value = self.safe_float(market['info'], 'contract_val')
            if contract_value:
                market['contractValue'] = contract_value

            if market['futures']:
                delivery_time = pendulum.parse(self.safe_string(market['info'], 'delivery'))
                delivery_time = delivery_time.add(hours=self.deliveryHourUTC)
                market['deliveryTime'] = delivery_time.to_iso8601_string()
            elif market['option']:
                market['deliveryTime'] = market['info']['delivery']
        return markets

    def order_book_merger(self):
        return OkexOrderBookMerger()

    def wshandler(self, topic_set):
        return OkexWSHandler(self, self.urls['wsapi'], topic_set)

    def convert_symbol(self, uxsymbol):
        if uxsymbol.market_type == 'spot':
            return uxsymbol.name

        if uxsymbol.market_type == 'swap':
            if uxsymbol.base == 'USDT':
                return f'{uxsymbol.quote}-{uxsymbol.base}-SWAP'
            elif uxsymbol.quote == 'USD':
                return f'{uxsymbol.base}-{uxsymbol.quote}-SWAP'

        if uxsymbol.market_type == 'futures':
            delivery_time = contract_delivery_time(
                expiration=uxsymbol.contract_expiration,
                delivery_hour=self.deliveryHourUTC)
            if uxsymbol.base == 'USDT':
                return f'{uxsymbol.quote}-{uxsymbol.base}-{delivery_time:%y%m%d}'
            elif uxsymbol.quote == 'USD':
                return f'{uxsymbol.base}-{uxsymbol.quote}-{delivery_time:%y%m%d}'

        if uxsymbol.market_type == 'index':
            return f'{uxsymbol.base}-{uxsymbol.quote}'

        if uxsymbol.market_type == 'option':
            base = uxsymbol.base
            dt = pendulum.parse(uxsymbol.contract_expiration)
            expiration = dt.format('YYMMDD')
            strike_price = uxsymbol.option_strike_price
            call_or_put = uxsymbol.option_type
            return f'{base}-USD-{expiration}-{strike_price}-{call_or_put}'

        raise ValueError(f'invalid symbol: {uxsymbol}')

    def convert_topic(self, uxtopic):
        maintype = uxtopic.maintype
        subtypes = uxtopic.subtypes
        template = self.wsapi[self.market_type][maintype]

        if maintype == 'instruments':
            return template

        params = {}
        if 'currency' in template:
            params['currency'] = uxtopic.extrainfo
        else:
            exchange_id, market_type, _, extrainfo = uxtopic
            uxsymbol = UXSymbol(exchange_id, market_type, extrainfo)
            params['symbol'] = self.market_id(uxsymbol)

        if maintype == 'ohlcv':
            params['period_in_sec'] = self.timeframes[subtypes[0]]

        if maintype == 'orderbook':
            if not subtypes:
                params['level'] = '5'
            elif subtypes[0] in ('full', 'tbt'):
                params['level'] = '_l2_tbt'
            else:
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

    def __call__(self, patch):
        if patch['action'] == 'partial':
            self.on_snapshot(patch)
        elif patch['action'] == 'update':
            self.merge(patch)
        else:
            raise ValueError('unexpected action')
        self.validate()
        return self.snapshot

    def on_snapshot(self, snapshot):
        data = snapshot['data'][-1]
        self.snapshot = snapshot
        self.snapshot['data'] = [data]
        self.prices = {
            'asks': [float(item[0]) for item in data['asks']],
            'bids': [-float(item[0]) for item in data['bids']]
        }

    def merge(self, patch):
        snapshot_data = self.snapshot['data'][0]
        patch_data_list = patch['data']
        for patch_data in patch_data_list:
            snapshot_data['timestamp'] = patch_data['timestamp']
            snapshot_data['checksum'] = patch_data['checksum']
            self.merge_asks_bids(snapshot_data['asks'], patch_data['asks'],
                                 self.prices['asks'], False)
            self.merge_asks_bids(snapshot_data['bids'], patch_data['bids'],
                                 self.prices['bids'], True)

    def merge_asks_bids(self, snapshot_lst, patch_lst, price_lst, negative_price):
        for item in patch_lst:
            price, amount = float(item[0]), float(item[1])
            if negative_price:
                price = -price
            i = bisect.bisect_left(price_lst, price)
            if i != len(price_lst) and price_lst[i] == price:
                if amount == 0:
                    price_lst.pop(i)
                    snapshot_lst.pop(i)
                else:
                    snapshot_lst[i] = item
            else:
                if amount != 0:
                    price_lst.insert(i, price)
                    snapshot_lst.insert(i, item)

    def validate(self):
        data = self.snapshot['data'][0]
        asks = (item[:2] for item in data['asks'][:25])
        bids = (item[:2] for item in data['bids'][:25])
        items = filter(None, chain(*zip_longest(bids, asks)))
        text = ':'.join(chain(*items))
        crc32 = binascii.crc32(text.encode())
        checksum = data['checksum'] & 0xffffffff
        if crc32 != checksum:
            raise RuntimeError('invalid order book data')
