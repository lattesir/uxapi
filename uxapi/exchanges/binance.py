import json
import bisect
import asyncio

import pendulum
from yarl import URL

from uxapi import register_exchange
from uxapi import UXSymbol
from uxapi import UXPatch
from uxapi import Session
from uxapi import WSHandler
from uxapi import Awaitables
from uxapi.exchanges.ccxt.binance import binance
from uxapi.helpers import deep_extend, is_sorted, contract_delivery_time


@register_exchange('binance')
class Binance(UXPatch, binance):
    def __init__(self, market_type, config=None):
        return super().__init__(market_type, deep_extend({
            'options': {
                'defaultType': market_type,
            }
        }, config or {}))

    def describe(self):
        return self.deep_extend(super().describe(), {
            'deliveryHourUTC': 8,

            'urls': {
                'wsapi': {
                    'market': 'wss://stream.binance.com:9443/stream',
                    'private': 'wss://stream.binance.com:9443/ws',
                    'dapiMarket': 'wss://dstream.binance.com/stream',
                    'dapiPrivate': 'wss://dstream.binance.com/ws',
                    'fapiMarket': 'wss://fstream.binance.com/stream',
                    'fapiPrivate': 'wss://fstream.binance.com/ws',
                }
            },

            'wsapi': {
                'market': {
                    'orderbook': '{symbol}@depth{level_speed}',
                    'ohlcv': '{symbol}@kline_{period}',
                    'trade': '{symbol}@trade',
                    'aggTrade': '{symbol}@aggTrade',
                    'ticker': '{symbol}@ticker',
                    '!ticker': '!ticker@arr',
                    'miniTicker': '{symbol}@miniTicker',
                    '!miniTicker': '!miniTicker@arr',
                    'quote': '{symbol}@bookTicker',
                    '!quote': '!bookTicker',
                },
                'private': {'private': 'private'},
                'dapiMarket': {
                    'aggTrade': '{symbol}@aggTrade',
                    'indexPrice': '{pair}@indexPrice{speed_in_seconds}',
                    'markPrice': '{symbol}@markPrice{speed_in_seconds}',
                    'markPriceOfPair': '{pair}@markPrice{speed_in_seconds}',
                    'ohlcv': '{symbol}@kline_{period}',
                    'continuousKline': '{pair_contractType}@continuousKline_{period}',
                    'indexPriceKline': '{pair}@indexPriceKline_{period}',
                    'markPriceKline': '{symbol}@markPriceKline_{period}',
                    'miniTicker': '{symbol}@miniTicker',
                    '!miniTicker': '!miniTicker@arr',
                    'ticker': '{symbol}@ticker',
                    '!ticker': '!ticker@arr',
                    'quote': '{symbol}@bookTicker',
                    '!quote': '!bookTicker',
                    'forceOrder': '{symbol}@forceOrder',
                    '!forceOrder': '!forceOrder@arr',
                    'orderbook': '{symbol}@depth{level_speed}',
                },
                'dapiPrivate': {'private': 'private'},
                'fapiMarket': {
                    'orderbook': '{symbol}@depth{level_speed}',
                    'ohlcv': '{symbol}@kline_{period}',
                    'aggTrade': '{symbol}@aggTrade',
                    'markPrice': '{symbol}@markPrice{speed_in_seconds}',
                    '!markPrice': '!markPrice@arr@{speed_in_seconds}',
                    'miniTicker': '{symbol}@miniTicker',
                    'ticker': '{symbol}@ticker',
                    'quote': '{symbol}@bookTicker',
                    '!quote': '!bookTicker',
                },
                'fapiPrivate': {'private': 'private'},
            }
        })

    def _fetch_markets(self, params=None):
        markets = super()._fetch_markets(params)
        for market in markets:
            if market['type'] == 'futures':
                market['contractValue'] = market['info']['contractSize']
                timestamp = self.safe_integer(market['info'], 'deliveryDate')
                delivery_time = pendulum.from_timestamp(timestamp / 1000)
                market['deliveryTime'] = delivery_time.to_iso8601_string()
            elif market['type'] == 'swap':
                market['contractValue'] = 1
        return markets

    def order_book_merger(self):
        return BinanceOrderBookMerger(self)

    def wshandler(self, topic_set):
        wsapi_types = set(self.wsapi_type(topic) for topic in topic_set)
        if len(wsapi_types) > 1:
            raise ValueError('invalid topic_set')
        wsapi_type = wsapi_types.pop()
        wsurl = self.urls['wsapi'][wsapi_type]
        return BinanceWSHandler(self, wsurl, topic_set, wsapi_type)

    def wsapi_type(self, uxtopic):
        if uxtopic.market_type == 'futures':
            prefix = 'dapi'
        elif uxtopic.market_type == 'swap':
            prefix = 'fapi'
        else:
            prefix = ''

        if uxtopic.maintype == 'private':
            wstype = 'private'
        else:
            wstype = 'market'

        if prefix:
            return f'{prefix}{wstype[0].upper()}{wstype[1:]}'
        else:
            return wstype

    def convert_symbol(self, uxsymbol):
        if uxsymbol.market_type == 'spot':
            return uxsymbol.name

        if uxsymbol.market_type == 'futures':
            delivery_time = contract_delivery_time(
                expiration=uxsymbol.contract_expiration,
                delivery_hour=self.deliveryHourUTC)
            return f'{uxsymbol.base}{uxsymbol.quote}_{delivery_time:%y%m%d}'

        if uxsymbol.market_type == 'swap':
            return f'{uxsymbol.quote}/{uxsymbol.base}'

        raise ValueError(f'invalid symbol: {uxsymbol}')

    def convert_topic(self, uxtopic):
        maintype = uxtopic.maintype
        subtypes = uxtopic.subtypes
        wsapi_type = self.wsapi_type(uxtopic)
        template = self.wsapi[wsapi_type][maintype]

        params = {}
        if uxtopic.extrainfo:
            exchange_id, market_type, _, extrainfo = uxtopic
            uxsymbol = UXSymbol(exchange_id, market_type, extrainfo)
            if maintype == 'continuousKline':
                params['pair_contractType'] = extrainfo
            elif maintype in ('indexPrice', 'markPriceOfPair', 'indexPriceKline'):
                params['pair'] = extrainfo
            else:
                params['symbol'] = self.market_id(uxsymbol).lower()

        if maintype == 'orderbook':
            if not subtypes:
                level_speed = '20@100ms'
            elif subtypes[0] == 'full':
                level_speed = '@0ms' if self.market_type == 'swap' else '@100ms'
            else:
                level_speed = subtypes[0]
            params['level_speed'] = level_speed

        if maintype in ('ohlcv', 'continuousKline', 'indexPriceKline', 'markPriceKline'):
            if subtypes:
                params['period'] = self.timeframes[subtypes[0]]
            else:
                params['period'] = '1m'

        if maintype in ('indexPrice', 'markPrice', '!markPrice'):
            if subtypes and subtypes[0] == '1s':
                speed_in_seconds = '@1s'
            else:
                speed_in_seconds = ''
            params['speed_in_seconds'] = speed_in_seconds

        return template.format(**params)


class BinanceWSHandler(WSHandler):
    def __init__(self, exchange, wsurl, topic_set, wsapi_type):
        super().__init__(exchange, wsurl, topic_set)
        self.wsapi_type = wsapi_type
        self.listen_key = None

    async def connect(self):
        if not self.session:
            self.session = Session()
            self.own_session = True

        if self.login_required:
            result = await self.request_listen_key('POST')
            self.listen_key = result['listenKey']
            wsurl = URL(f'{self.wsurl}/{self.listen_key}')
        else:
            topic_set = [self.convert_topic(topic) for topic in self.topic_set]
            query = {'streams': '/'.join(topic_set)}
            wsurl = URL(self.wsurl).with_query(query)
        ws = await self.session.ws_connect(str(wsurl))
        return ws

    async def request_listen_key(self, method, params=None):
        if self.wsapi_type == 'private':
            baseurl = self.exchange.urls['api']['public']
            url = f'{baseurl}/userDataStream'
        elif self.wsapi_type in ('dapiPrivate', 'fapiPrivate'):
            baseurl = self.exchange.urls['api'][self.wsapi_type]
            url = f'{baseurl}/listenKey'
        else:
            raise RuntimeError(f'invalid wsapi_type: {self.wsapi_type}')
        credentials = self.get_credentials()
        headers = {'X-MBX-APIKEY': credentials['apiKey']}
        async with self.session.request(method, url, params=params, headers=headers) as resp:
            result = await resp.json()
        return result

    @property
    def login_required(self):
        return 'private' in self.wsapi_type.lower()

    def prepare(self):
        if self.login_required:
            self.awaitables.create_task(self.keepalive(), 'keepalive')

    async def keepalive(self):
        interval = 20 * 60
        while True:
            await asyncio.sleep(interval)
            params = None
            if self.wsapi_type == 'private':
                params = {'listenKey': self.listen_key}
            try:
                await self.request_listen_key('PUT', params)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception('request listen key failed')

    def decode(self, data):
        return json.loads(data)


class BinanceOrderBookMerger:
    def __init__(self, exchange):
        self.exchange = exchange
        self.snapshot = None
        self.cache = []
        self.future = None
        self.prices = None

    def __call__(self, patch):
        if self.snapshot:
            self.merge(patch)
            return self.snapshot

        self.cache.append(patch)
        if not self.future:
            self.future = Awaitables.default().run_in_executor(
                self.fetch_order_book, patch['data']['s'])
        if not self.future.done():
            raise StopIteration

        snapshot = self.future.result()
        self.future = None
        array = [patch['data']['u'] for patch in self.cache]
        i = bisect.bisect_right(array, snapshot['lastUpdateId'])
        self.cache = self.cache[i:]
        self.on_snapshot(snapshot)
        return self.snapshot

    def on_snapshot(self, snapshot):
        self.snapshot = snapshot
        self.snapshot['lastUpdateId'] = None
        self.prices = {
            'asks': [float(item[0]) for item in snapshot['asks']],
            'bids': [-float(item[0]) for item in snapshot['bids']]
        }
        assert is_sorted(self.prices['asks'])
        assert is_sorted(self.prices['bids'])
        for patch in self.cache:
            self.merge(patch)
        self.cache = None

    def merge(self, patch):
        data = patch['data']
        lastUpdateId = self.snapshot['lastUpdateId']
        if lastUpdateId:
            if 'pu' in data:   # binance futures
                if data['pu'] != lastUpdateId:
                    raise ValueError('invalid patch')
            else:   # binance spot
                if data['U'] != lastUpdateId + 1:
                    raise ValueError('invalid patch')
        self.snapshot['lastUpdateId'] = data['u']
        self.merge_asks_bids(self.snapshot['asks'], data['a'],
                             self.prices['asks'], False)
        self.merge_asks_bids(self.snapshot['bids'], data['b'],
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

    def fetch_order_book(self, symbol):
        params = {
            'symbol': symbol,
            'limit': 1000,
        }
        market_type = self.exchange.market(symbol)['type']
        method = self.exchange.method_by_type('publicGetDepth', market_type)
        return getattr(self.exchange, method)(params)
