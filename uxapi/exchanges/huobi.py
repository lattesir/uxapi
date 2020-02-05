import datetime
import asyncio
import gzip
import json
import collections
import urllib.parse
import bisect

import yarl

import pendulum
from ccxt import huobipro
from uxapi import register
from uxapi import UXSymbol
from uxapi import WSHandler
from uxapi import UXPatch
from uxapi import Queue
from uxapi import Awaitables
from uxapi.exchanges.ccxt import huobidm
from uxapi.helpers import (
    all_equal,
    keysort,
    hmac,
    extend,
    is_sorted
)


@register
class Huobi:
    id = 'huobi'

    def __init__(self, market_type, config):
        if market_type == 'spot':
            cls = Huobipro
        elif market_type == 'futures':
            cls = Huobidm
        else:
            raise ValueError('invalid market_type: {market_type}')
        self._exchange = cls(market_type, config)

    def __getattr__(self, attr):
        return getattr(self._exchange, attr)

    def wshandler(self, topic_set):
        wsapi_types = {self.wsapi_type(topic) for topic in topic_set}
        if len(wsapi_types) > 1:
            raise ValueError('invalid topics')
        wsapi_type = wsapi_types.pop()
        wsurl = self.urls['wsapi'][wsapi_type]
        return HuobiWSHandler(self, wsurl, topic_set, wsapi_type)


class Huobipro(UXPatch, huobipro):
    id = 'huobi'

    def describe(self):
        return self.deep_extend(super().describe(), {
            'has': {
                'fetchCurrencies': True,
                'fetchOrders': True,
                'fetchOpenOrders': True,
                'cancelOrders': True,
                'cancelAllOrders': True,
            },

            'api': {
                'private': {
                    'post': 'order/orders/batchCancelOpenOrders',
                }
            },

            'urls': {
                'wsapi': {
                    'market': 'wss://api.huobi.pro/ws',
                    'private': 'wss://api.huobi.pro/ws/v1',
                },
            },

            'wsapi': {
                'market': {
                    'ticker': 'market.{symbol}.detail',
                    'ohlcv': 'market.{symbol}.kline.{period}',
                    'orderbook': 'market.{symbol}.depth.{level}',
                    'mbp': 'market.{symbol}.mbp.{level}',
                    'trade': 'market.{symbol}.trade.detail',
                    'bbo': 'market.{symbol}.bbo',
                },
                'private': {
                    'accounts': 'accounts',
                    'myorder': 'orders.{symbol}.update',
                    'myorder_deprecated': 'orders.{symbol}',
                }
            },
        })
 
    def order_book_merger(self):
        return HuobiproOrderBookMerger(self)

    def _fetch_markets(self, params=None):
        markets = super()._fetch_markets(params)
        for market in markets:
            market['type'] = 'spot'
        return markets

    def _cancel_orders(self, ids, uxsymbol, params):
        params = params or {}
        request = {
            'order-ids': ids,
        }
        return self.privatePostOrderOrdersBatchcancel(self.extend(request, params))

    def _cancel_all_orders(self, uxsymbol, params):
        params = params or {}
        request = {}
        if not self.safe_string(params, 'account-id'):
            self.loadAccounts()
            accounts = {item['type']: item['id'] for item in self.accounts}
            request['account-id'] = self.safe_string(accounts, 'spot')
        if not self.safe_string(params, 'symbol'):
            if uxsymbol:
                request['symbol'] = self.convert_symbol(uxsymbol)
        return self.privatePostOrderOrdersBatchCancelOpenOrders(
            self.extend(request, params))

    def convert_topic(self, uxtopic):
        maintype = uxtopic.maintype
        subtypes = uxtopic.subtypes
        wsapi_type = self.wsapi_type(uxtopic)
        template = self.wsapi[wsapi_type][maintype]
        if maintype == 'accounts':
            return template
        params = {}
        uxsymbol = UXSymbol(uxtopic.exchange_id, uxtopic.market_type,
                            uxtopic.extrainfo)
        params['symbol'] = self.market_id(uxsymbol)
        if maintype in ['orderbook', 'mbp']:
            if not subtypes:
                assert maintype == 'orderbook'
                params['level'] = 'step0'
            elif subtypes[0] == 'full':
                assert maintype == 'orderbook'
                template = self.wsapi[wsapi_type]['mbp']
                params['level'] = '150'
            else:
                params['level'] = subtypes[0]
        if maintype == 'ohlcv':
            params['period'] = self.timeframes[subtypes[0]]
        return template.format(**params)

    def wsapi_type(self, uxtopic):
        for type in self.wsapi:
            if uxtopic.maintype in self.wsapi[type]:
                return type
        raise ValueError('invalid topic')


class _HuobiOrderBookMerger:
    def merge_asks_bids(self, snapshot_lst, patch_lst, price_lst, negative_price):
        for item in patch_lst:
            price, amount = item
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


class HuobiproOrderBookMerger(_HuobiOrderBookMerger):
    def __init__(self, exchange):
        self.exchange = exchange
        self.snapshot = None
        self.topic = None
        self.wsreq = None
        self.wsreq_task = None
        self.future = None
        self.cache = []
        self.prices = None

    def __call__(self, patch):
        if self.snapshot:
            self.merge(patch)
            return self.snapshot

        if self.wsreq is None:
            self.topic = patch['ch']
            self.start_wsreq()
            
        self.cache.append(patch)
        if not self.future:
            self.future = self.wsreq.request({
                'req': self.topic
            })
        if not self.future.done():
            raise StopIteration

        snapshot = self.future.result()
        self.future = None
        seqnums = [item['tick']['prevSeqNum'] for item in self.cache]
        snapshot_seq = snapshot['data']['seqNum']
        assert is_sorted(seqnums)
        i = bisect.bisect_left(seqnums, snapshot_seq)
        if i != len(seqnums) and seqnums[i] == snapshot_seq:
            self.stop_wsreq()
            self.cache = self.cache[i:]
            self.on_snapshot(snapshot)
            return self.snapshot
        raise StopIteration

    def on_snapshot(self, snapshot):
        self.snapshot = snapshot
        self.prices = {
            'asks': [item[0] for item in snapshot['data']['asks']],
            'bids': [-item[0] for item in snapshot['data']['bids']]
        }
        for patch in self.cache:
            self.merge(patch)
        self.cache = None

    def merge(self, patch):
        self.snapshot['ts'] = patch['ts']
        snapshot_data = self.snapshot['data']
        patch_tick = patch['tick']
        if snapshot_data['seqNum'] != patch_tick['prevSeqNum']:
            raise RuntimeError('seqNum error')
        snapshot_data['seqNum'] = patch_tick['seqNum']
        self.merge_asks_bids(snapshot_data['asks'], patch_tick['asks'],
                             self.prices['asks'], False)
        self.merge_asks_bids(snapshot_data['bids'], patch_tick['bids'],
                             self.prices['bids'], True)

    def start_wsreq(self):
        self.wsreq = HuobiWSReq(self.exchange, 'market')

        async def run():
            try:
                await self.wsreq.run()
            except asyncio.CancelledError:
                pass

        self.wsreq_task = Awaitables.default().create_task(run(), 'wsreq')

    def stop_wsreq(self):
        self.wsreq_task.cancel()
        self.wsreq_task = None
        self.wsreq = None


class Huobidm(UXPatch, huobidm):
    id = 'huobi'

    def describe(self):
        return self.deep_extend(super().describe(), {
            'deliveryHourUTC': 8,

            'has': {
                'fetchOrders': True,
                'fetchOpenOrders': True,
                'cancelOrders': True,
                'cancelAllOrders': True,
            },

            'urls': {
                'wsapi': {
                    'market': 'wss://www.hbdm.com/ws',
                    'private': 'wss://api.hbdm.com/notification',
                },
            },

            'wsapi': {
                'market': {
                    'ticker': 'market.{symbol}.detail',
                    'orderbook': 'market.{symbol}.depth.{level}',
                    'high_freq': 'market.{symbol}.depth.size_{level}.high_freq',
                    'ohlcv': 'market.{symbol}.kline.{period}',
                    'trade': 'market.{symbol}.trade.detail',
                },
                'private': {
                    'myorder': 'orders.{currency}',
                    'position': 'positions.{currency}',
                    'accounts': 'accounts.{currency}',
                }
            },
        })

    def order_book_merger(self):
        return HuobidmOrderBookMerger()

    def _fetch_markets(self, params=None):
        markets = super()._fetch_markets(params)
        for market in markets:
            market['type'] = 'futures'
            contract_value = self.safe_float(market['info'], 'contract_size')
            market['contractValue'] = contract_value
            delivery_date = self.safe_string(market['info'], 'delivery_date')
            if delivery_date:
                delivery_time = pendulum.from_format(delivery_date, 'YYYYMMDD')
                delivery_time = delivery_time.add(hours=self.deliveryHourUTC)
                market['deliveryTime'] = delivery_time.to_iso8601_string()
            else:
                market['deliveryTime'] = None
        return markets

    def convert_symbol(self, uxsymbol):
        return f'{uxsymbol.base}_{uxsymbol.contract_expiration}'

    def convert_topic(self, uxtopic):
        maintype = uxtopic.maintype
        subtypes = uxtopic.subtypes
        params = {}
        if maintype in self.wsapi['market']:
            uxsymbol = UXSymbol(uxtopic.exchange_id, uxtopic.market_type,
                                uxtopic.extrainfo)
            params['symbol'] = self.market_id(uxsymbol)
        else:  # 'private'
            params['currency'] = uxtopic.extrainfo.lower()

        if maintype in ['orderbook', 'high_freq']:
            if not subtypes:
                assert maintype == 'orderbook'
                params['level'] = 'step0'
            elif subtypes[0] == 'full':
                assert maintype == 'orderbook'
                maintype = 'high_freq'
                params['level'] = '150'
            else:
                params['level'] = subtypes[0]
        elif maintype == 'ohlcv':
            params['period'] = self.timeframes[subtypes[0]]

        wsapi_type = self.wsapi_type(uxtopic)
        template = self.wsapi[wsapi_type][maintype]
        return template.format(**params)

    def wsapi_type(self, uxtopic):
        for type in self.wsapi:
            if uxtopic.maintype in self.wsapi[type]:
                return type
        raise ValueError('invalid topic')


class HuobidmOrderBookMerger(_HuobiOrderBookMerger):
    def __init__(self):
        self.snapshot = None
        self.prices = None

    def __call__(self, patch):
        if patch['tick']['event'] == 'snapshot':
            self.on_snapshot(patch)
        elif patch['tick']['event'] == 'update':
            self.merge(patch)
        else:
            raise ValueError('unexpected event')
        return self.snapshot

    def on_snapshot(self, snapshot):
        self.snapshot = snapshot
        self.prices = {
            'asks': [item[0] for item in snapshot['tick']['asks']],
            'bids': [-item[0] for item in snapshot['tick']['bids']]
        }

    def merge(self, patch):
        self.snapshot['ts'] = patch['ts']
        snapshot_tick = self.snapshot['tick']
        patch_tick = patch['tick']
        if snapshot_tick['version'] + 1 != patch_tick['version']:
            raise RuntimeError('version error')
        snapshot_tick.update({
            'mrid': patch_tick['mrid'],
            'id': patch_tick['id'],
            'ts': patch_tick['ts'],
            'version': patch_tick['version'],
        })
        self.merge_asks_bids(snapshot_tick['asks'], patch_tick['asks'],
                             self.prices['asks'], False)
        self.merge_asks_bids(snapshot_tick['bids'], patch_tick['bids'],
                             self.prices['bids'], True)
        

class HuobiWSHandler(WSHandler):
    def __init__(self, exchange, wsurl, topic_set, wsapi_type):
        super().__init__(exchange, wsurl, topic_set)
        self.wsapi_type = wsapi_type

    def on_connected(self):
        if self.wsapi_type == 'private':
            self.pre_processors.append(self.on_error_message)

    def on_error_message(self, msg):
        if msg['op'] == 'close':
            raise RuntimeError('server closed')
        elif msg['op'] == 'error':
            raise RuntimeError('invalid op or inner error')
        else:
            return msg

    def create_keepalive_task(self):
        self.keepalive_msq = Queue()
        return super().create_keepalive_task()

    async def keepalive(self):
        while True:
            ping = await self.keepalive_msq.get()
            try:
                while True:
                    ping = self.keepalive_msq.get_nowait()
            except asyncio.QueueEmpty:
                pass

            if 'ping' in ping:
                # {"ping": 18212558000}
                pong = {'pong': ping['ping']}
            else:
                # {"op": "ping", "ts": 1492420473058}
                pong = {'op': 'pong', 'ts': ping['ts']}
            await self.send(pong)

    def on_keepalive_message(self, msg):
        if 'ping' in msg or msg.get('op') == 'ping':
            self.keepalive_msq.put_nowait(msg)
            raise StopIteration
        else:
            return msg

    @property
    def login_required(self):
        return self.wsapi_type == 'private'

    def login_command(self, credentials):
        now = datetime.datetime.utcnow()
        timestamp = now.isoformat(timespec='seconds')
        params = keysort({
            'SignatureMethod': 'HmacSHA256',
            'SignatureVersion': '2',
            'AccessKeyId': credentials['apiKey'],
            'Timestamp': timestamp
        })
        auth = urllib.parse.urlencode(params)
        url = yarl.URL(self.wsurl)
        payload = '\n'.join(['GET', url.host, url.path, auth])
        signature = hmac(
            bytes(credentials['secret'], 'utf8'),
            bytes(payload, 'utf8'),
        )
        request = {
            'op': 'auth',
            'Signature': signature.decode(),
        }
        if (self.exchange.market_type == 'futures'
                and self.wsapi_type == 'private'):
            request['type'] = 'api'
        return extend(request, params)

    def on_login_message(self, msg):
        if msg['op'] == 'auth':
            if msg['err-code'] == 0:
                self.logger.info(f'logged in')
                self.on_logged_in()
            else:
                err_msg = msg['err-msg']
                raise RuntimeError(f'login failed: {err_msg}')
            raise StopIteration
        else:
            return msg

    def subscribe_commands(self, topic_set):
        commands = []
        for topic in topic_set:
            if self.wsapi_type == 'private':
                request = {'op': 'sub', 'topic': topic}
            else:
                request = {'sub': topic}
                if 'depth' in topic and topic.endswith('.high_freq'):
                    request['data_type'] = 'incremental'
            commands.append(request)
        return commands

    def on_subscribe_message(self, msg):
        ok = False
        if 'status' in msg:  # market feed
            if msg['status'] == 'ok':
                ok = True
                topic = msg['subbed']
            else:
                err_msg = msg['err-msg']
        elif msg.get('op') == 'sub':  # private feed
            if msg['err-code'] == 0:
                ok = True
                topic = msg['topic']
            else:
                err_msg = msg['err-msg']
        else:
            return msg

        if ok:
            self.logger.info(f'{topic} subscribed')
            self.on_subscribed(topic)
            raise StopIteration
        else:
            raise RuntimeError(f'subscribe failed: {err_msg}')

    def decode(self, data):
        msg = gzip.decompress(data).decode()
        return json.loads(msg)


class HuobiWSReq(HuobiWSHandler):
    def __init__(self, exchange, wsapi_type):
        wsurl = exchange.urls['wsapi'][wsapi_type]
        super().__init__(exchange, wsurl, None, wsapi_type)
        self.queue = Queue()
        self.future = None
        self.timeout = 10.0  # in seconds

    def on_prepared(self):
        self.awaitables.create_task(self.sendreq(), 'sendreq')

    async def do_run(self, collector):
        await super().do_run(lambda r: self.future.set_result(r))

    async def sendreq(self):
        while True:
            self.future, req = await self.queue.get()
            await self.send(req)
            await asyncio.wait_for(self.future, self.timeout)

    def request(self, req):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.queue.put_nowait((future, req))
        return future