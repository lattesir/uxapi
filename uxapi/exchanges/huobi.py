import datetime
import asyncio
import gzip
import json
import urllib.parse
import bisect
from urllib.parse import parse_qs

import yarl

import pendulum
from ccxt import huobipro
from uxapi import register_exchange
from uxapi import UXSymbol
from uxapi import WSHandler
from uxapi import UXPatch
from uxapi import Queue
from uxapi import Awaitables
from uxapi.exchanges.ccxt import huobidm
from uxapi.helpers import (
    keysort,
    hmac,
    extend,
    is_sorted
)


@register_exchange('huobi')
class Huobi:
    def __init__(self, market_type, config):
        if market_type == 'spot':
            cls = Huobipro
        else:
            cls = Huobidm
        cls.id = type(self).id
        self._exchange = cls(market_type, config)

    def __getattr__(self, attr):
        return getattr(self._exchange, attr)


class Huobipro(UXPatch, huobipro):
    def describe(self):
        return self.deep_extend(super().describe(), {
            'has': {
                'fetchCurrencies': True,
                'fetchOrders': True,
                'fetchOpenOrders': True,
                'cancelOrders': True,
                'cancelAllOrders': True,
            },

            'urls': {
                'wsapi': {
                    'market': 'wss://api.huobi.pro/ws',
                    'private': 'wss://api.huobi.pro/ws/v2',
                    'private_aws': 'wss://api-aws.huobi.pro/ws/v2',
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
                    'myorder': 'orders#{symbol}',
                    'accounts': 'accounts.update#{mode}',
                    'clearing': 'trade.clearing#{symbol}'
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
            mode = subtypes[0] if subtypes else '1'
            return template.format(mode=mode)

        params = {}
        uxsymbol = UXSymbol(uxtopic.exchange_id, uxtopic.market_type,
                            uxtopic.extrainfo)
        if uxsymbol.name == '*':
            params['symbol'] = '*'
        else:
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

    def wshandler(self, topic_set):
        wsapi_types = {self.wsapi_type(topic) for topic in topic_set}
        if len(wsapi_types) > 1:
            raise ValueError('invalid topics')
        wsapi_type = wsapi_types.pop()
        wsurl = self.urls['wsapi'][wsapi_type]
        return HuobiWSHandler(self, wsurl, topic_set, wsapi_type)

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
        self.snapshot = {
            'ch': snapshot['rep'],
            'tick': snapshot['data'],
        }
        self.prices = {
            'asks': [item[0] for item in snapshot['data']['asks']],
            'bids': [-item[0] for item in snapshot['data']['bids']]
        }
        for patch in self.cache:
            self.merge(patch)
        self.cache = None

    def merge(self, patch):
        snapshot_tick = self.snapshot['tick']
        patch_tick = patch['tick']
        if snapshot_tick['seqNum'] != patch_tick['prevSeqNum']:
            raise RuntimeError('seqNum error')
        snapshot_tick['seqNum'] = patch_tick['seqNum']
        snapshot_tick['ts'] = patch['ts']
        self.merge_asks_bids(snapshot_tick['asks'], patch_tick['asks'],
                             self.prices['asks'], False)
        self.merge_asks_bids(snapshot_tick['bids'], patch_tick['bids'],
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
    def __init__(self, market_type, config=None):
        if market_type == 'index':
            market_type = 'futures'
        return super().__init__(market_type, extend({
            'options': {
                'fetchMarkets': market_type,
                'defaultType': market_type,
            }
        }, config or {}))

    def describe(self):
        return self.deep_extend(super().describe(), {
            'deliveryHourUTC': 8,

            'has': {
                'fetchOrders': True,
                'fetchOpenOrders': True,
                'cancelAllOrders': True,
            },

            'urls': {
                'wsapi': {
                    'market': {
                        'futures': 'wss://api.hbdm.com/ws',
                        'swap': 'wss://api.hbdm.com/swap-ws'
                    },
                    'private': {
                        'futures': 'wss://api.hbdm.com/notification',
                        'swap': 'wss://api.hbdm.com/swap-notification'
                    },
                    'index': 'wss://api.hbdm.com/ws_index',
                },
            },

            'wsapi': {
                'market': {
                    'ticker': 'market.{symbol}.detail',
                    'orderbook': 'market.{symbol}.depth.{level}',
                    'high_freq': 'market.{symbol}.depth.size_{level}.high_freq?data_type={data_type}',  # noqa: E501
                    'ohlcv': 'market.{symbol}.kline.{period}',
                    'trade': 'market.{symbol}.trade.detail',
                },
                'private': {
                    'myorder': 'orders.{currency}',
                    'position': 'positions.{currency}',
                    'accounts': 'accounts.{currency}',
                    'liquidationOrders': 'liquidationOrders.{currency}',
                    'funding_rate': 'funding_rate.{currency}',
                },
                'index': {
                    'ohlcv': 'market.{symbol}.index.{period}',
                    'basis': 'market.{symbol}.basis.{period}.{basis_price_type}'
                }
            },
        })

    def order_book_merger(self):
        return HuobidmOrderBookMerger()

    def _fetch_markets(self, params=None):
        markets = super()._fetch_markets(params)
        for market in markets:
            contract_value = self.safe_float(market['info'], 'contract_size')
            market['contractValue'] = contract_value
            if market['type'] == 'futures':
                delivery_date = self.safe_string(market['info'], 'delivery_date')
                if delivery_date:
                    delivery_time = pendulum.from_format(delivery_date, 'YYYYMMDD')
                    delivery_time = delivery_time.add(hours=self.deliveryHourUTC)
                    market['deliveryTime'] = delivery_time.to_iso8601_string()
                else:
                    market['deliveryTime'] = None
        return markets

    def convert_symbol(self, uxsymbol):
        if uxsymbol.market_type == 'futures':
            return f'{uxsymbol.base}_{uxsymbol.contract_expiration}'
        else:
            return f'{uxsymbol.base}-{uxsymbol.quote}'

    def convert_topic(self, uxtopic):
        wsapi_type = self.wsapi_type(uxtopic)
        maintype = uxtopic.maintype
        subtypes = uxtopic.subtypes
        params = {}
        if wsapi_type == 'index':
            params['symbol'] = uxtopic.extrainfo
        elif wsapi_type == 'market':
            uxsymbol = UXSymbol(uxtopic.exchange_id, uxtopic.market_type,
                                uxtopic.extrainfo)
            params['symbol'] = self.market_id(uxsymbol)
        else:  # 'private'
            if uxtopic.market_type == 'futures':
                params['currency'] = uxtopic.extrainfo.lower()
            else:
                params['currency'] = uxtopic.extrainfo + '-USD'

        if maintype == 'orderbook':
            if not subtypes:
                params['level'] = 'step0'
            elif subtypes[0] == 'full':
                maintype = 'high_freq'
                params['level'] = '150'
                params['data_type'] = 'incremental'
            else:
                params['level'] = subtypes[0]
        elif maintype == 'high_freq':
            assert subtypes and len(subtypes) == 2
            params['level'] = subtypes[0]
            params['data_type'] = subtypes[1]
        elif maintype == 'ohlcv':
            params['period'] = self.timeframes[subtypes[0]]
        elif maintype == 'basis':
            params['period'] = self.timeframes[subtypes[0]]
            params['basis_price_type'] = subtypes[1]
        template = self.wsapi[wsapi_type][maintype]
        return template.format(**params)

    def wshandler(self, topic_set):
        wsapi_types = {self.wsapi_type(topic) for topic in topic_set}
        if len(wsapi_types) > 1:
            raise ValueError('invalid topics')
        wsapi_type = wsapi_types.pop()
        if wsapi_type == 'index':
            wsurl = self.urls['wsapi'][wsapi_type]
        else:
            wsurl = self.urls['wsapi'][wsapi_type][self.market_type]
        return HuobiWSHandler(self, wsurl, topic_set, wsapi_type)

    def wsapi_type(self, uxtopic):
        if uxtopic.market_type == 'index':
            return 'index'
        for type in ('market', 'private'):
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
        self.market_type = exchange.market_type

    def on_connected(self):
        if self.market_type != 'spot' and self.wsapi_type == 'private':
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
            msg = await self.keepalive_msq.get()
            try:
                while True:
                    msg = self.keepalive_msq.get_nowait()
            except asyncio.QueueEmpty:
                pass

            if 'ping' in msg:
                # {"ping": 18212558000}
                pong = {'pong': msg['ping']}
            elif msg.get('op') == 'ping':
                # {"op": "ping", "ts": 1492420473058}
                pong = {'op': 'pong', 'ts': msg['ts']}
            else:
                # {"action": "ping", "data": {"ts": 1575537778295}}
                pong = {
                    'action': 'pong',
                    'data': {'ts': msg['data']['ts']}
                }
            await self.send(pong)

    def on_keepalive_message(self, msg):
        if ('ping' in msg or msg.get('op') == 'ping'
                or msg.get('action') == 'ping'):
            self.keepalive_msq.put_nowait(msg)
            raise StopIteration
        else:
            return msg

    @property
    def login_required(self):
        return 'private' in self.wsapi_type

    def on_login_message(self, msg):
        login_msg = False
        login_ok = False

        if msg.get('op') == 'auth':
            login_msg = True
            login_ok = (msg['err-code'] == 0)
        elif msg.get('action') == 'req' and msg.get('ch') == 'auth':
            login_msg = True
            login_ok = (msg['code'] == 200)

        if login_msg:
            if login_ok:
                self.logger.info(f'logged in')
                self.on_logged_in()
                raise StopIteration
            else:
                raise RuntimeError(f'login failed: {msg}')
        return msg

    def login_command(self, credentials):
        signature_method = 'HmacSHA256'
        apikey = credentials['apiKey']
        now = datetime.datetime.utcnow()
        timestamp = now.isoformat(timespec='seconds')

        if self.market_type == 'spot':
            params = keysort({
                'signatureMethod': signature_method,
                'signatureVersion': '2.1',
                'accessKey': apikey,
                'timestamp': timestamp
            })
        else:
            params = keysort({
                'SignatureMethod': signature_method,
                'SignatureVersion': '2',
                'AccessKeyId': apikey,
                'Timestamp': timestamp
            })

        auth = urllib.parse.urlencode(params)
        url = yarl.URL(self.wsurl)
        payload = '\n'.join(['GET', url.host, url.path, auth])
        signature_bytes = hmac(
            bytes(credentials['secret'], 'utf8'),
            bytes(payload, 'utf8'),
        )
        signature = signature_bytes.decode()

        if self.market_type == 'spot':
            params.update({
                'authType': 'api',
                'signature': signature
            })
            request = {
                'action': 'req',
                'ch': 'auth',
                'params': params
            }
        else:
            request = extend({
                'op': 'auth',
                'Signature': signature,
                'type': 'api'
            }, params)
        return request

    def create_subscribe_task(self):
        topics = {}
        for topic in self.topic_set:
            converted = self.convert_topic(topic)
            ch, params = self._split_params(converted)
            topics[ch] = params
        self.pre_processors.append(self.on_subscribe_message)
        self.pending_topics = set(topics)
        return self.awaitables.create_task(
            self.subscribe(topics), 'subscribe')

    def on_subscribe_message(self, msg):
        sub_msg = False
        sub_ok = False
        topic = None

        if 'subbed' in msg:  # huobipro & huobidm market
            sub_msg = True
            sub_ok = (msg['status'] == 'ok')
            topic = msg['subbed']
        elif msg.get('op') == 'sub':  # huobidm private
            sub_msg = True
            sub_ok = (msg['err-code'] == 0)
            topic = msg['topic']
        elif msg.get('action') == 'sub':  # huobipro private
            sub_msg = True
            sub_ok = (msg['code'] == 200)
            topic = msg['ch']

        if sub_msg:
            if sub_ok:
                self.logger.info(f'{topic} subscribed')
                self.on_subscribed(topic)
                raise StopIteration
            else:
                raise RuntimeError(f'subscribe failed: {msg}')
        return msg

    def subscribe_commands(self, topics):
        commands = []
        for ch, params in topics.items():
            if self.wsapi_type == 'private':
                if self.market_type == 'spot':
                    request = {'action': 'sub', 'ch': ch}
                else:
                    request = {'op': 'sub', 'topic': ch}
            else:
                request = {'sub': ch}
            request.update(params)
            commands.append(request)
        return commands

    @staticmethod
    def _split_params(topic):
        ch, *params_string = topic.split('?', maxsplit=1)
        params = parse_qs(params_string[0]) if params_string else {}
        params = {k: lst[0] for k, lst in params.items()}
        return ch, params

    def decode(self, data):
        # huobipro private return str not bytes
        if isinstance(data, bytes):
            msg = gzip.decompress(data).decode()
        else:
            msg = data
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
