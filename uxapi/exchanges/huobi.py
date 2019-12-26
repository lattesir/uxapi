import datetime
import asyncio
import gzip
import json
import collections
import urllib.parse

import yarl

import pendulum
from ccxt import huobipro
from uxapi import register
from uxapi import UXSymbol
from uxapi import WSHandler
from uxapi import UXPatch
from uxapi import Queue
from uxapi.exchanges.ccxt import huobidm
from uxapi.helpers import (
    all_equal,
    keysort,
    hmac,
    extend
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

    def order_book_merge(self):
        raise NotImplementedError

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
        if maintype == 'orderbook':
            params['level'] = subtypes[0] if subtypes else 'step0'
        if maintype == 'ohlcv':
            params['period'] = self.timeframes[subtypes[0]]
        return template.format(**params)

    def wsapi_type(self, uxtopic):
        for type in self.wsapi:
            if uxtopic.maintype in self.wsapi[type]:
                return type
        raise ValueError('invalid topic')


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

        if maintype == 'orderbook':
            params['level'] = subtypes[0] if subtypes else 'step0'
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