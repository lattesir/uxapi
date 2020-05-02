import time
import asyncio
import json
import operator

import ccxt

from uxapi import register_exchange
from uxapi import UXSymbol
from uxapi import UXPatch
from uxapi import WSHandler
from uxapi.helpers import (
    hmac,
    contract_delivery_time
)


@register_exchange('bitmex')
class Bitmex(UXPatch, ccxt.bitmex):
    def describe(self):
        return self.deep_extend(super().describe(), {
            'deliveryHourUTC': 12,

            'options': {
                'ws-api-expires': 60*60*24*1000,  # 1000 days
            },

            'urls': {
                'wsapi': 'wss://www.bitmex.com/realtime',
                'wsapiTest': 'wss://testnet.bitmex.com/realtime',
            },

            'wsapi': [
                'orderbook',
                'trade',
                'quote',
                'announcement',
                'chat',
                'connected',
                'funding',
                'instrument',
                'insurance',
                'liquidation',
                'publicNotifications',
                'settlement',
                'affiliate',
                'execution',
                'myorder',
                'margin',
                'position',
                'privateNotifications',
                'transact',
                'wallet',
            ],
        })

    @classmethod
    def testnet(cls, market_type, config):
        bitmex = cls(market_type, config)
        bitmex.urls['api'] = bitmex.urls['test']
        bitmex.urls['wsapi'] = bitmex.urls['wsapiTest']
        return bitmex

    def _fetch_markets(self, params):
        markets = super()._fetch_markets(params)
        for market in markets:
            contract_value = self.safe_float(market['info'], 'lotSize')
            if contract_value:
                market['contractValue'] = contract_value
            if market['type'] == 'future':
                market['type'] == 'futures'
                market['deliveryTime'] = market['info']['settle']
        return markets

    def parse_order_book(self, orderbook):
        # [
        #     {
        #         "symbol": "XBTUSD",
        #         "id": 15599187800,
        #         "side": "Sell",
        #         "size": 542,
        #         "price": 8122
        #     },
        #     ...
        # ]
        asks = []
        bids = []
        for item in orderbook:
            price = float(item['price'])
            amount = float(item['size'])
            if item['side'] == 'Sell':
                asks.append([price, amount])
            else:
                bids.append([price, amount])
        asks = sorted(asks, key=operator.itemgetter(0))
        bids = sorted(bids, key=operator.itemgetter(0), reverse=True)
        return {
            'asks': asks,
            'bids': bids,
            'timestamp': None,
            'datetime': None,
            'nonce': None
        }

    def order_book_merger(self):
        return BitmexOrderBookMerger()

    def wshandler(self, topic_set):
        return BitmexWSHandler(self, self.urls['wsapi'], topic_set)

    def convert_symbol(self, uxsymbol):
        if uxsymbol.market_type == 'swap':
            if uxsymbol.name == '!ETHUSD/BTC':
                return 'ETH/USD'
            elif uxsymbol.name == '!XRPUSD/BTC':
                return 'XRP/USD'
            elif uxsymbol.name == 'BTC/USD':
                return uxsymbol.name

        elif uxsymbol.market_type == 'futures':
            if uxsymbol.base == 'BTC':
                delivery_time = contract_delivery_time(
                    expiration=uxsymbol.contract_expiration,
                    delivery_hour=self.deliveryHourUTC)
                code = self._contract_code(delivery_time)
                if uxsymbol.quote == 'USD':
                    return f'XBT{code}'
                elif uxsymbol.quote in ('ADA', 'BCH', 'EOS', 'ETH', 'LTC', 'TRX', 'XRP'):
                    return f'{uxsymbol.quote}{code}'

        elif uxsymbol.market_type == 'index':
            return uxsymbol.name

        raise ValueError('invalid symbol')

    @staticmethod
    def _contract_code(delivery_time):
        month_codes = "FGHJKMNQUVXZ"
        year = delivery_time.year - 2000
        quarter = (delivery_time.month + 2) // 3
        delivery_month = month_codes[quarter * 3 - 1]
        return f'{delivery_month}{year}'

    def convert_topic(self, uxtopic):
        maintype = uxtopic.maintype
        assert maintype in self.wsapi
        subtypes = uxtopic.subtypes
        topic = None
        if maintype == 'orderbook':
            if not subtypes:
                level = '10'
            elif subtypes[0] == 'full':
                level = 'L2'
            elif subtypes[0] == '25':
                level = 'L2_25'
            else:
                raise ValueError('invalid topic')
            topic = f'orderBook{level}'
        elif maintype == 'quote':
            if subtypes:
                topic = f'quoteBin{subtypes[0]}'
            else:
                topic = 'quote'
        elif maintype == 'trade':
            if subtypes:
                topic = f'tradeBin{subtypes[0]}'
            else:
                topic = 'trade'
        elif maintype == 'myorder':
            topic = 'order'
        else:
            topic = maintype

        if uxtopic.extrainfo:
            uxsymbol = UXSymbol(uxtopic.exchange_id, uxtopic.market_type,
                                uxtopic.extrainfo)
            symbol = self.market_id(uxsymbol)
            topic = f'{topic}:{symbol}'

        return topic


class BitmexWSHandler(WSHandler):
    default_api_expires = 60 * 60 * 24 * 1000  # 1000 days

    def on_connected(self):
        self.pre_processors.append(self.on_info_message)
        self.pre_processors.append(self.on_error_message)

    def on_info_message(self, msg):
        if 'info' in msg:
            self.logger.info(msg)
            raise StopIteration
        else:
            return msg

    def on_error_message(self, msg):
        if 'error' in msg:
            raise RuntimeError(msg)
        else:
            return msg

    def create_keepalive_task(self):
        self.last_message_timestamp = time.time()
        return super().create_keepalive_task()

    async def keepalive(self):
        interval = 5
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
        private_types = {
            'myorder', 'margin', 'position', 'affiliate', 'execution',
            'privateNotifications', 'transact', 'wallet',
        }
        topic_types = {topic.maintype for topic in self.topic_set}
        for type in private_types:
            if type in topic_types:
                return True
        return False

    def login_command(self, credentials):
        if self.exchange:
            expires = self.exchange.options['ws-api-expires']
        else:
            expires = self.default_api_expires
        expires += int(time.time())

        payload = 'GET' + '/realtime' + str(expires)
        signature = hmac(
            bytes(credentials['secret'], 'utf8'),
            bytes(payload, 'utf8'),
            digest='hex'
        )
        return {
            'op': 'authKeyExpires',
            'args': [
                credentials['apiKey'],
                expires,
                signature,
            ]
        }

    def on_login_message(self, msg):
        if ('request' in msg) and (msg['request'].get('op') == 'authKeyExpires'):
            if msg['success']:
                self.logger.info('logged in')
                self.on_logged_in()
                raise StopIteration
            else:
                raise RuntimeError('login failed')
        else:
            return msg

    def subscribe_commands(self, topic_set):
        command = {
            'op': 'subscribe',
            'args': list(topic_set),
        }
        return [command]

    def on_subscribe_message(self, msg):
        if 'subscribe' in msg:
            topic = msg['subscribe']
            self.logger.info(f'{topic} subscribed')
            self.on_subscribed(topic)
            raise StopIteration
        else:
            return msg

    def decode(self, data):
        try:
            jsonmsg = json.loads(data)
            return jsonmsg
        except json.JSONDecodeError:
            return data


class BitmexOrderBookMerger:
    def __init__(self):
        self.snapshot = None
        self.data = None

    def __call__(self, patch):
        if patch['action'] == 'partial':
            self.on_snapshot(patch)
        elif patch['action'] in ('update', 'delete', 'insert'):
            self.merge(patch)
        else:
            raise ValueError('unexpected action')
        return self.snapshot

    def on_snapshot(self, snapshot):
        self.snapshot = snapshot
        self.data = {item['id']: item for item in snapshot['data']}
        self.update_snapshot()

    def merge(self, patch):
        if not self.snapshot:
            raise StopIteration

        if patch['action'] == 'update':
            for item in patch['data']:
                self.data[item['id']]['size'] = item['size']

        elif patch['action'] == 'delete':
            for item in patch['data']:
                del self.data[item['id']]
            self.update_snapshot()

        elif patch['action'] == 'insert':
            self.data.update((item['id'], item) for item in patch['data'])
            self.update_snapshot()

    def update_snapshot(self):
        def sortkey(item):
            if item['side'] == 'Sell':
                side = 0
                price = item['price']
            else:
                side = 1
                price = -item['price']
            return side, price

        data = sorted(self.data.values(), key=sortkey)
        self.snapshot['data'] = data
