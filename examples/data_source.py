import os
import time
import asyncio
import argparse
import reprlib
import itertools
from operator import itemgetter
from dataclasses import dataclass
from datetime import datetime

import orjson as json
import yaml
import dotenv
import redis
from utils import timeit
from uxapi import UXTopic
from uxapi import new_exchange


class DSTopic(UXTopic):
    def __init__(self, exchange_id, market_type, datatype, extrainfo='', uid=''):
        UXTopic.__init__(self, exchange_id, market_type, datatype, extrainfo)
        self.uid = uid


config = r"""
okex.futures.orderbook_full:
    exchange_id: okex
    market_type: futures
    datatype: orderbook.full
    extrainfo: BTC/USD.CQ
    uid: merge

okex.futures.orderbook_delta:
    exchange_id: okex
    market_type: futures
    datatype: orderbook.full
    extrainfo: BTC/USD.CQ

huobi.futures.orderbook_delta:
    exchange_id: huobi
    market_type: futures
    datatype: high_freq.150.incremental
    extrainfo: BTC/USD.CQ
    
okex.futures.public_trades:
    exchange_id: okex
    market_type: futures
    datatype: trade
    extrainfo: BTC/USD.CQ
    
huobi.futures.public_trades:
    exchange_id: huobi
    market_type: futures
    datatype: trade
    extrainfo: BTC/USD.CQ
"""


class WebsocketPipeline:
    def __init__(self, topic, redis, maxlen, processors=None):
        self.topic = topic
        self.redis = redis
        self.maxlen = maxlen
        self.processors = processors or []

    def __call__(self, v):
        for processor in self.processors:
            try:
                v = processor(self, v)
            except StopIteration:
                break


@dataclass
class OrderBookDelta:
    event: str
    ts: int
    asks: list
    bids: list
    exchange: str
    market_type: str
    symbol: str
    key: str
    extra: dict
    info: dict


def okex_orderbook_delta(context: WebsocketPipeline, msg) -> OrderBookDelta:
    data = msg['data'][0]
    utc_time = datetime.strptime(data['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
    epoch_time = int((utc_time - datetime.utcfromtimestamp(0)).total_seconds() * 1000)
    return OrderBookDelta(
        event=msg['action'],
        ts=epoch_time,
        asks=data['asks'],
        bids=data['bids'],
        exchange=context.topic.exchange_id,
        market_type=context.topic.market_type,
        symbol=context.topic.extrainfo,
        key=f"{context.topic.exchange_id}:{context.topic.market_type}:{context.topic.extrainfo}:orderbook_delta",
        extra={'checksum': data['checksum'], 'dt': utc_time},
        info=msg
    )


def huobi_orderbook_delta(context: WebsocketPipeline, msg) -> OrderBookDelta:
    tick = msg['tick']
    return OrderBookDelta(
        event=tick['event'],
        ts=tick['ts'],
        asks=tick['asks'],
        bids=tick['bids'],
        exchange=context.topic.exchange_id,
        market_type=context.topic.market_type,
        symbol=context.topic.extrainfo,
        key=f"{context.topic.exchange_id}:{context.topic.market_type}:{context.topic.extrainfo}:orderbook_delta",
        extra={'version': tick['version'], 'id': tick['id'], 'mrid': tick['mrid']},
        info=msg
    )


def orderbook_delta_redis_stream(context: WebsocketPipeline, delta: OrderBookDelta) -> OrderBookDelta:
    context.redis.xadd(delta.key,{
        'ts': delta.ts,
        'event': delta.event,
        'asks': json.dumps(delta.asks),
        'bids': json.dumps(delta.bids),
        'extra': json.dumps(delta.extra)
    },maxlen=context.maxlen)
    return delta


@dataclass
class PublicTrade:
    price: float
    size: float
    is_buy: int
    ts: int
    id: str
    exchange: str
    market_type: str
    symbol: str
    key: str
    extra: dict or None
    info: dict


def okex_public_trades(context: WebsocketPipeline, trades) -> [PublicTrade]:
    """
    {'table': 'futures/trade', 'data': [{'side': 'buy', 'trade_id': '47201570', 'price': '9703.26', 'qty': '9', 'instrument_id': 'BTC-USD-200626', 'timestamp': '2020-06-04T05:19:07.537Z'}]}

    :param context:
    :param trades:
    :return:
    """
    return [
        PublicTrade(
            price=trade['price'],
            size=trade['qty'],
            is_buy= 1 if (trade['side'] == 'buy') else 0,
            id=trade['trade_id'],
            ts=int((datetime.strptime(trade['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ") - datetime.utcfromtimestamp(0)).total_seconds() * 1000),
            exchange=context.topic.exchange_id,
            market_type=context.topic.market_type,
            symbol=context.topic.extrainfo,
            key=f"{context.topic.exchange_id}:{context.topic.market_type}:{context.topic.extrainfo}:public_trades",
            extra={'instrument_id': trade['instrument_id'], 'dt': datetime.strptime(trade['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")},
            info=trade
        )
    for trade in trades['data']]


def huobi_public_trades(context: WebsocketPipeline, trades) -> [PublicTrade]:
    return [
        PublicTrade(
            price=trade['price'],
            size=trade['amount'],
            is_buy= 1 if (trade['direction'] == 'buy') else 0,
            id=trade['id'],
            ts=trade['ts'],
            exchange=context.topic.exchange_id,
            market_type=context.topic.market_type,
            symbol=context.topic.extrainfo,
            key=f"{context.topic.exchange_id}:{context.topic.market_type}:{context.topic.extrainfo}:public_trades",
            extra=None,
            info=trade
        )
    for trade in trades['tick']['data']]


def public_trades_redis_stream(context: WebsocketPipeline, trades: [PublicTrade]) -> [PublicTrade]:
    for trade in trades:
        context.redis.xadd(trade.key,{
            'price': trade.price,
            'size': trade.size,
            'is_buy': trade.is_buy,
            'ts': trade.ts,
            'id': trade.id
        }, maxlen=context.maxlen)
    print(trades)
    return trades


@dataclass
class OrderBookSnapShot:
    key:str
    asks: list
    bids: list
    ts: int
    extra: {} or None


class FullOrderBook:
    def __init__(self, merger):
        self.merger = merger

    def __call__(self, context, msg):
        msg = self.merger(msg)
        return msg

def convert_response(okex_responses):
    if type(okex_responses[0][0]) is float:
        return okex_responses
    else:
        return [[float(r[0]), float(r[1])] for r in okex_responses]
def okex_orderbook_snapshot(context, msg):
    fullbook = msg['data'][0]
    utc_time = datetime.strptime(fullbook['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
    epoch_time = int((utc_time - datetime.utcfromtimestamp(0)).total_seconds() * 1000)
    return OrderBookSnapShot(
        key=f"{context.topic.exchange_id}:{context.topic.market_type}:{context.topic.extrainfo}:orderbook_snapshot",
        asks=convert_response(fullbook['asks']),
        bids=convert_response(fullbook['bids']),
        ts=epoch_time,
        extra={'dt': utc_time}
    )


def is_price_ascending(orderbook_side, reverse=False):
    return is_sorted(orderbook_side, key=itemgetter(0), reverse=reverse)
def is_sorted(iterable, *, key=None, reverse=False):
    it1, it2 = itertools.tee(iterable)
    next(it2, None)
    if reverse:
        it1, it2 = it2, it1
    for a, b in zip(it1, it2):
        if key:
            a, b = key(a), key(b)
        if a > b:
            return False
    return True
class SliceOrderbook:
    """截取 ask, bid 最顶端的 n 个 order"""

    def __init__(self, n):
        self.n = n

    def __call__(self, context: WebsocketPipeline, parsed_orderbook:OrderBookSnapShot):
        if is_price_ascending(parsed_orderbook.asks):
            parsed_orderbook.asks = parsed_orderbook.asks[:self.n]
        else:
            parsed_orderbook.asks = parsed_orderbook.asks[-self.n:]

        if is_price_ascending(parsed_orderbook.bids):
            parsed_orderbook.bids = parsed_orderbook.bids[-self.n:]
        else:
            parsed_orderbook.bids = parsed_orderbook.bids[:self.n]
        return parsed_orderbook


def orderbook_top_redis_stream(context, orderbook):
    context.redis.xadd(orderbook.key,{
        'ts': orderbook.ts,
        'asks': json.dumps(orderbook.asks),
        'bids': json.dumps(orderbook.bids),
    },maxlen=context.maxlen)
    return orderbook


def main():
    parser = argparse.ArgumentParser(
        usage='python ws.py [-h] topic_to_run',
        description='Websocket API Test',
        epilog='Example: python ws.py okex.swap.ohlcv'
    )
    parser.add_argument('topic_to_run')
    args = parser.parse_args()

    dotenv.load_dotenv()

    topics = yaml.safe_load(config)
    topic = DSTopic(**topics[args.topic_to_run])
    exchange_id = topic.exchange_id
    market_type = topic.market_type
    exchange = new_exchange(exchange_id, market_type, {
        'apiKey': os.environ.get(f'{exchange_id}_apiKey'),
        'secret': os.environ.get(f'{exchange_id}_secret'),
        'password': os.environ.get(f'{exchange_id}_password'),
    })
    exchange.load_markets()
    redis_client =r = redis.Redis(host='localhost', port=6379, db=0)

    print(topic)
    handlers = []
    if topic.uid == 'merge':
        full_orderbook = FullOrderBook(exchange.order_book_merger())
        slice_orderbook = SliceOrderbook(5)
        handlers = [full_orderbook, okex_orderbook_snapshot, slice_orderbook, orderbook_top_redis_stream]
    else:
        handlers = [okex_public_trades, public_trades_redis_stream]
        # handlers = [okex_orderbook_delta, orderbook_delta_redis_stream]

    pipeline = WebsocketPipeline(topic, redis_client, 1000, handlers)
    wshandler = exchange.wshandler({topic})
    asyncio.run(wshandler.run(pipeline))


if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception as e:
            print(e)
            time.sleep(10)
            continue
