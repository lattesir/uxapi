import os
import asyncio
import reprlib
import argparse
import calendar
import dateutil
import json
from datetime import datetime

import yaml
import dotenv
from redistimeseries.client import Client as rtsClient

from uxapi import UXTopic
from uxapi import new_exchange
from uxapi import Pipeline


config = r"""
okex.futures.orderbook.full:
    exchange_id: okex
    market_type: futures
    datatype: orderbook.full
    extrainfo: BTC/USD.CQ

huobi.futures.high_freq:
    exchange_id: huobi
    market_type: futures
    datatype: high_freq.150.incremental
    extrainfo: BTC/USD.CQ
"""


class OkexParseOrderBookDelta:
    def __init__(self, topic: UXTopic):
        self.topic = topic

    def __call__(self, msg):
        data = msg['data'][0]
        utc_time = datetime.strptime(data['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
        epoch_time = int((utc_time - datetime.utcfromtimestamp(0)).total_seconds() * 1000)
        return {
            'event': msg['action'],
            'ts': epoch_time,
            'asks': data['asks'],
            'bids': data['bids'],
            'exchange': self.topic.exchange_id,
            'market_type': self.topic.market_type,
            'symbol': self.topic.extrainfo,
            'key': f"{self.topic.exchange_id}:{self.topic.market_type}:{self.topic.extrainfo}:orderbook_delta",
            'info': msg,
        }


class HuobiParseOrderBookDelta:
    def __init__(self, topic: UXTopic):
        self.topic = topic

    def __call__(self, msg):
        tick = msg['tick']
        return  {
            'event': tick['event'],
            'ts': tick['ts'],
            'asks': tick['asks'],
            'bids': tick['bids'],
            'exchange': self.topic.exchange_id,
            'market_type': self.topic.market_type,
            'symbol': self.topic.extrainfo,
            'key': f"{self.topic.exchange_id}:{self.topic.market_type}:{self.topic.extrainfo}:orderbook_delta",
            'info': msg,
        }


class OkexOrderBookDeltaToRedisStream:
    def __init__(self, redis_client, maxlen):
        self.r = redis_client
        self.maxlen = maxlen

    def __call__(self, delta):
        self.r.xadd(delta['key'], {'event': delta['event'], 'ts': delta['ts'], 'asks': json.dumps(delta['asks']), 'bids': json.dumps(delta['bids'])}, maxlen=self.maxlen)


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
    topic = UXTopic(**topics[args.topic_to_run])
    exchange_id = topic.exchange_id
    market_type = topic.market_type
    exchange = new_exchange(exchange_id, market_type, {
        'apiKey': os.environ.get(f'{exchange_id}_apiKey'),
        'secret': os.environ.get(f'{exchange_id}_secret'),
        'password': os.environ.get(f'{exchange_id}_password'),
    })
    exchange.load_markets()
    pipeline = Pipeline([OkexParseOrderBookDelta(topic), OkexOrderBookDeltaToRedisStream(rtsClient(), 1000)])
    wshandler = exchange.wshandler({topic})
    asyncio.run(wshandler.run(pipeline))


if __name__ == '__main__':
    main()
