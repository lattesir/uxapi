import os
import asyncio
import reprlib
import argparse

import yaml
import dotenv

from uxapi import UXTopic
from uxapi import new_exchange


config = r"""

################
# Binance Spot
################

binance.spot.orderbook:
    exchange_id: binance
    market_type: spot
    datatype: orderbook
    extrainfo: BTC/USDT

binance.spot.orderbook.full:
    exchange_id: binance
    market_type: spot
    datatype: orderbook.full
    extrainfo: BTC/USDT

binance.spot.ohlcv:
    exchange_id: binance
    market_type: spot
    datatype: ohlcv.1m
    extrainfo: BTC/USDT

binance.spot.trade:
    exchange_id: binance
    market_type: spot
    datatype: trade
    extrainfo: BTC/USDT

binance.spot.aggTrade:
    exchange_id: binance
    market_type: spot
    datatype: aggTrade
    extrainfo: BTC/USDT

binance.spot.ticker:
    exchange_id: binance
    market_type: spot
    datatype: ticker
    extrainfo: BTC/USDT

binance.spot.private:
    exchange_id: binance
    market_type: spot
    datatype: private
    extrainfo: BTC/USDT

##################
# Binance Futures
##################

binance.futures.orderbook:
    exchange_id: binance
    market_type: futures
    datatype: orderbook.5@100ms
    extrainfo: BTC/USD.NQ

binance.futures.orderbook.full:
    exchange_id: binance
    market_type: futures
    datatype: orderbook.full
    extrainfo: BTC/USD.CQ

binance.futures.continuousKline:
    exchange_id: binance
    market_type: futures
    datatype: continuousKline
    extrainfo: btcusd_current_quarter

binance.futures.indexPriceKline:
    exchange_id: binance
    market_type: futures
    datatype: indexPriceKline.1h
    extrainfo: btcusd

binance.futures.markPrice:
    exchange_id: binance
    market_type: futures
    datatype: markPrice.1s
    extrainfo: BTC/USD.CQ

binance.futures.private:
    exchange_id: binance
    market_type: futures
    datatype: private
    extrainfo: ''

#################
# Binance Swap
#################

binance.swap.orderbook:
    exchange_id: binance
    market_type: swap
    datatype: orderbook
    extrainfo: USDT/BTC

binance.swap.orderbook.full:
    exchange_id: binance
    market_type: swap
    datatype: orderbook.full
    extrainfo: USDT/BTC

binance.swap.orderbook.250ms:
    exchange_id: binance
    market_type: swap
    datatype: orderbook.@250ms
    extrainfo: USDT/BTC

binance.swap.ohlcv:
    exchange_id: binance
    market_type: swap
    datatype: ohlcv.1m
    extrainfo: USDT/BTC

binance.swap.markPrice:
    exchange_id: binance
    market_type: swap
    datatype: markPrice.1s
    extrainfo: USDT/BTC

binance.swap.private:
    exchange_id: binance
    market_type: swap
    datatype: private
    extrainfo: ''

#################
# Bitmex Futures
#################

bitmex.futures.orderbook:
    exchange_id: bitmex
    market_type: futures
    datatype: orderbook
    extrainfo: BTC/USD.CQ

bitmex.futures.orderbook.full:
    exchange_id: bitmex
    market_type: futures
    datatype: orderbook.full
    extrainfo: BTC/USD.CQ

bitmex.futures.quote:
    exchange_id: bitmex
    market_type: futures
    datatype: quote.1m
    extrainfo: BTC/USD.CQ

bitmex.futures.trade:
    exchange_id: bitmex
    market_type: futures
    datatype: trade
    extrainfo: BTC/USD.CQ

################
# Bitmex Swap
################

bitmex.swap.orderbook:
    exchange_id: bitmex
    market_type: swap
    datatype: orderbook
    extrainfo: BTC/USD

bitmex.swap.orderbook.ethusd:
    exchange_id: bitmex
    market_type: swap
    datatype: orderbook
    extrainfo: '!ETHUSD/BTC'

bitmex.swap.quote:
    exchange_id: bitmex
    market_type: swap
    datatype: quote.1m
    extrainfo: BTC/USD

bitmex.swap.trade:
    exchange_id: bitmex
    market_type: swap
    datatype: trade
    extrainfo: BTC/USD

bitmex.swap.myorder:
    exchange_id: bitmex
    market_type: swap
    datatype: myorder
    extrainfo: BTC/USD

################
# Bitmex Index
################

bitmex.index.quote:
    exchange_id: bitmex
    market_type: index
    datatype: quote
    extrainfo: .BXBT

bitmex.index.trade:
    exchange_id: bitmex
    market_type: index
    datatype: trade
    extrainfo: .BXBT

##############
# Huobi Spot
##############

huobi.spot.orderbook:
    exchange_id: huobi
    market_type: spot
    datatype: orderbook
    extrainfo: BTC/USDT

huobi.spot.orderbook.full:
    exchange_id: huobi
    market_type: spot
    datatype: orderbook.full
    extrainfo: BTC/USDT

huobi.spot.ohlcv:
    exchange_id: huobi
    market_type: spot
    datatype: ohlcv.1m
    extrainfo: BTC/USDT

huobi.spot.trade:
    exchange_id: huobi
    market_type: spot
    datatype: trade
    extrainfo: BTC/USDT

huobi.spot.myorder:
    exchange_id: huobi
    market_type: spot
    datatype: myorder
    extrainfo: BTC/USDT

huobi.spot.myorder.all:
    exchange_id: huobi
    market_type: spot
    datatype: myorder
    extrainfo: '*'

huobi.spot.accounts:
    exchange_id: huobi
    market_type: spot
    datatype: accounts
    extrainfo: ''

################
# Huobi Futures
################

huobi.futures.orderbook:
    exchange_id: huobi
    market_type: futures
    datatype: orderbook
    extrainfo: BTC/USD.CQ

huobi.futures.orderbook.full:
    exchange_id: huobi
    market_type: futures
    datatype: orderbook.full
    extrainfo: BTC/USD.CQ

huobi.futures.high_freq:
    exchange_id: huobi
    market_type: futures
    datatype: high_freq.150.incremental
    extrainfo: BTC/USD.CQ

huobi.futures.ohlcv:
    exchange_id: huobi
    market_type: futures
    datatype: ohlcv.1m
    extrainfo: BTC/USD.CQ

huobi.futures.trade:
    exchange_id: huobi
    market_type: futures
    datatype: trade
    extrainfo: BTC/USD.CQ

huobi.futures.myorder:
    exchange_id: huobi
    market_type: futures
    datatype: myorder
    extrainfo: BTC

huobi.futures.accounts:
    exchange_id: huobi
    market_type: futures
    datatype: accounts
    extrainfo: BTC

################
# Huobi Swap
################

huobi.swap.orderbook:
    exchange_id: huobi
    market_type: swap
    datatype: orderbook
    extrainfo: BTC/USD

huobi.swap.orderbook.full:
    exchange_id: huobi
    market_type: swap
    datatype: orderbook.full
    extrainfo: BTC/USD

huobi.swap.myorder:
    exchange_id: huobi
    market_type: swap
    datatype: myorder
    extrainfo: BTC

huobi.swap.funding_rate:
    exchange_id: huobi
    market_type: swap
    datatype: funding_rate
    extrainfo: BTC


################
# Huobi Index
################

huobi.index.ohlcv:
    exchange_id: huobi
    market_type: index
    datatype: ohlcv.1m
    extrainfo: BTC-USD

huobi.index.basis:
    exchange_id: huobi
    market_type: index
    datatype: basis.1m.open
    extrainfo: BTC_CW

#############
# Okex Spot
#############

okex.spot.orderbook:
    exchange_id: okex
    market_type: spot
    datatype: orderbook
    extrainfo: BTC/USDT

okex.spot.orderbook.full:
    exchange_id: okex
    market_type: spot
    datatype: orderbook.full
    extrainfo: BTC/USDT

okex.spot.ohlcv:
    exchange_id: okex
    market_type: spot
    datatype: ohlcv.1m
    extrainfo: BTC/USDT

okex.spot.ticker:
    exchange_id: okex
    market_type: spot
    datatype: ticker
    extrainfo: BTC/USDT

okex.spot.myorder:
    exchange_id: okex
    market_type: spot
    datatype: myorder
    extrainfo: BTC/USDT

okex.spot.account:
    exchange_id: okex
    market_type: spot
    datatype: account
    extrainfo: BTC

###############
# Okex Futures
###############

okex.futures.orderbook:
    exchange_id: okex
    market_type: futures
    datatype: orderbook
    extrainfo: BTC/USD.CQ

okex.futures.orderbook.full:
    exchange_id: okex
    market_type: futures
    datatype: orderbook.full
    extrainfo: BTC/USD.CQ

okex.futures.ohlcv:
    exchange_id: okex
    market_type: futures
    datatype: ohlcv.1m
    extrainfo: BTC/USD.CQ

okex.futures.ticker:
    exchange_id: okex
    market_type: futures
    datatype: ticker
    extrainfo: BTC/USD.CQ

okex.futures.account:
    exchange_id: okex
    market_type: futures
    datatype: account
    extrainfo: BTC

okex.futures.position:
    exchange_id: okex
    market_type: futures
    datatype: position
    extrainfo: BTC/USD.CQ

okex.futures.myorder:
    exchange_id: okex
    market_type: futures
    datatype: myorder
    extrainfo: BTC/USD.CQ

#############
# Okex Swap
#############

okex.swap.orderbook:
    exchange_id: okex
    market_type: swap
    datatype: orderbook
    extrainfo: BTC/USD

okex.swap.ohlcv:
    exchange_id: okex
    market_type: swap
    datatype: ohlcv.1m
    extrainfo: BTC/USD

okex.swap.ticker:
    exchange_id: okex
    market_type: swap
    datatype: ticker
    extrainfo: BTC/USD

okex.swap.trade:
    exchange_id: okex
    market_type: swap
    datatype: trade
    extrainfo: BTC/USD

okex.swap.account:
    exchange_id: okex
    market_type: swap
    datatype: account
    extrainfo: BTC/USD

okex.swap.myorder:
    exchange_id: okex
    market_type: swap
    datatype: myorder
    extrainfo: BTC/USD

#############
# Okex Option
#############

okex.option.orderbook:
    exchange_id: okex
    market_type: option
    datatype: orderbook.400
    extrainfo: BTC/USD.2020-09-25.3000.C

okex.option.ohlcv:
    exchange_id: okex
    market_type: option
    datatype: ohlcv.1m
    extrainfo: EOS/USD.20200925.1_20.P

okex.option.account:
    exchange_id: okex
    market_type: option
    datatype: account
    extrainfo: BTC-USD

okex.option.position:
    exchange_id: okex
    market_type: option
    datatype: position
    extrainfo: BTC-USD

okex.option.myorder:
    exchange_id: okex
    market_type: option
    datatype: myorder
    extrainfo: BTC-USD
"""


class FullOrderBook:
    def __init__(self, merger):
        self.merger = merger

    def __call__(self, msg):
        try:
            msg = self.merger(msg)
            print(reprlib.repr(msg))
        except StopIteration:
            pass


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
        'apiKey': os.getenv(f'{exchange_id}_apiKey'),
        'secret': os.getenv(f'{exchange_id}_secret'),
        'password': os.getenv(f'{exchange_id}_password'),
    })
    exchange.load_markets()
    wshandler = exchange.wshandler({topic})
    if topic.datatype == 'orderbook.full':
        full_order_book = FullOrderBook(exchange.order_book_merger())
        asyncio.run(wshandler.run(full_order_book))
    else:
        asyncio.run(wshandler.run(print))


if __name__ == '__main__':
    main()
