from uxapi.__version__ import VERSION, __version__
from uxapi.symbol import UXSymbol
from uxapi.topic import UXTopic
from uxapi.pipeline import Pipeline
from uxapi.listiter import listiter
from uxapi.event import Event
from uxapi.queue import Queue
from uxapi.session import Session
from uxapi.awaitables import (
    Awaitables,
    run_in_executor,
    ExecutionResult,
    ExecutionError,
)
from uxapi.patch import UXPatch
from uxapi.wshandler import WSHandler


_registry = {}


def register(exchange_cls):
    _registry[exchange_cls.id] = exchange_cls
    return exchange_cls


def new_exchange(exchange_id, market_type, config=None):
    return _registry[exchange_id](market_type, config)


from uxapi.exchanges.okex import (
    Okex, OkexWSHandler, OkexOrderBookMerger)
from uxapi.exchanges.huobi import (
    Huobi, HuobiWSHandler, HuobiWSReq,
    Huobipro, HuobiproOrderBookMerger,
    Huobidm, HuobidmOrderBookMerger)
from uxapi.exchanges.bitmex import (
    Bitmex, BitmexWSHandler, BitmexOrderBookMerger)
from uxapi.exchanges.binance import (
    Binance, BinanceWSHandler, BinanceOrderBookMerger)