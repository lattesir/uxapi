from uxapi import UXSymbol
from uxapi.helpers import extend


class UXPatch:
    def __init__(self, market_type, config=None):
        super().__init__(extend({
            'id': type(self).id,
            'market_type': market_type,
        }, config or {}))
        service_providers = getattr(self, 'serviceProviders', {})
        for service, provider in service_providers.items():
            if service in self.has:
                self.has[service] = True

    def describe(self):
        return self.deep_extend(super().describe(), {
            'has': {
                'fetchMarkets': True,
                'fetchCurrencies': False,
                'fetchTicker': True,
                'fetchTickers': False,
                'fetchOrderBook': True,
                'fetchOrderBooks': False,
                'fetchL2OrderBook': True,
                'fetchOHLCV': True,
                'fetchTrades': True,
                'fetchOrder': True,
                'fetchOrders': False,
                'fetchOpenOrders': False,
                'fetchClosedOrders': False,
                'fetchMyTrades': True,
                'createOrder': True,
                'createLimitOrder': True,
                'createMarketOrder': False,
                'cancelOrder': True,
                'cancelOrders': False,
                'cancelAllOrders': False,
                'editOrder': False,
                'fetchBalance': False,
                'CORS': False,
                'createDepositAddress': False,
                'deposit': False,
                'fetchDepositAddress': False,
                'fetchDeposits': False,
                'fetchLedger': False,
                'fetchStatus': 'emulated',
                'fetchTime': False,
                'fetchTradingFee': False,
                'fetchTradingFees': False,
                'fetchFundingFee': False,
                'fetchFundingFees': False,
                'fetchTradingLimits': False,
                'fetchTransactions': False,
                'fetchWithdrawals': False,
                'privateAPI': True,
                'publicAPI': True,
                'withdraw': False,
            },
        })

    def request(self, path, api='public', method='GET',
                params=None, headers=None, body=None):
        params = params or {}
        self.lastRestRequestTimestamp = self.milliseconds()
        r = self.sign(path, api, method, params, headers, body)
        sp = self.get_service_provider('fetch')
        if sp:
            return sp(self, r['url'], r['method'], r['headers'], r['body'])
        else:
            return self.fetch(r['url'], r['method'], r['headers'], r['body'])

    def fetch_markets(self, params=None):
        params = params or {}
        sp = self.get_service_provider('fetchMarkets')
        if sp:
            return sp(self, params)
        else:
            return self._fetch_markets(params)

    def _fetch_markets(self, params):
        return super().fetch_markets(params)

    def fetch_currencies(self, params=None):
        params = params or {}
        sp = self.get_service_provider('fetchCurrencies')
        if sp:
            return sp(self, params)
        else:
            return self._fetch_currencies(params)

    def _fetch_currencies(self, params):
        return super().fetch_currencies(params)

    def fetch_ticker(self, symbol, params=None):
        params = params or {}
        uxsymbol = self.to_uxsymbol(symbol)
        sp = self.get_service_provider('fetchTicker')
        if sp:
            return sp(self, uxsymbol, params)
        else:
            return self._fetch_ticker(uxsymbol, params)

    def _fetch_ticker(self, uxsymbol, params):
        symbol = self.convert_symbol(uxsymbol)
        return super().fetch_ticker(symbol, params)

    def fetch_tickers(self, symbols=None, params=None):
        params = params or {}
        if symbols:
            uxsymbols = [self.to_uxsymbol(s) for s in symbols]
        else:
            uxsymbols = None
        sp = self.get_service_provider('fetchTickers')
        if sp:
            return sp(self, uxsymbols, params)
        else:
            return self._fetch_tickers(uxsymbols, params)

    def _fetch_tickers(self, uxsymbols, params):
        if uxsymbols:
            symbols = [self.convert_symbol(s) for s in uxsymbols]
        else:
            symbols = None
        return super().fetch_tickers(symbols, params)

    def fetch_order_book(self, symbol, limit=None, params=None):
        params = params or {}
        uxsymbol = self.to_uxsymbol(symbol)
        sp = self.get_service_provider('fetchOrderBook')
        if sp:
            return sp(self, uxsymbol, limit, params)
        else:
            return self._fetch_order_book(uxsymbol, limit, params)

    def _fetch_order_book(self, uxsymbol, limit, params):
        symbol = self.convert_symbol(uxsymbol)
        return super().fetch_order_book(symbol, limit, params)

    def fetch_l2_order_book(self, symbol, limit=None, params=None):
        params = params or {}
        uxsymbol = self.to_uxsymbol(symbol)
        sp = self.get_service_provider('fetchL2OrderBook')
        if sp:
            return sp(self, uxsymbol, limit, params)
        else:
            return self._fetch_l2_order_book(uxsymbol, limit, params)

    def _fetch_l2_order_book(self, uxsymbol, limit, params):
        symbol = self.convert_symbol(uxsymbol)
        return super().fetch_l2_order_book(symbol, limit, params)

    def fetch_order_books(self, symbols=None, params=None):
        params = params or {}
        if symbols:
            uxsymbols = [self.to_uxsymbol(s) for s in symbols]
        else:
            uxsymbols = None
        sp = self.get_service_provider('fetchOrderBooks')
        if sp:
            return sp(self, uxsymbols, params)
        else:
            return self._fetch_order_books(uxsymbols, params)

    def _fetch_order_books(self, uxsymbols, params):
        if uxsymbols:
            symbols = [self.convert_symbol(s) for s in uxsymbols]
        else:
            symbols = None
        return super().fetch_order_books(symbols, params)

    def fetch_ohlcv(self, symbol, timeframe='1m', since=None,
                    limit=None, params=None):
        params = params or {}
        uxsymbol = self.to_uxsymbol(symbol)
        sp = self.get_service_provider('fetchOHLCV')
        if sp:
            return sp(self, uxsymbol, timeframe, since, limit, params)
        else:
            return self._fetch_ohlcv(uxsymbol, timeframe, since, limit, params)

    def _fetch_ohlcv(self, uxsymbol, timeframe, since, limit, params):
        symbol = self.convert_symbol(uxsymbol)
        return super().fetch_ohlcv(symbol, timeframe, since, limit, params)

    def fetch_trades(self, symbol, since=None, limit=None, params=None):
        params = params or {}
        uxsymbol = self.to_uxsymbol(symbol)
        sp = self.get_service_provider('fetchTrades')
        if sp:
            return sp(self, uxsymbol, since, limit, params)
        else:
            return self._fetch_trades(uxsymbol, since, limit, params)

    def _fetch_trades(self, uxsymbol, since, limit, params):
        symbol = self.convert_symbol(uxsymbol)
        return super().fetch_trades(symbol, since, limit, params)

    def fetch_order(self, id, symbol=None, params=None):
        params = params or {}
        if symbol:
            uxsymbol = self.to_uxsymbol(symbol)
        else:
            uxsymbol = None
        sp = self.get_service_provider('fetchOrder')
        if sp:
            return sp(self, id, uxsymbol, params)
        else:
            return self._fetch_order(id, uxsymbol, params)

    def _fetch_order(self, id, uxsymbol, params):
        if uxsymbol:
            symbol = self.convert_symbol(uxsymbol)
        else:
            symbol = None
        return super().fetch_order(id, symbol, params)

    def fetch_orders(self, symbol=None, since=None, limit=None, params=None):
        params = params or {}
        if symbol:
            uxsymbol = self.to_uxsymbol(symbol)
        else:
            uxsymbol = None
        sp = self.get_service_provider('fetchOrders')
        if sp:
            return sp(self, uxsymbol, since, limit, params)
        else:
            return self._fetch_orders(uxsymbol, since, limit, params)

    def _fetch_orders(self, uxsymbol, since, limit, params):
        if uxsymbol:
            symbol = self.convert_symbol(uxsymbol)
        else:
            symbol = None
        return super().fetch_orders(symbol, since, limit, params)

    def fetch_open_orders(self, symbol=None, since=None, limit=None, params=None):
        params = params or {}
        if symbol:
            uxsymbol = self.to_uxsymbol(symbol)
        else:
            uxsymbol = None
        sp = self.get_service_provider('fetchOpenOrders')
        if sp:
            return sp(self, uxsymbol, since, limit, params)
        else:
            return self._fetch_open_orders(uxsymbol, since, limit, params)

    def _fetch_open_orders(self, uxsymbol, since, limit, params):
        if uxsymbol:
            symbol = self.convert_symbol(uxsymbol)
        else:
            symbol = None
        return super().fetch_open_orders(symbol, since, limit, params)

    def fetch_closed_orders(self, symbol=None, since=None, limit=None,
                            params=None):
        params = params or {}
        if symbol:
            uxsymbol = self.to_uxsymbol(symbol)
        else:
            uxsymbol = None
        sp = self.get_service_provider('fetchClosedOrders')
        if sp:
            return sp(self, uxsymbol, since, limit, params)
        else:
            return self._fetch_closed_orders(uxsymbol, since, limit, params)

    def _fetch_closed_orders(self, uxsymbol, since, limit, params):
        if uxsymbol:
            symbol = self.convert_symbol(uxsymbol)
        else:
            symbol = None
        return super().fetch_closed_orders(symbol, since, limit, params)

    def create_order(self, symbol, type, side, amount, price=None, params=None):
        params = params or {}
        uxsymbol = self.to_uxsymbol(symbol)
        sp = self.get_service_provider('createOrder')
        if sp:
            return sp(self, uxsymbol, type, side, amount, price, params)
        else:
            return self._create_order(uxsymbol, type, side, amount, price, params)

    def _create_order(self, uxsymbol, type, side, amount, price, params):
        symbol = self.convert_symbol(uxsymbol)
        return super().create_order(symbol, type, side, amount, price, params)

    def cancel_order(self, id, symbol=None, params=None):
        params = params or {}
        if symbol:
            uxsymbol = self.to_uxsymbol(symbol)
        else:
            uxsymbol = None
        sp = self.get_service_provider('cancelOrder')
        if sp:
            return sp(self, id, uxsymbol, params)
        else:
            return self._cancel_order(id, uxsymbol, params)

    def _cancel_order(self, id, uxsymbol, params):
        if uxsymbol:
            symbol = self.convert_symbol(uxsymbol)
        else:
            symbol = None
        return super().cancel_order(id, symbol, params)

    def cancel_orders(self, ids, symbol=None, params=None):
        params = params or {}
        if symbol:
            uxsymbol = self.to_uxsymbol(symbol)
        else:
            uxsymbol = None
        sp = self.get_service_provider('cancelOrders')
        if sp:
            return sp(self, ids, uxsymbol, params)
        else:
            return self._cancel_orders(ids, uxsymbol, params)

    def _cancel_orders(self, ids, uxsymbol, params):
        if uxsymbol:
            symbol = self.convert_symbol(uxsymbol)
        else:
            uxsymbol = None
        return super().cancel_orders(ids, symbol, params)

    def cancel_all_orders(self, symbol=None, params=None):
        params = params or {}
        if symbol:
            uxsymbol = self.to_uxsymbol(symbol)
        else:
            uxsymbol = None
        sp = self.get_service_provider('cancelAllOrders')
        if sp:
            return sp(self, uxsymbol, params)
        else:
            return self._cancel_all_orders(uxsymbol, params)

    def _cancel_all_orders(self, uxsymbol, params):
        if uxsymbol:
            symbol = self.convert_symbol(uxsymbol)
        else:
            symbol = None
        return super().cancel_all_orders(symbol, params)

    def edit_order(self, id, symbol=None, *args):
        if symbol:
            uxsymbol = self.to_uxsymbol(symbol)
        else:
            uxsymbol = None
        sp = self.get_service_provider('editOrder')
        if sp:
            return sp(self, id, uxsymbol, *args)
        else:
            return self._edit_order(id, uxsymbol, *args)

    def _edit_order(self, id, uxsymbol, *args):
        if uxsymbol:
            symbol = self.convert_symbol(uxsymbol)
        else:
            symbol = None
        return super().edit_order(id, symbol, *args)

    def fetch_my_trades(self, symbol=None, since=None, limit=None, params=None):
        params = params or {}
        if symbol:
            uxsymbol = self.to_uxsymbol(symbol)
        else:
            uxsymbol = None
        sp = self.get_service_provider('fetchMyTrades')
        if sp:
            return sp(self, uxsymbol, since, limit, params)
        else:
            return self._fetch_my_trades(uxsymbol, since, limit, params)

    def _fetch_my_trades(self, uxsymbol, since, limit, params):
        if uxsymbol:
            symbol = self.convert_symbol(uxsymbol)
        else:
            symbol = None
        return super().fetch_my_trades(symbol, since, limit, params)

    def to_uxsymbol(self, symbol):
        assert symbol, 'symbol is None'
        if isinstance(symbol, UXSymbol):
            return symbol
        else:
            return UXSymbol(self.id, self.market_type, symbol)

    def convert_symbol(self, uxsymbol):
        return uxsymbol.name

    def convert_topic(self, uxtopic):
        raise NotImplementedError

    def market(self, symbol):
        if not self.markets:
            raise RuntimeError('Markets not loaded')

        if isinstance(symbol, str):
            if symbol in self.markets:
                return self.markets[symbol]
            else:
                symbol = UXSymbol(self.id, self.market_type, symbol)

        if isinstance(symbol, UXSymbol):
            try:
                symbol = self.convert_symbol(symbol)
            except Exception:
                pass
            else:
                if symbol in self.markets:
                    return self.markets[symbol]

        raise ValueError(f'symbol not found: {symbol}')

    def get_service_provider(self, service):
        service_providers = getattr(self, 'serviceProviders', {})
        return service_providers.get(service)
