import os
import sys
import argparse

import dotenv

from uxapi import new_exchange


class RestRunner:
    def __init__(self, exchange_id, market_type):
        exchange = new_exchange(exchange_id, market_type, {
            'apiKey': os.environ.get(f'{exchange_id}_apiKey'),
            'secret': os.environ.get(f'{exchange_id}_secret'),
            'password': os.environ.get(f'{exchange_id}_password')
        })
        exchange.load_markets()
        self.exchange = exchange

    def __getattr__(self, attr):
        return getattr(self.exchange, attr)

    def _run(self, method, args):
        result = getattr(self, method)(*args)
        print(result)

    def create_limit_order(self, symbol, side, amount, price):
        return self.exchange.create_order(symbol, 'limit', side,
                                          amount, price)


def main():
    parser = argparse.ArgumentParser(
        usage='python rest.py [-h] exchange_id market_type method [arg [arg ...]]',
        description='Rest API Test',
        epilog='Example: python rest.py huobi futures create_limit_order BTC/USD.CQ buy 1 7000',
    )
    parser.add_argument('exchange_id')
    parser.add_argument('market_type')
    parser.add_argument('method')
    parser.add_argument('args', nargs='*')
    args = parser.parse_args()

    dotenv.load_dotenv()

    runner = RestRunner(args.exchange_id, args.market_type)
    runner._run(args.method, args.args)

if __name__ == '__main__':
    main()