import ccxt
import pendulum

from uxapi import register_exchange
from uxapi import UXPatch
from uxapi.helpers import contract_delivery_time


@register_exchange('deribit')
class Deribit(UXPatch, ccxt.deribit):
    def describe(self):
        return self.deep_extend(super().describe(), {
            'deliveryHourUTC': 8,
        })

    def _fetch_markets(self, params):
        markets = super()._fetch_markets(params)
        for market in markets:
            market['contractValue'] = self.safe_float(market['info'], 'contract_size')
            if market['info']['settlement_period'] != 'perpetual':
                timestamp = self.safe_integer(market['info'], 'expiration_timestamp')
                delivery_time = pendulum.from_timestamp(timestamp / 1000)
                market['deliveryTime'] = delivery_time.to_iso8601_string()
        return markets

    def convert_symbol(self, uxsymbol):
        month_names = [
            '', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
            'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC',
        ]

        if uxsymbol.market_type == 'futures':
            dt = contract_delivery_time(
                expiration=uxsymbol.contract_expiration,
                delivery_hour=self.deliveryHourUTC)
            return f'{uxsymbol.base}-{dt:%d}{month_names[dt.month]}{dt:%y}'

        elif uxsymbol.market_type == 'swap':
            return f'{uxsymbol.base}-PERPETUAL'

        elif uxsymbol.market_type == 'option':
            dt = pendulum.parse(uxsymbol.contract_expiration)
            return (
                f'{uxsymbol.base}-{dt:%d}{month_names[dt.month]}{dt:%y}-'
                f'{uxsymbol.option_strike_price}-{uxsymbol.option_type}'
            )

        raise ValueError('invalid symbol')
