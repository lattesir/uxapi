from typing import NamedTuple


class UXSymbol(NamedTuple):
    exchange_id: str
    market_type: str
    name: str

    @classmethod
    def fromstring(cls, s):
        return cls(*s.split(':'))

    @classmethod
    def fromargs(cls, exchange_id, market_type, **kwargs):
        if market_type in ('spot', 'swap'):
            if 'base' in kwargs and 'quote' in kwargs:
                base = kwargs['base']
                quote = kwargs['quote']
                name = f'{base}/{quote}'.upper()
                return cls(exchange_id, market_type, name)

        if market_type == 'futures':
            if 'base' in kwargs and 'contract_expiration' in kwargs:
                base = kwargs['base']
                quote = kwargs.get('quote') or 'USD'
                contract_expiration = kwargs['contract_expiration']
                name = f'{base}/{quote}.{contract_expiration}'.upper()
                return cls(exchange_id, market_type, name)

        raise ValueError('unknown market_type or missing arguments')

    def __str__(self):
        return self.name

    def base_quote(self):
        base_quote, *_ = self.name.split('.')
        assert '/' in base_quote, 'invalid format'
        return base_quote.split('/')

    @property
    def base(self):
        return self.base_quote()[0]

    @property
    def quote(self):
        return self.base_quote()[1]

    @property
    def contract_expiration(self):
        info = self.name.split('.')
        assert len(info) > 1, 'invalid format'
        return info[1]