class UXSymbol:
    def __init__(self, exchange_id, market_type, name):
        self.exchange_id = exchange_id
        self.market_type = market_type
        self.name = name
        self.isspecial = self.name.startswith('!')
        if self.isspecial:
            self.name_info = name[1:].split('.')
        else:
            self.name_info = name.split('.')

    @classmethod
    def fromstring(cls, s):
        return cls(*s.split(':'))

    def __repr__(self):
        args = ', '.join(repr(field) for field in tuple(self))
        return f'UXSymbol({args})'

    def __str__(self):
        return ':'.join(self)

    def __eq__(self, other):
        if isinstance(other, UXSymbol):
            return tuple(self) == tuple(other)
        return False

    def __hash__(self):
        return hash(str(self))

    def __iter__(self):
        return (field for field in (self.exchange_id, self.market_type, self.name))

    @property
    def base_quote(self):
        base, *quote = self.name_info[0].split('/', maxsplit=1)
        if quote:
            return base, quote[0]
        else:
            return base, base

    @property
    def base(self):
        return self.base_quote[0]

    @property
    def quote(self):
        return self.base_quote[1]

    @property
    def contract_expiration(self):
        return self.name_info[1]

    @property
    def option_strike_price(self):
        return self.name_info[2].replace('_', '.')

    @property
    def option_type(self):
        return self.name_info[3]
